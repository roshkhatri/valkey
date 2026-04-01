/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Streaming compression/decompression using algorithm-native framing.
 * Currently supports LZ4 via LZ4F frame API. */

#include "compression.h"
#include <limits.h>
#include <lz4frame.h>
#include <string.h>
#include <unistd.h>

int compressionAlgoSupportsStreaming(compression_algo_t algo) {
    return algo == ALGO_LZ4;
}

int compressionAlgoSupportsLevel(compression_algo_t algo) {
    return algo == ALGO_LZ4;
}

const char *compressionAlgoName(compression_algo_t algo) {
    switch (algo) {
    case ALGO_NONE:
        return "none";
    case ALGO_LZF:
        return "lzf";
    case ALGO_LZ4:
        return "lz4";
    default:
        return "unknown";
    }
}

/* --- Envelope --- */

/* Write 8-byte VKCS envelope via callback.
 * Shared utility used by BOTH sync (rio decorator) and async (replication)
 * paths.  Prevents "sync emits envelope but async forgets it" class of bugs.
 *
 * Layout (little-endian where applicable):
 *   [0..3] magic  "VKCS" (0x56 0x4B 0x43 0x53)
 *   [4]    version (VKCS_VERSION, currently 1)
 *   [5]    algo_id (compression_algo_t value)
 *   [6]    flags   (bit 0 = stream_kind: 0=RDB, 1=REPL;
 *                   bit 1 = codec checksum enabled; bits 2-7 reserved)
 *   [7]    reserved (must be 0)
 *
 * Returns 0 on success, -1 on error (invalid algo or emit_cb failure). */
int writeVkcsEnvelope(vkcsEmitFn emit_cb,
                      void *ctx,
                      compression_algo_t algo,
                      uint8_t stream_kind,
                      int codec_checksum_enabled) {
    /* Only streaming algorithms are valid in the envelope. */
    if (!emit_cb) return -1;
    if (!compressionAlgoSupportsStreaming(algo)) return -1;
    if (stream_kind != STREAM_KIND_RDB && stream_kind != STREAM_KIND_REPL) return -1;

    uint8_t envelope[VKCS_ENVELOPE_SIZE];
    envelope[0] = VKCS_MAGIC_0;
    envelope[1] = VKCS_MAGIC_1;
    envelope[2] = VKCS_MAGIC_2;
    envelope[3] = VKCS_MAGIC_3;
    envelope[4] = VKCS_VERSION;
    envelope[5] = (uint8_t)algo;
    envelope[6] = (uint8_t)(stream_kind & VKCS_FLAG_STREAM_KIND);
    if (codec_checksum_enabled) envelope[6] |= VKCS_FLAG_CODEC_CHECKSUM;
    envelope[7] = 0; /* reserved */

    int rc = emit_cb(ctx, envelope, VKCS_ENVELOPE_SIZE);
    return rc == 0 ? 0 : -1;
}

/* Parse 8-byte VKCS envelope from buffer.
 * Validates magic bytes, version, algorithm, and reserved fields.
 * Rejects envelopes with non-zero reserved bits so future versions are
 * detected early rather than causing silent data corruption.
 * On success populates *algo, *stream_kind, and *codec_checksum_enabled and
 * returns 0.
 * Returns -1 on error (bad magic, unsupported version, unknown algo,
 * reserved bits set). */
int readVkcsEnvelope(const uint8_t *buf,
                     size_t len,
                     compression_algo_t *algo,
                     uint8_t *stream_kind,
                     int *codec_checksum_enabled) {
    if (!buf || len < VKCS_ENVELOPE_SIZE) return -1;

    /* Validate magic */
    if (buf[0] != VKCS_MAGIC_0 || buf[1] != VKCS_MAGIC_1 ||
        buf[2] != VKCS_MAGIC_2 || buf[3] != VKCS_MAGIC_3) {
        return -1;
    }

    /* Validate version — only version 1 is supported */
    if (buf[4] != VKCS_VERSION) return -1;

    /* Extract and validate algorithm */
    uint8_t algo_id = buf[5];
    if (!compressionAlgoSupportsStreaming((compression_algo_t)algo_id)) return -1;

    /* Reject envelopes with reserved bits/bytes set (strict reader pattern) */
    uint8_t flags = buf[6];
    if (flags & 0xFC) return -1; /* reserved flag bits 2-7 must be 0 */
    if (buf[7] != 0) return -1;  /* reserved byte must be 0 */

    uint8_t kind = flags & VKCS_FLAG_STREAM_KIND;
    int checksum_enabled = (flags & VKCS_FLAG_CODEC_CHECKSUM) != 0;

    if (algo) *algo = (compression_algo_t)algo_id;
    if (stream_kind) *stream_kind = kind;
    if (codec_checksum_enabled) *codec_checksum_enabled = checksum_enabled;

    return 0;
}

/* --- Streaming compressor --- */

/* Initialize a streaming compressor for the given algorithm.
 * For LZ4: creates an LZ4F compression context.
 * Returns 0 on success, -1 on error. */
int streamCompressorInit(stream_compressor_t *sc, compression_algo_t algo, int level) {
    if (!sc) return -1;
    memset(sc, 0, sizeof(*sc));

    if (!compressionAlgoSupportsStreaming(algo)) return -1;

    LZ4F_cctx *cctx = NULL;
    LZ4F_errorCode_t err = LZ4F_createCompressionContext(&cctx, LZ4F_VERSION);
    if (LZ4F_isError(err)) return -1;

    sc->algo = algo;
    sc->level = level;
    sc->ctx.lz4f = cctx;
    return 0;
}

/* Destroy a streaming compressor, freeing the algorithm context.
 * Safe to call on a zero-initialized or already-destroyed compressor. */
void streamCompressorDestroy(stream_compressor_t *sc) {
    if (!sc) return;

    if (sc->algo == ALGO_LZ4 && sc->ctx.lz4f) {
        LZ4F_freeCompressionContext((LZ4F_cctx *)sc->ctx.lz4f);
        sc->ctx.lz4f = NULL;
    }
    sc->algo = ALGO_NONE;
    sc->frame_started = false;
}

/* Initialize a streaming decompressor for the given algorithm.
 * For LZ4: creates an LZ4F decompression context.
 * Returns 0 on success, -1 on error. */
int streamDecompressorInit(stream_decompressor_t *sd, compression_algo_t algo) {
    if (!sd) return -1;
    memset(sd, 0, sizeof(*sd));

    if (!compressionAlgoSupportsStreaming(algo)) return -1;

    LZ4F_dctx *dctx = NULL;
    LZ4F_errorCode_t err = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(err)) return -1;

    sd->algo = algo;
    sd->ctx.lz4f = dctx;
    return 0;
}

/* Destroy a streaming decompressor, freeing the algorithm context and buffers.
 * Safe to call on a zero-initialized or already-destroyed decompressor. */
void streamDecompressorDestroy(stream_decompressor_t *sd) {
    if (!sd) return;

    if (sd->algo == ALGO_LZ4 && sd->ctx.lz4f) {
        LZ4F_freeDecompressionContext((LZ4F_dctx *)sd->ctx.lz4f);
        sd->ctx.lz4f = NULL;
    }
    sd->algo = ALGO_NONE;
    sd->errored = false;
}

/* Shared LZ4F preferences template.
 * - Used by streamCompressOutputBound() for bounds.
 * - Copied and selectively overridden in streamCompressFeed() before
 *   LZ4F_compressBegin() (compression level, checksum mode, block mode).
 *
 * Bounds are computed with block checksum enabled and block-independent
 * mode (worst-case for output size). */
static const LZ4F_preferences_t lz4f_prefs = {
    .frameInfo = {
        .blockChecksumFlag = LZ4F_blockChecksumEnabled,
        .contentChecksumFlag = LZ4F_noContentChecksum,
        .blockSizeID = LZ4F_max64KB,
        .blockMode = LZ4F_blockIndependent,
    },
    .compressionLevel = 0, /* bound calculation uses 0 (worst-case); actual
                            * compression uses sc->level via a local copy */
};

/* Return upper bound on compressed output size.
 * Accounts for frame header overhead when !frame_started and
 * flush/end overhead for internally buffered data.
 * For LZ4: uses lz4f_prefs (64KB blocks) to match streamCompressFeed. */
size_t streamCompressOutputBound(compression_algo_t algo, size_t input_len, int frame_started, compress_flush_mode_t flush_mode) {
    switch (algo) {
    case ALGO_LZ4: {
        size_t bound = LZ4F_compressBound(input_len, &lz4f_prefs);
        if (!frame_started) {
            bound += LZ4F_HEADER_SIZE_MAX;
        }
        if (flush_mode != FLUSH_CONTINUE) {
            /* LZ4F_compressBound(0) covers buffered flush bytes and frame end. */
            bound += LZ4F_compressBound(0, &lz4f_prefs);
        }
        return bound;
    }
    default:
        return 0;
    }
}

/* Feed data through the streaming compressor.
 * flush_mode: 0=continue (buffer internally), 1=flush (emit all buffered),
 *             2=end (finalize frame).
 * Returns bytes written to output, 0 for no output, -1 on error. */
ssize_t streamCompressFeed(stream_compressor_t *sc,
                           uint8_t *output,
                           size_t output_capacity,
                           const uint8_t *input,
                           size_t input_len,
                           compress_flush_mode_t flush_mode) {
    if (!sc || !output) return -1;
    if (sc->errored) return -1;

    switch (sc->algo) {
    case ALGO_LZ4: {
        if (!sc->ctx.lz4f) return -1;
        size_t offset = 0;

        /* Begin frame on first call */
        if (!sc->frame_started) {
            /* Local copy of shared prefs so we can set the actual level,
             * checksum mode, and block mode per-stream. */
            LZ4F_preferences_t prefs = lz4f_prefs;
            prefs.compressionLevel = sc->level;
            prefs.frameInfo.blockChecksumFlag = sc->block_checksum
                                                    ? LZ4F_blockChecksumEnabled
                                                    : LZ4F_noBlockChecksum;
            prefs.frameInfo.blockMode = sc->block_mode == COMPRESS_BLOCK_LINKED
                                            ? LZ4F_blockLinked
                                            : LZ4F_blockIndependent;
            size_t r = LZ4F_compressBegin((LZ4F_cctx *)sc->ctx.lz4f,
                                          output, output_capacity, &prefs);
            if (LZ4F_isError(r)) {
                /* compressBegin failure before any frame bytes are emitted is
                 * recoverable — the LZ4F context is still clean. Caller can
                 * retry with a larger buffer. Don't set errored. */
                return -1;
            }
            offset = r;
            sc->frame_started = true;
        }

        /* Compress input data */
        if (input_len > 0) {
            if (offset >= output_capacity) goto lz4_error;
            /* stableSrc is caller-controlled. The async replication path
             * sets sc->stable_src=true because the accumulator sds is swapped
             * out before submission (exclusive ownership). The sync RDB
             * path leaves it at false (default) since callers may reuse the
             * input buffer between writes. */
            LZ4F_compressOptions_t opts = {.stableSrc = (unsigned)sc->stable_src};
            size_t r = LZ4F_compressUpdate((LZ4F_cctx *)sc->ctx.lz4f,
                                           output + offset,
                                           output_capacity - offset,
                                           input, input_len, &opts);
            if (LZ4F_isError(r)) goto lz4_error;
            offset += r;
        }

        /* Handle flush/end modes */
        if (flush_mode == FLUSH_SYNC) {
            if (offset >= output_capacity) goto lz4_error;
            size_t r = LZ4F_flush((LZ4F_cctx *)sc->ctx.lz4f,
                                  output + offset,
                                  output_capacity - offset, NULL);
            if (LZ4F_isError(r)) goto lz4_error;
            offset += r;
        } else if (flush_mode == FLUSH_END) {
            if (offset >= output_capacity) goto lz4_error;
            size_t r = LZ4F_compressEnd((LZ4F_cctx *)sc->ctx.lz4f,
                                        output + offset,
                                        output_capacity - offset, NULL);
            if (LZ4F_isError(r)) goto lz4_error;
            offset += r;
            sc->frame_started = false;
        }

        if (offset > (size_t)SSIZE_MAX) goto lz4_error;
        return (ssize_t)offset;

    lz4_error:
        /* LZ4F state is undefined after an error (lz4frame.h line 325).
         * Mark permanently failed — no mid-stream retry is possible because
         * already-emitted frame bytes cannot be unsent. The caller must
         * tear down the stream (disconnect replica / abort RDB save). */
        sc->errored = true;
        return -1;
    }
    default:
        return -1;
    }
}

/* Feed compressed data through the streaming decompressor.
 * Returns bytes written to output, 0 for no output, -1 on error.
 * *input_consumed is set to the number of input bytes consumed.
 * On fatal errors, sd->errored is latched and subsequent calls return -1. */
ssize_t streamDecompressFeed(stream_decompressor_t *sd,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             size_t *input_consumed) {
    if (!sd || !input_consumed) return -1;
    if (sd->errored) return -1;
    *input_consumed = 0;
    if (sd->frame_done) return 0;
    /* Zero output capacity is a caller bug — returning 0 with no progress
     * would cause streaming loops to spin forever. */
    if (!output || output_capacity == 0) {
        sd->errored = true;
        return -1;
    }

    switch (sd->algo) {
    case ALGO_LZ4: {
        if (!sd->ctx.lz4f) {
            sd->errored = true;
            return -1;
        }
        size_t dst_size = output_capacity;
        size_t src_size = input_len;
        size_t ret = LZ4F_decompress((LZ4F_dctx *)sd->ctx.lz4f,
                                     output, &dst_size,
                                     input, &src_size, NULL);
        if (LZ4F_isError(ret)) {
            sd->errored = true;
            return -1;
        }
        *input_consumed = src_size;
        if (ret == 0) sd->frame_done = true;
        if (dst_size > (size_t)SSIZE_MAX) {
            sd->errored = true;
            return -1;
        }
        return (ssize_t)dst_size;
    }
    default:
        sd->errored = true;
        return -1;
    }
}
