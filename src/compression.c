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

bool compressionAlgoSupportsStreaming(compression_algo_t algo) {
    return algo == ALGO_LZ4;
}

bool compressionAlgoSupportsLevel(compression_algo_t algo) {
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
 *   LZ4F_compressBegin() (compression level and checksum mode).
 *
 * Bounds are computed with block-independent mode and block checksums enabled
 * so the returned capacity is safe for both checksum settings. */
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
size_t streamCompressOutputBound(compression_algo_t algo, size_t input_len, bool frame_started, compress_flush_mode_t flush_mode) {
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
            /* Local copy of shared prefs so we can set the actual level
             * and checksum mode per-stream. */
            LZ4F_preferences_t prefs = lz4f_prefs;
            prefs.compressionLevel = sc->level;
            prefs.frameInfo.blockChecksumFlag = sc->codec_checksum
                                                    ? LZ4F_blockChecksumEnabled
                                                    : LZ4F_noBlockChecksum;
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
            size_t r = LZ4F_compressUpdate((LZ4F_cctx *)sc->ctx.lz4f,
                                           output + offset,
                                           output_capacity - offset,
                                           input, input_len, NULL);
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
