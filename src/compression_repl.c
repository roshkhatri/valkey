/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_repl.h"
#include "zmalloc.h"
#include <string.h>

/* Release the staging/scratch buffer once it grows past this, so a bursty
 * batch does not leave a long-lived replica holding peak allocation forever.
 * Roughly 16 x PROTO_REPLY_CHUNK_BYTES, expressed locally to keep this adapter
 * independent of server.h. */
#define REPL_COMPRESSION_RETAIN_LIMIT (256 * 1024)

/* Decoded-output chunk pulled from the reader per iteration. */
#define REPL_DECODE_CHUNK (16 * 1024)

/* ===== Primary-side per-replica compressor ===== */

/* Emit callback for the streamWriter: append compressed bytes to out_buf. */
static int replCompressorEmit(void *ctx, const uint8_t *data, size_t len) {
    replCompressor *rc = (replCompressor *)ctx;
    rc->out_buf = sdscatlen(rc->out_buf, data, len);
    return 0;
}

replCompressor *replCompressorCreate(compressionAlgo algo, int level) {
    replCompressor *rc = zcalloc(sizeof(*rc));

    streamWriterConfig cfg = {
        .algo = algo,
        .level = level,
        .stream_kind = STREAM_KIND_REPL,
        .codec_checksum_enabled = 0,
    };
    rc->out_buf = sdsempty();
    if (streamWriterInit(&rc->writer, &cfg, replCompressorEmit, rc) != 0) {
        sdsfree(rc->out_buf);
        zfree(rc);
        return NULL;
    }
    return rc;
}

void replCompressorDestroy(replCompressor *rc) {
    if (!rc) return;
    streamWriterFree(&rc->writer);
    sdsfree(rc->out_buf);
    zfree(rc);
}

ssize_t replCompressorWrite(replCompressor *rc, const void *buf, size_t len) {
    return streamWriterWrite(&rc->writer, buf, len);
}

int replCompressorFlush(replCompressor *rc) {
    return streamWriterFlush(&rc->writer);
}

void replCompressorResetBatch(replCompressor *rc) {
    sdsclear(rc->out_buf);
    rc->out_buf_pos = 0;
    rc->raw_bytes = 0;
    if (sdsalloc(rc->out_buf) > REPL_COMPRESSION_RETAIN_LIMIT) {
        sdsfree(rc->out_buf);
        rc->out_buf = sdsempty();
    }
}

size_t replCompressorMemUsage(const replCompressor *rc) {
    if (!rc) return 0;
    size_t total = sizeof(*rc) + streamWriterMemUsage(&rc->writer);
    if (rc->out_buf) total += sdsalloc(rc->out_buf);
    return total;
}

compressionAlgo replCompressorAlgo(const replCompressor *rc) {
    return rc ? rc->writer.compressor.algo : ALGO_NONE;
}

/* ===== Replica-side decompressor ===== */

replDecompressor *replDecompressorCreate(void) {
    replDecompressor *rd = zcalloc(sizeof(*rd));
    rd->decode_buf = sdsempty();
    return rd;
}

void replDecompressorDestroy(replDecompressor *rd) {
    if (!rd) return;
    if (rd->mode == REPL_DECODE_MODE_COMPRESSED) streamDecompressorFree(&rd->decompressor);
    sdsfree(rd->decode_buf);
    zfree(rd);
}

/* Append raw bytes to the decode buffer (passthrough), enforcing output_max. */
static replDecodeResult replDecodeEmit(replDecompressor *rd, const uint8_t *in, size_t len, size_t output_max) {
    if (len == 0) return REPL_DECODE_OK;
    if (sdslen(rd->decode_buf) + len > output_max) return REPL_DECODE_OVERFLOW;
    rd->decode_buf = sdscatlen(rd->decode_buf, in, len);
    return REPL_DECODE_OK;
}

/* Drain compressed bytes [in, in+len) through the codec into rd->decode_buf,
 * bounded by output_max (decompression-bomb guard). */
static replDecodeResult replDecodeFeed(replDecompressor *rd, const uint8_t *in, size_t len, size_t output_max) {
    size_t off = 0;
    while (off < len) {
        char out_buf[REPL_DECODE_CHUNK];
        size_t consumed = 0;
        ssize_t produced = streamDecompressorFeed(&rd->decompressor, (uint8_t *)out_buf, sizeof(out_buf),
                                                  in + off, len - off, &consumed);
        if (produced < 0 || consumed > len - off) return REPL_DECODE_ERR;
        if (produced > 0) {
            if (sdslen(rd->decode_buf) + (size_t)produced > output_max) return REPL_DECODE_OVERFLOW;
            rd->decode_buf = sdscatlen(rd->decode_buf, out_buf, (size_t)produced);
        }
        off += consumed;
        /* A long-lived replication stream must never reach a compressed frame
         * end. If it does, the stream is corrupt or the primary sent an
         * unexpected terminator: the caller should disconnect. */
        if (rd->decompressor.frame_done) return REPL_DECODE_FRAME_DONE;
        /* No progress: the codec has buffered a partial frame and needs more
         * bytes. Resume on the next read tick. */
        if (consumed == 0 && produced == 0) break;
    }
    return REPL_DECODE_OK;
}

replDecodeResult replDecompressorDecode(replDecompressor *rd,
                                        const void *src,
                                        size_t len,
                                        size_t output_max,
                                        size_t *out_len) {
    static const uint8_t vcs_magic[VCS_MAGIC_SIZE] = {VCS_MAGIC_0, VCS_MAGIC_1, VCS_MAGIC_2};

    if (out_len) *out_len = 0;
    sdsclear(rd->decode_buf);

    const uint8_t *in = src;
    size_t off = 0;

    /* Probe phase: classify the stream from its leading bytes. The magic may
     * arrive split across feeds, so accumulate until we can match or rule it
     * out against the VCS magic. */
    if (rd->mode == REPL_DECODE_MODE_PROBE) {
        while (rd->envelope_len < VCS_MAGIC_SIZE && off < len) {
            if (in[off] != vcs_magic[rd->envelope_len]) {
                rd->mode = REPL_DECODE_MODE_PASSTHROUGH; /* not a VCS stream */
                break;
            }
            rd->envelope[rd->envelope_len++] = in[off++];
        }

        if (rd->mode == REPL_DECODE_MODE_PROBE) {
            /* Magic matches so far; gather the rest of the envelope. */
            while (rd->envelope_len < VCS_ENVELOPE_SIZE && off < len) rd->envelope[rd->envelope_len++] = in[off++];
            if (rd->envelope_len < VCS_ENVELOPE_SIZE) return REPL_DECODE_OK; /* need more header */

            streamReaderInfo info;
            if (streamReadEnvelopeInfo(rd->envelope, VCS_ENVELOPE_SIZE, STREAM_KIND_REPL, &info) != 0)
                return REPL_DECODE_ERR;
            if (!info.compressed) return REPL_DECODE_ERR;
            if (streamDecompressorInit(&rd->decompressor, info.algo) != 0) return REPL_DECODE_ERR;
            rd->mode = REPL_DECODE_MODE_COMPRESSED;
        }
    }

    if (rd->mode == REPL_DECODE_MODE_PASSTHROUGH) {
        /* Replay any buffered magic-prefix bytes once, then forward the rest. */
        replDecodeResult r = replDecodeEmit(rd, rd->envelope, rd->envelope_len, output_max);
        rd->envelope_len = 0;
        if (r != REPL_DECODE_OK) return r;
        r = replDecodeEmit(rd, in + off, len - off, output_max);
        if (r != REPL_DECODE_OK) return r;
    } else if (off < len) {
        replDecodeResult r = replDecodeFeed(rd, in + off, len - off, output_max);
        if (r != REPL_DECODE_OK) return r;
    }

    /* Shrink the scratch buffer if it grew large and is now mostly empty. */
    if (sdsalloc(rd->decode_buf) > REPL_COMPRESSION_RETAIN_LIMIT &&
        sdslen(rd->decode_buf) < sdsalloc(rd->decode_buf) / 4) {
        rd->decode_buf = sdsRemoveFreeSpace(rd->decode_buf, 0);
    }

    if (out_len) *out_len = sdslen(rd->decode_buf);
    return REPL_DECODE_OK;
}

sds replDecompressorBuf(replDecompressor *rd) {
    return rd ? rd->decode_buf : NULL;
}
