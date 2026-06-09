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
 * Mirrors the 16 x PROTO_REPLY_CHUNK_BYTES threshold used previously, expressed
 * locally to keep this adapter independent of server.h. */
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

/* ===== Replica-side decompressor ===== */

replDecompressor *replDecompressorCreate(size_t feed_cap) {
    replDecompressor *rd = zcalloc(sizeof(*rd));

    streamReaderConfig cfg = {
        .expected_stream_kind = STREAM_KIND_REPL,
        .allow_passthrough = true,
        .buffer_size = 0, /* streamReaderInitPush selects the default. */
    };
    if (streamReaderInitPush(&rd->reader, &cfg, feed_cap) != 0) {
        zfree(rd);
        return NULL;
    }
    rd->decode_buf = sdsempty();
    return rd;
}

void replDecompressorDestroy(replDecompressor *rd) {
    if (!rd) return;
    streamReaderFree(&rd->reader);
    sdsfree(rd->decode_buf);
    zfree(rd);
}

replDecodeResult replDecompressorDecode(replDecompressor *rd,
                                        const void *src,
                                        size_t len,
                                        size_t output_max,
                                        size_t *out_len) {
    if (out_len) *out_len = 0;
    sdsclear(rd->decode_buf);

    if (streamReaderFeed(&rd->reader, src, len) != 0) return REPL_DECODE_ERR;

    char out_buf[REPL_DECODE_CHUNK];
    for (;;) {
        ssize_t got = streamReaderRead(&rd->reader, out_buf, sizeof(out_buf));
        if (got < 0) return REPL_DECODE_ERR;
        if (got == 0) {
            /* A long-lived replication stream must never reach a compressed
             * frame end. If it does, the stream is corrupt or the primary sent
             * an unexpected terminator: the caller should disconnect. */
            if (streamReaderFrameDone(&rd->reader)) return REPL_DECODE_FRAME_DONE;
            break; /* WOULD_BLOCK: need more bytes, resume next read tick. */
        }
        if (sdslen(rd->decode_buf) + (size_t)got > output_max) return REPL_DECODE_OVERFLOW;
        rd->decode_buf = sdscatlen(rd->decode_buf, out_buf, (size_t)got);
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
