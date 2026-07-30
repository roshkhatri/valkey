/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_repl.h"
#include "zmalloc.h"

/* Release the decode scratch buffer once it grows past this so a replica does
 * not retain peak allocation. ~16 x PROTO_REPLY_CHUNK_BYTES, expressed locally
 * to keep this adapter independent of server.h. */
#define REPL_COMPRESSION_RETAIN_LIMIT (256 * 1024)

/* Retain the encode staging buffer up to a full batch's LZ4 worst-case output. */
#define REPL_COMPRESSION_ENCODE_RETAIN_LIMIT \
    (REPL_COMPRESSION_BATCH_LIMIT + (REPL_COMPRESSION_BATCH_LIMIT / 255) + 1024)

/* Decoded-output room offered to the codec per feed iteration. */
#define REPL_DECODE_CHUNK (16 * 1024)

/* ===== Primary-side per-replica compressor ===== */

replCompressor *replCompressorCreate(compressionAlgo algo, int level) {
    replCompressor *rc = zcalloc(sizeof(*rc));

    streamWriterConfig cfg = {
        .algo = algo,
        .level = level,
        .codec_checksum_enabled = false,
        .stream_kind = VCS_STREAM_REPL,
    };
    rc->out_buf = sdsempty();
    if (streamWriterInit(&rc->writer, &cfg, NULL, NULL) != 0) {
        sdsfree(rc->out_buf);
        zfree(rc);
        return NULL;
    }
    streamWriterSetSink(&rc->writer, &rc->out_buf);
    return rc;
}

void replCompressorDestroy(replCompressor *rc) {
    if (!rc) return;
    streamWriterFree(&rc->writer);
    sdsfree(rc->out_buf);
    zfree(rc);
}

int replCompressorWrite(replCompressor *rc, const void *buf, size_t len) {
    return streamWriterWrite(&rc->writer, buf, len);
}

int replCompressorFlush(replCompressor *rc) {
    return streamWriterFlush(&rc->writer);
}

void replCompressorResetBatch(replCompressor *rc) {
    /* Decide retention from the payload length, not sdsalloc: SDS grows capacity
     * greedily, so a normal batch would otherwise be reclaimed and reallocated
     * every cycle. A batch is capped at REPL_COMPRESSION_BATCH_LIMIT, so its
     * output stays within the bound; only an oversized payload is reclaimed. */
    size_t used = sdslen(rc->out_buf);
    sdsclear(rc->out_buf);
    rc->out_buf_pos = 0;
    rc->raw_bytes = 0;
    if (used > REPL_COMPRESSION_ENCODE_RETAIN_LIMIT) {
        sdsfree(rc->out_buf);
        rc->out_buf = sdsempty();
    }
}

size_t replCompressorMemUsage(const replCompressor *rc) {
    if (!rc) return 0;
    /* Codec context memory is small and fixed; only the staging SDS is measured. */
    size_t total = sizeof(*rc);
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
        size_t used = sdslen(rd->decode_buf);
        /* Budget exhausted with input still pending: the stream expands past
         * the bomb-guard cap. Checked up front so the codec is never handed
         * more room than the remaining budget allows. */
        if (used >= output_max) return REPL_DECODE_OVERFLOW;
        size_t room = REPL_DECODE_CHUNK;
        if (room > output_max - used) room = output_max - used;
        rd->decode_buf = sdsMakeRoomFor(rd->decode_buf, room);
        size_t consumed = 0;
        ssize_t produced = streamDecompressorFeed(&rd->decompressor,
                                                  (uint8_t *)rd->decode_buf + used,
                                                  room,
                                                  in + off, len - off, &consumed);
        if (produced < 0 || consumed > len - off) return REPL_DECODE_ERR;
        if (produced > 0) sdsIncrLen(rd->decode_buf, (size_t)produced);
        off += consumed;
        /* A long-lived replication stream must never reach a compressed frame
         * end. If it does, the stream is corrupt or the primary sent an
         * unexpected terminator: the caller should disconnect. */
        if (rd->decompressor.frame_done) return REPL_DECODE_FRAME_DONE;
        /* The codec always makes progress given input and output room; no
         * progress with input still pending is a stuck state. Fail rather
         * than let the caller drop the unconsumed tail. */
        if (consumed == 0 && produced == 0) return REPL_DECODE_ERR;
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
     * arrive split across feeds, so bytes accumulate until the prefix matches
     * or rules out the VCS magic. */
    if (rd->mode == REPL_DECODE_MODE_PROBE) {
        while (rd->envelope_len < VCS_MAGIC_SIZE && off < len) {
            if (in[off] != vcs_magic[rd->envelope_len]) {
                rd->mode = REPL_DECODE_MODE_PASSTHROUGH; /* Not a VCS stream. */
                break;
            }
            rd->envelope[rd->envelope_len++] = in[off++];
        }

        if (rd->mode == REPL_DECODE_MODE_PROBE) {
            /* Magic matches so far; gather the rest of the envelope. */
            while (rd->envelope_len < VCS_ENVELOPE_SIZE && off < len) rd->envelope[rd->envelope_len++] = in[off++];
            if (rd->envelope_len < VCS_ENVELOPE_SIZE) return REPL_DECODE_OK; /* Need more header. */

            compressionAlgo algo = ALGO_NONE;
            if (streamParseVcsEnvelope(rd->envelope, VCS_ENVELOPE_SIZE, VCS_STREAM_REPL, &algo) != 0)
                return REPL_DECODE_ERR;
            if (streamDecompressorInit(&rd->decompressor, algo, false) != 0) return REPL_DECODE_ERR;
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

bool replDecompressorIsPassthrough(const replDecompressor *rd) {
    return rd && rd->mode == REPL_DECODE_MODE_PASSTHROUGH;
}
