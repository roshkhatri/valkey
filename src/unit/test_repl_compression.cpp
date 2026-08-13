/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Unit tests for replication compression configuration and capability constants. */

#include "generated_wrappers.hpp"

extern "C" {
#include "compression.h"
#include "compression_repl.h"
#include "server.h"
}

/* zmalloc.h defines helper macros that collide with libstdc++ internals. */
#ifdef __xstr
#undef __xstr
#endif
#ifdef __str
#undef __str
#endif

TEST(replCompression, capaCompressionBitNoConflict) {
    /* Each REPLICA_CAPA_* must occupy a unique bit position. */
    int all_capas[] = {
        REPLICA_CAPA_EOF,
        REPLICA_CAPA_PSYNC2,
        REPLICA_CAPA_DUAL_CHANNEL,
        REPLICA_CAPA_SKIP_RDB_CHECKSUM,
        REPLICA_CAPA_COMPRESSION,
    };
    int count = sizeof(all_capas) / sizeof(all_capas[0]);

    for (int i = 0; i < count; i++) {
        /* Each value must be a power of two (single bit set). */
        EXPECT_NE(all_capas[i], 0) << "capability " << i << " must be non-zero";
        EXPECT_EQ(all_capas[i] & (all_capas[i] - 1), 0)
            << "capability " << i << " must be a power of two";

        for (int j = i + 1; j < count; j++) {
            EXPECT_EQ(all_capas[i] & all_capas[j], 0)
                << "capabilities " << i << " and " << j << " must not share bits";
        }
    }

    /* Verify the specific value. */
    EXPECT_EQ(REPLICA_CAPA_COMPRESSION, (1 << 4));
}

TEST(replCompression, resetBatchRetainsAllocationForIncompressibleBatch) {
    replCompressor *rc = replCompressorCreate(ALGO_LZ4);
    ASSERT_TRUE(rc != NULL);
    /* ~1 MiB of incompressible (xorshift) bytes: compressed payload ~= input,
     * exercising the greedy-SDS over-allocation path. Stays at/under the 1 MiB
     * batch cap so the payload is within the retention bound. */
    const size_t n = 1000000;
    unsigned char *buf = (unsigned char *)zmalloc(n);
    uint32_t x = 0x9e3779b9u;
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        buf[i] = (unsigned char)(x >> 24);
    }
    ASSERT_EQ(replCompressorWrite(rc, buf, n), C_OK);
    ASSERT_EQ(replCompressorFlush(rc), C_OK);
    size_t alloc_before = sdsalloc(rc->out_buf);
    size_t len_before = sdslen(rc->out_buf);
    EXPECT_GT(len_before, (size_t)(900 * 1024));    /* ratio ~1: payload is large */
    EXPECT_GT(alloc_before, (size_t)(1024 * 1024)); /* greedy SDS over-allocates past 1 MiB */
    replCompressorResetBatch(rc);
    EXPECT_EQ(sdslen(rc->out_buf), (size_t)0);      /* cleared */
    EXPECT_EQ(sdsalloc(rc->out_buf), alloc_before); /* retained, not freed to empty */
    zfree(buf);
    replCompressorDestroy(rc);
}

/* ===== Decoder corruption and edge cases (crafted bytes) ===== */

/* Fill buf with incompressible xorshift bytes. */
static void fillIncompressible(unsigned char *buf, size_t n, uint32_t seed) {
    uint32_t x = seed;
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        buf[i] = (unsigned char)(x >> 24);
    }
}

/* LZ4 frame FLG byte (follows the 4-byte frame magic): bit 2 = content
 * checksum present, bit 4 = block checksums present. Frozen wire format. */
#define LZ4F_FLG_CONTENT_CHECKSUM 0x04
#define LZ4F_FLG_BLOCK_CHECKSUM 0x10

TEST(replCompression, replFrameOmitsContentChecksum) {
    /* A repl frame never ends, so its content checksum would be computed on
     * every byte but never emitted or validated. It must be off in the frame
     * header while block checksums stay on. */
    replCompressor *rc = replCompressorCreate(ALGO_LZ4);
    ASSERT_TRUE(rc != NULL);
    const char payload[] = "content-checksum-off-for-repl";
    ASSERT_EQ(replCompressorWrite(rc, payload, sizeof(payload)), C_OK);
    ASSERT_EQ(replCompressorFlush(rc), C_OK);
    const unsigned char *stream = (const unsigned char *)rc->out_buf;
    ASSERT_GE(sdslen(rc->out_buf), (size_t)(VCS_ENVELOPE_SIZE + 5));
    /* LZ4 frame magic 0x184D2204 (little-endian) right after the envelope. */
    EXPECT_EQ(stream[VCS_ENVELOPE_SIZE + 0], 0x04);
    EXPECT_EQ(stream[VCS_ENVELOPE_SIZE + 1], 0x22);
    EXPECT_EQ(stream[VCS_ENVELOPE_SIZE + 2], 0x4D);
    EXPECT_EQ(stream[VCS_ENVELOPE_SIZE + 3], 0x18);
    unsigned char flg = stream[VCS_ENVELOPE_SIZE + 4];
    EXPECT_EQ(flg & LZ4F_FLG_CONTENT_CHECKSUM, 0x00);
    EXPECT_EQ(flg & LZ4F_FLG_BLOCK_CHECKSUM, LZ4F_FLG_BLOCK_CHECKSUM);

    /* Round-trip: the decoder learns checksum presence from the frame header,
     * so it needs no matching configuration. */
    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    size_t out_len = 0;
    ASSERT_EQ(replDecompressorDecode(rd, rc->out_buf, sdslen(rc->out_buf), 1024 * 1024, &out_len),
              REPL_DECODE_OK);
    ASSERT_EQ(out_len, sizeof(payload));
    EXPECT_EQ(memcmp(replDecompressorBuf(rd), payload, sizeof(payload)), 0);

    replDecompressorDestroy(rd);
    replCompressorDestroy(rc);
}

TEST(replCompression, rdbFrameKeepsContentChecksum) {
    /* Contrast: the default (RDB) stream kind finishes its frame, so the
     * content checksum stays on. */
    streamWriter writer;
    sds sink = sdsempty();
    ASSERT_EQ(streamWriterInit(&writer, ALGO_LZ4, true, NULL, NULL), C_OK);
    streamWriterSetSink(&writer, &sink);
    ASSERT_EQ(streamWriterWrite(&writer, "rdb-bytes", 9), C_OK);
    ASSERT_GE(sdslen(sink), (size_t)(VCS_ENVELOPE_SIZE + 5));
    unsigned char flg = ((const unsigned char *)sink)[VCS_ENVELOPE_SIZE + 4];
    EXPECT_EQ(flg & LZ4F_FLG_CONTENT_CHECKSUM, LZ4F_FLG_CONTENT_CHECKSUM);
    EXPECT_EQ(flg & LZ4F_FLG_BLOCK_CHECKSUM, LZ4F_FLG_BLOCK_CHECKSUM);
    streamWriterFree(&writer);
    sdsfree(sink);
}

TEST(replCompression, decodeFrameDoneOnLiveLink) {
    replCompressor *rc = replCompressorCreate(ALGO_LZ4);
    ASSERT_TRUE(rc != NULL);
    const char payload[] = "frame-done-on-live-link";
    ASSERT_EQ(replCompressorWrite(rc, payload, sizeof(payload)), C_OK);
    /* Finish ends the frame; a live replication link must never see that. */
    ASSERT_EQ(streamWriterFinish(&rc->writer), C_OK);

    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    size_t out_len = 0;
    EXPECT_EQ(replDecompressorDecode(rd, rc->out_buf, sdslen(rc->out_buf), 1024 * 1024, &out_len),
              REPL_DECODE_FRAME_DONE);

    replDecompressorDestroy(rd);
    replCompressorDestroy(rc);
}

TEST(replCompression, decodeOverflowGuard) {
    replCompressor *rc = replCompressorCreate(ALGO_LZ4);
    ASSERT_TRUE(rc != NULL);
    /* Incompressible payload so decoded output far exceeds the cap. */
    const size_t n = 64 * 1024;
    unsigned char *buf = (unsigned char *)zmalloc(n);
    fillIncompressible(buf, n, 0x12345678u);
    ASSERT_EQ(replCompressorWrite(rc, buf, n), C_OK);
    ASSERT_EQ(replCompressorFlush(rc), C_OK); /* frame stays open */

    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    size_t out_len = 0;
    EXPECT_EQ(replDecompressorDecode(rd, rc->out_buf, sdslen(rc->out_buf), 1024, &out_len),
              REPL_DECODE_OVERFLOW);

    replDecompressorDestroy(rd);
    zfree(buf);
    replCompressorDestroy(rc);
}

TEST(replCompression, decodeEnvelopeSplitAcrossFeeds) {
    replCompressor *rc = replCompressorCreate(ALGO_LZ4);
    ASSERT_TRUE(rc != NULL);
    const size_t n = 10 * 1024;
    unsigned char *payload = (unsigned char *)zmalloc(n);
    memset(payload, 'A', n);
    ASSERT_EQ(replCompressorWrite(rc, payload, n), C_OK);
    ASSERT_EQ(replCompressorFlush(rc), C_OK);
    const unsigned char *stream = (const unsigned char *)rc->out_buf;
    const size_t stream_len = sdslen(rc->out_buf);
    ASSERT_GT(stream_len, (size_t)VCS_ENVELOPE_SIZE);

    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    unsigned char *decoded = (unsigned char *)zmalloc(n);
    size_t decoded_len = 0;
    size_t out_len = 0;

    /* Byte 0 alone: probe cannot classify yet, nothing decodes. */
    ASSERT_EQ(replDecompressorDecode(rd, stream, 1, 1024 * 1024, &out_len), REPL_DECODE_OK);
    EXPECT_EQ(out_len, (size_t)0);
    memcpy(decoded + decoded_len, replDecompressorBuf(rd), out_len);
    decoded_len += out_len;

    /* Bytes 1-2: magic complete, envelope still short. */
    ASSERT_EQ(replDecompressorDecode(rd, stream + 1, 2, 1024 * 1024, &out_len), REPL_DECODE_OK);
    EXPECT_EQ(out_len, (size_t)0);
    memcpy(decoded + decoded_len, replDecompressorBuf(rd), out_len);
    decoded_len += out_len;

    /* Remainder: envelope parses and the payload decodes. */
    ASSERT_EQ(replDecompressorDecode(rd, stream + 3, stream_len - 3, 1024 * 1024, &out_len),
              REPL_DECODE_OK);
    ASSERT_LE(decoded_len + out_len, n);
    memcpy(decoded + decoded_len, replDecompressorBuf(rd), out_len);
    decoded_len += out_len;

    ASSERT_EQ(decoded_len, n);
    EXPECT_EQ(memcmp(decoded, payload, n), 0);

    zfree(decoded);
    zfree(payload);
    replDecompressorDestroy(rd);
    replCompressorDestroy(rc);
}

TEST(replCompression, decodePassthroughReplaysPrefix) {
    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    size_t out_len = 0;
    /* "V" alone could still open the VCS magic: buffered, nothing emitted. */
    ASSERT_EQ(replDecompressorDecode(rd, "V", 1, 1024, &out_len), REPL_DECODE_OK);
    EXPECT_EQ(out_len, (size_t)0);
    EXPECT_FALSE(replDecompressorIsPassthrough(rd));
    /* "X" rules out the magic: the buffered "V" replays ahead of the new bytes. */
    ASSERT_EQ(replDecompressorDecode(rd, "XYZ", 3, 1024, &out_len), REPL_DECODE_OK);
    EXPECT_TRUE(replDecompressorIsPassthrough(rd));
    ASSERT_EQ(out_len, (size_t)4);
    ASSERT_EQ(sdslen(replDecompressorBuf(rd)), (size_t)4);
    EXPECT_EQ(memcmp(replDecompressorBuf(rd), "VXYZ", 4), 0);
    replDecompressorDestroy(rd);
}

TEST(replCompression, decodeRejectsBadCodec) {
    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    /* Valid magic/version/kind, unknown codec id 0xFF. */
    unsigned char stream[VCS_ENVELOPE_SIZE + 4] = {'V', 'C', 'S', VCS_VERSION, 0xFF, 0x00,
                                                   VCS_STREAM_REPL, 0xDE, 0xAD, 0xBE, 0xEF};
    size_t out_len = 0;
    EXPECT_EQ(replDecompressorDecode(rd, stream, sizeof(stream), 1024, &out_len), REPL_DECODE_ERR);
    replDecompressorDestroy(rd);
}

TEST(replCompression, decodeRejectsNonzeroReserved) {
    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    /* Valid magic/version/codec/kind, nonzero reserved byte. */
    unsigned char stream[VCS_ENVELOPE_SIZE + 4] = {'V', 'C', 'S', VCS_VERSION, VCS_CODEC_LZ4, 0x01,
                                                   VCS_STREAM_REPL, 0xDE, 0xAD, 0xBE, 0xEF};
    size_t out_len = 0;
    EXPECT_EQ(replDecompressorDecode(rd, stream, sizeof(stream), 1024, &out_len), REPL_DECODE_ERR);
    replDecompressorDestroy(rd);
}

TEST(replCompression, decodeErrOnCorruptPayload) {
    /* A real compressor emits the envelope on first write; reuse those bytes. */
    replCompressor *rc = replCompressorCreate(ALGO_LZ4);
    ASSERT_TRUE(rc != NULL);
    ASSERT_EQ(replCompressorWrite(rc, "seed", 4), C_OK);
    ASSERT_GE(sdslen(rc->out_buf), (size_t)VCS_ENVELOPE_SIZE);

    /* Valid envelope followed by garbage that LZ4F rejects. */
    unsigned char stream[VCS_ENVELOPE_SIZE + 64];
    memcpy(stream, rc->out_buf, VCS_ENVELOPE_SIZE);
    memset(stream + VCS_ENVELOPE_SIZE, 0xFF, 64);

    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    size_t out_len = 0;
    EXPECT_EQ(replDecompressorDecode(rd, stream, sizeof(stream), 1024 * 1024, &out_len),
              REPL_DECODE_ERR);

    replDecompressorDestroy(rd);
    replCompressorDestroy(rc);
}

TEST(replCompression, decodeDrainsBufferedOutputWithoutMoreInput) {
    /* The writer emits 64KB LZ4 blocks while the decoder offers 16KB of room
     * per iteration, so LZ4F decodes a compressed block into its internal
     * buffer and can report the block's input consumed with output still
     * undelivered. Once input runs out the decoder must keep draining with
     * empty input; otherwise the tail is stranded inside the codec until
     * later transport bytes arrive (worst case a 10s replication PING). The
     * payload ends with a compressible run: a stored (incompressible) block
     * streams straight to the caller's buffer and would not strand. */
    const size_t incompressible = 36 * 1024;
    const size_t compressible = 64 * 1024;
    const size_t n = incompressible + compressible; /* ~100KB: multiple blocks */
    unsigned char *payload = (unsigned char *)zmalloc(n);
    fillIncompressible(payload, incompressible, 0xC0FFEE42u);
    memset(payload + incompressible, 'A', compressible);

    replCompressor *rc = replCompressorCreate(ALGO_LZ4);
    ASSERT_TRUE(rc != NULL);
    ASSERT_EQ(replCompressorWrite(rc, payload, n), C_OK);
    ASSERT_EQ(replCompressorFlush(rc), C_OK); /* frame stays open */

    /* All compressed bytes in ONE call: no later input can push out whatever
     * the codec buffered, so the decode itself must drain it. */
    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    size_t out_len = 0;
    ASSERT_EQ(replDecompressorDecode(rd, rc->out_buf, sdslen(rc->out_buf), 4 * 1024 * 1024, &out_len),
              REPL_DECODE_OK);
    EXPECT_EQ(out_len, n);
    ASSERT_EQ(sdslen(replDecompressorBuf(rd)), n);
    EXPECT_EQ(memcmp(replDecompressorBuf(rd), payload, n), 0);

    replDecompressorDestroy(rd);
    replCompressorDestroy(rc);
    zfree(payload);
}

TEST(replCompression, decodeCallOutputStaysUnderCapAtMaxRatio) {
    /* Feed paths hand the decoder at most PROTO_IOBUF_LEN (16KB) per call and
     * LZ4 expansion is bounded, so one call's output stays far under the
     * 16MB overflow cap even for maximally compressible input. */
    const size_t n = 4 * 1024 * 1024; /* 4MB of one byte: near-max ratio */
    unsigned char *payload = (unsigned char *)zmalloc(n);
    memset(payload, 'Z', n);

    replCompressor *rc = replCompressorCreate(ALGO_LZ4);
    ASSERT_TRUE(rc != NULL);
    ASSERT_EQ(replCompressorWrite(rc, payload, n), C_OK);
    ASSERT_EQ(replCompressorFlush(rc), C_OK);

    /* One decode call fed a single clamped read (16KB of wire bytes): output
     * must stay far under the 16MB cap; a 255x bound on 16KB is ~4MB. */
    size_t chunk = sdslen(rc->out_buf);
    if (chunk > (size_t)16 * 1024) chunk = (size_t)16 * 1024;
    replDecompressor *rd = replDecompressorCreate();
    ASSERT_TRUE(rd != NULL);
    size_t out_len = 0;
    ASSERT_EQ(replDecompressorDecode(rd, rc->out_buf, chunk, 16 * 1024 * 1024, &out_len),
              REPL_DECODE_OK);
    EXPECT_GT(out_len, (size_t)1024 * 1024); /* high ratio actually exercised */
    EXPECT_LT(out_len, (size_t)16 * 1024 * 1024);

    replDecompressorDestroy(rd);
    replCompressorDestroy(rc);
    zfree(payload);
}
