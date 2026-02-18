/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Property-based tests for the compression module envelope.
 *
 * **Property: Envelope Format Compliance**
 * **Validates: Requirements 2.2, 2.3, 2.4, 2.15, 2.19** */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../compression.h"
#include "../zmalloc.h"
#include "test_help.h"

/* --- Emit callback that writes into a flat buffer --- */
typedef struct {
    uint8_t buf[64];
    size_t pos;
} emit_buf_t;

static int emitToBuf(void *ctx, const uint8_t *data, size_t len) {
    emit_buf_t *eb = (emit_buf_t *)ctx;
    assert(eb->pos + len <= sizeof(eb->buf)); /* crash loudly in tests */
    memcpy(eb->buf + eb->pos, data, len);
    eb->pos += len;
    return 0;
}

/* --- Property: Envelope round-trip ---
 * For every valid (algo, stream_kind) pair, writeVkcsEnvelope followed by
 * readVkcsEnvelope must recover the original algo and stream_kind. */
int test_envelopeRoundTrip(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    compression_algo_t algos[] = {ALGO_LZ4, ALGO_ZSTD};
    uint8_t kinds[] = {STREAM_KIND_RDB, STREAM_KIND_REPL};

    for (int a = 0; a < 2; a++) {
        for (int k = 0; k < 2; k++) {
            emit_buf_t eb = {.pos = 0};
            int wret = writeVkcsEnvelope(emitToBuf, &eb, algos[a], kinds[k]);
            TEST_ASSERT_MESSAGE("writeVkcsEnvelope should succeed for valid params", wret == 0);
            TEST_ASSERT_MESSAGE("envelope should be exactly 8 bytes", eb.pos == VKCS_ENVELOPE_SIZE);

            compression_algo_t got_algo = ALGO_NONE;
            uint8_t got_kind = 0xFF;
            int rret = readVkcsEnvelope(eb.buf, eb.pos, &got_algo, &got_kind);
            TEST_ASSERT_MESSAGE("readVkcsEnvelope should succeed", rret == 0);
            TEST_ASSERT_MESSAGE("round-trip algo must match", got_algo == algos[a]);
            TEST_ASSERT_MESSAGE("round-trip stream_kind must match", got_kind == kinds[k]);
        }
    }
    return 0;
}

/* --- Property: Envelope magic bytes are "VKCS" (Req 2.3) --- */
int test_envelopeMagicBytes(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    emit_buf_t eb = {.pos = 0};
    int ret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB);
    TEST_ASSERT_MESSAGE("write must succeed", ret == 0);

    TEST_ASSERT_MESSAGE("magic[0] == 'V'", eb.buf[0] == 0x56);
    TEST_ASSERT_MESSAGE("magic[1] == 'K'", eb.buf[1] == 0x4B);
    TEST_ASSERT_MESSAGE("magic[2] == 'C'", eb.buf[2] == 0x43);
    TEST_ASSERT_MESSAGE("magic[3] == 'S'", eb.buf[3] == 0x53);
    TEST_ASSERT_MESSAGE("version == VKCS_VERSION", eb.buf[4] == VKCS_VERSION);
    TEST_ASSERT_MESSAGE("reserved == 0", eb.buf[7] == 0);
    return 0;
}

/* --- Property: Envelope flags encode stream_kind in bit 0 (Req 2.4) --- */
int test_envelopeStreamKindFlag(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* RDB: bit 0 = 0 */
    emit_buf_t eb_rdb = {.pos = 0};
    int ret = writeVkcsEnvelope(emitToBuf, &eb_rdb, ALGO_LZ4, STREAM_KIND_RDB);
    TEST_ASSERT_MESSAGE("write RDB must succeed", ret == 0);
    TEST_ASSERT_MESSAGE("RDB stream_kind: flags bit 0 == 0", (eb_rdb.buf[6] & 0x01) == 0);

    /* REPL: bit 0 = 1 */
    emit_buf_t eb_repl = {.pos = 0};
    ret = writeVkcsEnvelope(emitToBuf, &eb_repl, ALGO_LZ4, STREAM_KIND_REPL);
    TEST_ASSERT_MESSAGE("write REPL must succeed", ret == 0);
    TEST_ASSERT_MESSAGE("REPL stream_kind: flags bit 0 == 1", (eb_repl.buf[6] & 0x01) == 1);
    return 0;
}

/* --- Property: Unrecognized algo_id is rejected (Req 2.15) --- */
int test_envelopeRejectsUnknownAlgo(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Build a valid envelope, then corrupt the algo_id byte */
    emit_buf_t eb = {.pos = 0};
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB);
    TEST_ASSERT_MESSAGE("write must succeed", wret == 0);

    /* Try every invalid algo_id value 0..255 except ALGO_LZ4 and ALGO_ZSTD */
    for (int i = 0; i < 256; i++) {
        if (i == ALGO_LZ4 || i == ALGO_ZSTD) continue;
        eb.buf[5] = (uint8_t)i;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k);
        TEST_ASSERT_MESSAGE("readVkcsEnvelope must reject unknown algo_id", ret == -1);
    }
    return 0;
}

/* --- Property: write rejects non-streaming algorithms --- */
int test_envelopeRejectsNonStreamingAlgo(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    emit_buf_t eb = {.pos = 0};
    TEST_ASSERT_MESSAGE("ALGO_NONE rejected", writeVkcsEnvelope(emitToBuf, &eb, ALGO_NONE, STREAM_KIND_RDB) == -1);
    TEST_ASSERT_MESSAGE("ALGO_LZF rejected", writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZF, STREAM_KIND_RDB) == -1);
    return 0;
}

/* --- Property: readVkcsEnvelope rejects truncated input --- */
int test_envelopeRejectsTruncated(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    emit_buf_t eb = {.pos = 0};
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB);
    TEST_ASSERT_MESSAGE("write must succeed", wret == 0);

    /* Every length < 8 must fail */
    for (size_t l = 0; l < VKCS_ENVELOPE_SIZE; l++) {
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, l, &a, &k);
        TEST_ASSERT_MESSAGE("truncated envelope must be rejected", ret == -1);
    }
    return 0;
}

/* --- Property: readVkcsEnvelope rejects bad magic --- */
int test_envelopeRejectsBadMagic(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    emit_buf_t eb = {.pos = 0};
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB);
    TEST_ASSERT_MESSAGE("write must succeed", wret == 0);

    /* Flip each magic byte and verify rejection */
    for (int i = 0; i < 4; i++) {
        uint8_t orig = eb.buf[i];
        eb.buf[i] = ~orig;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k);
        TEST_ASSERT_MESSAGE("bad magic must be rejected", ret == -1);
        eb.buf[i] = orig;
    }
    return 0;
}

/* --- Property: readVkcsEnvelope rejects reserved bits set (Req 2.19) --- */
int test_envelopeRejectsReservedBits(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    emit_buf_t eb = {.pos = 0};
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB);
    TEST_ASSERT_MESSAGE("write must succeed", wret == 0);

    /* Setting any reserved flag bit (1-7) must cause rejection */
    for (int bit = 1; bit < 8; bit++) {
        uint8_t orig = eb.buf[6];
        eb.buf[6] = orig | (1 << bit);
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k);
        TEST_ASSERT_MESSAGE("reserved flag bits must be rejected", ret == -1);
        eb.buf[6] = orig;
    }

    /* Non-zero reserved byte [7] must cause rejection */
    for (int val = 1; val < 256; val++) {
        eb.buf[7] = (uint8_t)val;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k);
        TEST_ASSERT_MESSAGE("non-zero reserved byte must be rejected", ret == -1);
    }
    eb.buf[7] = 0;

    return 0;
}

/* --- Property: Bit-flip fuzz — write valid envelope, flip random bit,
 * verify readVkcsEnvelope exercises rejection paths.
 * Replaces the previous random round-trip test which only covered 4
 * combinations and added no coverage beyond the exhaustive test. */
int test_envelopeBitFlipFuzz(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);

    int iterations = (flags & UNIT_TEST_ACCURATE) ? 10000 : 1000;
    compression_algo_t algos[] = {ALGO_LZ4, ALGO_ZSTD};
    uint8_t kinds[] = {STREAM_KIND_RDB, STREAM_KIND_REPL};

    /* Use random() seeded by the test harness for reproducibility via --seed */
    for (int i = 0; i < iterations; i++) {
        compression_algo_t algo = algos[random() % 2];
        uint8_t kind = kinds[random() % 2];

        emit_buf_t eb = {.pos = 0};
        int wret = writeVkcsEnvelope(emitToBuf, &eb, algo, kind);
        TEST_ASSERT_MESSAGE("write must succeed", wret == 0);
        TEST_ASSERT_MESSAGE("size must be 8", eb.pos == VKCS_ENVELOPE_SIZE);

        /* Flip a random bit in the envelope */
        int bit = random() % (VKCS_ENVELOPE_SIZE * 8);
        eb.buf[bit / 8] ^= (1 << (bit % 8));

        compression_algo_t got_algo;
        uint8_t got_kind;
        /* Most flips should cause rejection; some may land on don't-care
         * bits and still parse. When parse succeeds, validate the returned
         * values are valid enum members. */
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &got_algo, &got_kind);
        if (ret == 0) {
            TEST_ASSERT_MESSAGE("parsed algo must be LZ4 or ZSTD",
                                got_algo == ALGO_LZ4 || got_algo == ALGO_ZSTD);
            TEST_ASSERT_MESSAGE("parsed kind must be RDB or REPL",
                                got_kind == STREAM_KIND_RDB || got_kind == STREAM_KIND_REPL);
        }
    }
    return 0;
}

/* --- Property: emit_cb failure propagates through writeVkcsEnvelope --- */
static int emitAlwaysFail(void *ctx, const uint8_t *data, size_t len) {
    (void)ctx;
    (void)data;
    (void)len;
    return -1;
}

int test_envelopeEmitFailure(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int ret = writeVkcsEnvelope(emitAlwaysFail, NULL, ALGO_LZ4, STREAM_KIND_RDB);
    TEST_ASSERT_MESSAGE("writeVkcsEnvelope must propagate emit_cb failure", ret == -1);
    return 0;
}

/* ===================================================================
 * Streaming compression/decompression tests
 * =================================================================== */

/* --- Test: LZ4 compressor init/destroy lifecycle --- */
int test_streamCompressorInitDestroy(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    stream_compressor_t sc;
    TEST_ASSERT_MESSAGE("LZ4 init should succeed",
                        streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);
    TEST_ASSERT_MESSAGE("algo should be LZ4", sc.algo == ALGO_LZ4);
    TEST_ASSERT_MESSAGE("frame_started should be false", sc.frame_started == false);
    TEST_ASSERT_MESSAGE("ctx should be non-NULL", sc.ctx.lz4f != NULL);
    streamCompressorDestroy(&sc);
    TEST_ASSERT_MESSAGE("ctx should be NULL after destroy", sc.ctx.lz4f == NULL);
    TEST_ASSERT_MESSAGE("algo should be NONE after destroy", sc.algo == ALGO_NONE);

    /* ZSTD should fail (not yet implemented) */
    stream_compressor_t sc2;
    TEST_ASSERT_MESSAGE("ZSTD init should fail",
                        streamCompressorInit(&sc2, ALGO_ZSTD, 0) == -1);
    TEST_ASSERT_MESSAGE("algo should be NONE after failed init", sc2.algo == ALGO_NONE);

    /* ALGO_NONE should fail */
    stream_compressor_t sc3;
    TEST_ASSERT_MESSAGE("NONE init should fail",
                        streamCompressorInit(&sc3, ALGO_NONE, 0) == -1);

    /* NULL should fail */
    TEST_ASSERT_MESSAGE("NULL init should fail",
                        streamCompressorInit(NULL, ALGO_LZ4, 0) == -1);

    return 0;
}

/* --- Test: LZ4 decompressor init/destroy lifecycle --- */
int test_streamDecompressorInitDestroy(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    stream_decompressor_t sd;
    TEST_ASSERT_MESSAGE("LZ4 decomp init should succeed",
                        streamDecompressorInit(&sd, ALGO_LZ4) == 0);
    TEST_ASSERT_MESSAGE("algo should be LZ4", sd.algo == ALGO_LZ4);
    TEST_ASSERT_MESSAGE("ctx should be non-NULL", sd.ctx.lz4f != NULL);
    streamDecompressorDestroy(&sd);
    TEST_ASSERT_MESSAGE("ctx should be NULL after destroy", sd.ctx.lz4f == NULL);
    TEST_ASSERT_MESSAGE("algo should be NONE after destroy", sd.algo == ALGO_NONE);

    /* ZSTD should fail */
    stream_decompressor_t sd2;
    TEST_ASSERT_MESSAGE("ZSTD decomp init should fail",
                        streamDecompressorInit(&sd2, ALGO_ZSTD) == -1);
    TEST_ASSERT_MESSAGE("algo should be NONE after failed init", sd2.algo == ALGO_NONE);

    return 0;
}

/* --- Test: LZ4 compress → decompress round-trip --- */
int test_streamCompressDecompressRoundTrip(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    const char *input = "Hello, Valkey compression module! This is a test payload.";
    size_t input_len = strlen(input);

    /* Compress */
    stream_compressor_t sc;
    TEST_ASSERT(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);

    size_t bound = streamCompressOutputBound(ALGO_LZ4, input_len, 0, FLUSH_END);
    TEST_ASSERT_MESSAGE("bound should be > 0", bound > 0);

    uint8_t *compressed = zmalloc(bound);
    TEST_ASSERT(compressed != NULL);
    uint8_t *out_ptr = compressed;

    ssize_t compressed_len = streamCompressFeed(&sc, &out_ptr, bound,
                                                (const uint8_t *)input, input_len,
                                                FLUSH_END);
    TEST_ASSERT_MESSAGE("compress should succeed", compressed_len > 0);
    TEST_ASSERT_MESSAGE("frame should be closed after FLUSH_END",
                        sc.frame_started == false);
    streamCompressorDestroy(&sc);

    /* Decompress */
    stream_decompressor_t sd;
    TEST_ASSERT(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[256];
    size_t input_consumed = 0;
    ssize_t decompressed_len = streamDecompressFeed(&sd, decompressed, sizeof(decompressed),
                                                    compressed, (size_t)compressed_len,
                                                    &input_consumed);
    TEST_ASSERT_MESSAGE("decompress should succeed", decompressed_len > 0);
    TEST_ASSERT_MESSAGE("decompressed length should match input",
                        (size_t)decompressed_len == input_len);
    TEST_ASSERT_MESSAGE("decompressed content should match input",
                        memcmp(decompressed, input, input_len) == 0);
    TEST_ASSERT_MESSAGE("all compressed input should be consumed",
                        input_consumed == (size_t)compressed_len);

    streamDecompressorDestroy(&sd);
    zfree(compressed);
    return 0;
}

/* --- Test: streamCompressOutputBound returns sane values --- */
int test_streamCompressOutputBound(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Basic: bound for 1KB input should be > 0 */
    size_t b1 = streamCompressOutputBound(ALGO_LZ4, 1024, 0, FLUSH_CONTINUE);
    TEST_ASSERT_MESSAGE("bound for 1KB continue should be > 0", b1 > 0);

    /* Bound with frame header should be larger than without */
    size_t b_no_frame = streamCompressOutputBound(ALGO_LZ4, 1024, 1, FLUSH_CONTINUE);
    size_t b_with_frame = streamCompressOutputBound(ALGO_LZ4, 1024, 0, FLUSH_CONTINUE);
    TEST_ASSERT_MESSAGE("bound with frame header should be >= without frame",
                        b_with_frame >= b_no_frame);

    /* Flush bound should be >= continue bound */
    size_t b_flush = streamCompressOutputBound(ALGO_LZ4, 1024, 1, FLUSH_SYNC);
    size_t b_cont = streamCompressOutputBound(ALGO_LZ4, 1024, 1, FLUSH_CONTINUE);
    TEST_ASSERT_MESSAGE("flush bound should be >= continue bound",
                        b_flush >= b_cont);

    /* End bound should be >= flush bound */
    size_t b_end = streamCompressOutputBound(ALGO_LZ4, 1024, 1, FLUSH_END);
    TEST_ASSERT_MESSAGE("end bound should be >= flush bound",
                        b_end >= b_flush);

    /* Zero input should still return > 0 for flush/end (internal buffering) */
    size_t b_zero_flush = streamCompressOutputBound(ALGO_LZ4, 0, 1, FLUSH_SYNC);
    TEST_ASSERT_MESSAGE("zero input flush bound should be > 0", b_zero_flush > 0);

    return 0;
}

/* --- Test: streamCompressFeed error paths --- */
int test_streamCompressFeedErrors(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    uint8_t buf[64];
    uint8_t *ptr = buf;

    /* NULL compressor */
    TEST_ASSERT_MESSAGE("NULL sc should return -1",
                        streamCompressFeed(NULL, &ptr, sizeof(buf),
                                           (const uint8_t *)"x", 1, FLUSH_CONTINUE) == -1);

    /* NULL output_ptr */
    stream_compressor_t sc;
    TEST_ASSERT(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);
    TEST_ASSERT_MESSAGE("NULL output_ptr should return -1",
                        streamCompressFeed(&sc, NULL, sizeof(buf),
                                           (const uint8_t *)"x", 1, FLUSH_CONTINUE) == -1);
    streamCompressorDestroy(&sc);

    return 0;
}

/* --- Test: streamDecompressFeed error paths --- */
int test_streamDecompressFeedErrors(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    uint8_t buf[64];
    size_t consumed = 0;

    /* NULL decompressor */
    TEST_ASSERT_MESSAGE("NULL sd should return -1",
                        streamDecompressFeed(NULL, buf, sizeof(buf),
                                             (const uint8_t *)"x", 1, &consumed) == -1);

    /* NULL input_consumed */
    stream_decompressor_t sd;
    TEST_ASSERT(streamDecompressorInit(&sd, ALGO_LZ4) == 0);
    TEST_ASSERT_MESSAGE("NULL input_consumed should return -1",
                        streamDecompressFeed(&sd, buf, sizeof(buf),
                                             (const uint8_t *)"x", 1, NULL) == -1);

    /* Zero output capacity should return -1 (no-progress prevention) */
    TEST_ASSERT_MESSAGE("zero output capacity should return -1",
                        streamDecompressFeed(&sd, buf, 0,
                                             (const uint8_t *)"x", 1, &consumed) == -1);

    streamDecompressorDestroy(&sd);
    return 0;
}

/* --- Test: pre-frame errors are recoverable, mid-frame errors are permanent --- */
int test_streamCompressFeedErrorRecovery(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    stream_compressor_t sc;
    TEST_ASSERT(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);

    /* Pre-frame error: compressBegin fails with tiny buffer, but no frame
     * bytes have been emitted yet — this is recoverable. */
    uint8_t tiny[1];
    uint8_t *ptr = tiny;
    ssize_t ret = streamCompressFeed(&sc, &ptr, 1,
                                     (const uint8_t *)"test data", 9, FLUSH_END);
    TEST_ASSERT_MESSAGE("should fail with tiny buffer", ret == -1);
    TEST_ASSERT_MESSAGE("errored should NOT be set (pre-frame failure)",
                        sc.errored == false);
    TEST_ASSERT_MESSAGE("frame_started should still be false",
                        sc.frame_started == false);

    /* Retry with a proper buffer — should succeed */
    size_t bound = streamCompressOutputBound(ALGO_LZ4, 9, 0, FLUSH_END);
    uint8_t *buf = zmalloc(bound);
    uint8_t *ptr2 = buf;
    ssize_t ret2 = streamCompressFeed(&sc, &ptr2, bound,
                                      (const uint8_t *)"test data", 9, FLUSH_END);
    TEST_ASSERT_MESSAGE("retry after pre-frame error should succeed", ret2 > 0);
    zfree(buf);
    streamCompressorDestroy(&sc);

    /* Mid-frame error: start a frame, then force an error — this is permanent. */
    stream_compressor_t sc2;
    TEST_ASSERT(streamCompressorInit(&sc2, ALGO_LZ4, 0) == 0);

    /* First call with enough space to start the frame */
    size_t bound2 = streamCompressOutputBound(ALGO_LZ4, 5, 0, FLUSH_CONTINUE);
    uint8_t *buf2 = zmalloc(bound2);
    uint8_t *ptr3 = buf2;
    ssize_t ret3 = streamCompressFeed(&sc2, &ptr3, bound2,
                                      (const uint8_t *)"hello", 5, FLUSH_CONTINUE);
    TEST_ASSERT_MESSAGE("first write should succeed", ret3 >= 0);
    TEST_ASSERT_MESSAGE("frame should be started", sc2.frame_started == true);

    /* Now force a mid-frame error with a tiny buffer */
    uint8_t tiny2[1];
    uint8_t *ptr4 = tiny2;
    ssize_t ret4 = streamCompressFeed(&sc2, &ptr4, 1,
                                      (const uint8_t *)"more data to compress", 21,
                                      FLUSH_END);
    TEST_ASSERT_MESSAGE("mid-frame error should fail", ret4 == -1);
    TEST_ASSERT_MESSAGE("errored should be set (mid-frame failure)",
                        sc2.errored == true);

    /* Subsequent calls must fail immediately */
    size_t bound3 = streamCompressOutputBound(ALGO_LZ4, 5, 0, FLUSH_END);
    uint8_t *buf3 = zmalloc(bound3);
    uint8_t *ptr5 = buf3;
    ssize_t ret5 = streamCompressFeed(&sc2, &ptr5, bound3,
                                      (const uint8_t *)"hello", 5, FLUSH_END);
    TEST_ASSERT_MESSAGE("must fail on errored compressor", ret5 == -1);
    zfree(buf3);
    zfree(buf2);
    streamCompressorDestroy(&sc2);

    return 0;
}
