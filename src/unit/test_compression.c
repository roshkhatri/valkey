/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Property-based tests for the compression module envelope.
 *
 * **Property: Envelope Format Compliance**
 * **Validates: Requirements 2.2, 2.3, 2.4, 2.15, 2.19** */

#include "../compression.h"
#include "../compression_stream.h"
#include "../zmalloc.h"
#include "test_help.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* --- Emit callback that writes into a flat buffer --- */
typedef struct {
    uint8_t buf[64];
    size_t pos;
} emit_buf_t;

typedef struct {
    const uint8_t *data;
    size_t len;
    size_t pos;
    size_t max_chunk; /* 0 => unbounded */
} mem_reader_t;

typedef struct {
    const uint8_t *data;
    size_t len;
    size_t pos;
    size_t max_chunk;
    int fail_after_success_reads;
    int success_reads;
} flaky_reader_t;

static int emitToBuf(void *ctx, const uint8_t *data, size_t len) {
    emit_buf_t *eb = (emit_buf_t *)ctx;
    assert(eb->pos + len <= sizeof(eb->buf)); /* crash loudly in tests */
    memcpy(eb->buf + eb->pos, data, len);
    eb->pos += len;
    return 0;
}

static ssize_t memReaderRead(void *ctx, void *buf, size_t len) {
    mem_reader_t *r = (mem_reader_t *)ctx;
    if (!r || !buf) return -1;
    if (r->pos >= r->len) return 0;

    size_t avail = r->len - r->pos;
    size_t n = len < avail ? len : avail;
    if (r->max_chunk && n > r->max_chunk) n = r->max_chunk;

    memcpy(buf, r->data + r->pos, n);
    r->pos += n;
    return (ssize_t)n;
}

static ssize_t flakyReaderRead(void *ctx, void *buf, size_t len) {
    flaky_reader_t *r = (flaky_reader_t *)ctx;
    if (!r || !buf) return -1;
    if (r->success_reads >= r->fail_after_success_reads) return -1;
    if (r->pos >= r->len) return 0;

    size_t avail = r->len - r->pos;
    size_t n = len < avail ? len : avail;
    if (r->max_chunk && n > r->max_chunk) n = r->max_chunk;

    memcpy(buf, r->data + r->pos, n);
    r->pos += n;
    r->success_reads++;
    return (ssize_t)n;
}

/* --- Property: Envelope round-trip ---
 * For every valid (algo, stream_kind, checksum flag) tuple, writeVkcsEnvelope
 * followed by readVkcsEnvelope must recover the original fields. */
int test_envelopeRoundTrip(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    compression_algo_t algos[] = {ALGO_LZ4};
    size_t algo_count = sizeof(algos) / sizeof(algos[0]);
    uint8_t kinds[] = {STREAM_KIND_RDB, STREAM_KIND_REPL};

    for (size_t a = 0; a < algo_count; a++) {
        for (int k = 0; k < 2; k++) {
            for (int checksum_enabled = 0; checksum_enabled <= 1; checksum_enabled++) {
                emit_buf_t eb = {.pos = 0};
                int wret = writeVkcsEnvelope(emitToBuf, &eb, algos[a], kinds[k], checksum_enabled);
                TEST_ASSERT_MESSAGE("writeVkcsEnvelope should succeed for valid params", wret == 0);
                TEST_ASSERT_MESSAGE("envelope should be exactly 8 bytes", eb.pos == VKCS_ENVELOPE_SIZE);

                compression_algo_t got_algo = ALGO_NONE;
                uint8_t got_kind = 0xFF;
                int got_checksum_enabled = -1;
                int rret = readVkcsEnvelope(eb.buf, eb.pos, &got_algo, &got_kind, &got_checksum_enabled);
                TEST_ASSERT_MESSAGE("readVkcsEnvelope should succeed", rret == 0);
                TEST_ASSERT_MESSAGE("round-trip algo must match", got_algo == algos[a]);
                TEST_ASSERT_MESSAGE("round-trip stream_kind must match", got_kind == kinds[k]);
                TEST_ASSERT_MESSAGE("round-trip checksum flag must match", got_checksum_enabled == checksum_enabled);
            }
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
    int ret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
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
    int ret = writeVkcsEnvelope(emitToBuf, &eb_rdb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    TEST_ASSERT_MESSAGE("write RDB must succeed", ret == 0);
    TEST_ASSERT_MESSAGE("RDB stream_kind: flags bit 0 == 0", (eb_rdb.buf[6] & 0x01) == 0);
    TEST_ASSERT_MESSAGE("RDB stream_kind: no extra bits set", (eb_rdb.buf[6] & (uint8_t)~VKCS_FLAG_STREAM_KIND) == 0);

    /* REPL: bit 0 = 1 */
    emit_buf_t eb_repl = {.pos = 0};
    ret = writeVkcsEnvelope(emitToBuf, &eb_repl, ALGO_LZ4, STREAM_KIND_REPL, 0);
    TEST_ASSERT_MESSAGE("write REPL must succeed", ret == 0);
    TEST_ASSERT_MESSAGE("REPL stream_kind: flags bit 0 == 1", (eb_repl.buf[6] & 0x01) == 1);
    TEST_ASSERT_MESSAGE("REPL stream_kind: no extra bits set", (eb_repl.buf[6] & (uint8_t)~VKCS_FLAG_STREAM_KIND) == 0);
    return 0;
}

/* --- Property: Unrecognized algo_id is rejected (Req 2.15) --- */
int test_envelopeRejectsUnknownAlgo(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Build a valid envelope, then corrupt the algo_id byte */
    emit_buf_t eb = {.pos = 0};
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    TEST_ASSERT_MESSAGE("write must succeed", wret == 0);

    /* Try every invalid algo_id value 0..255 except ALGO_LZ4 */
    for (int i = 0; i < 256; i++) {
        if (i == ALGO_LZ4) continue;
        eb.buf[5] = (uint8_t)i;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k, NULL);
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
    TEST_ASSERT_MESSAGE("ALGO_NONE rejected", writeVkcsEnvelope(emitToBuf, &eb, ALGO_NONE, STREAM_KIND_RDB, 0) == -1);
    TEST_ASSERT_MESSAGE("ALGO_LZF rejected", writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZF, STREAM_KIND_RDB, 0) == -1);
    return 0;
}

/* --- Property: readVkcsEnvelope rejects truncated input --- */
int test_envelopeRejectsTruncated(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    emit_buf_t eb = {.pos = 0};
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    TEST_ASSERT_MESSAGE("write must succeed", wret == 0);

    /* Every length < 8 must fail */
    for (size_t l = 0; l < VKCS_ENVELOPE_SIZE; l++) {
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, l, &a, &k, NULL);
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
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    TEST_ASSERT_MESSAGE("write must succeed", wret == 0);

    /* Flip each magic byte and verify rejection */
    for (int i = 0; i < 4; i++) {
        uint8_t orig = eb.buf[i];
        eb.buf[i] = ~orig;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k, NULL);
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
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    TEST_ASSERT_MESSAGE("write must succeed", wret == 0);

    /* Setting any reserved flag bit (2-7) must cause rejection */
    for (int bit = 2; bit < 8; bit++) {
        uint8_t orig = eb.buf[6];
        eb.buf[6] = orig | (1 << bit);
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k, NULL);
        TEST_ASSERT_MESSAGE("reserved flag bits must be rejected", ret == -1);
        eb.buf[6] = orig;
    }

    /* Non-zero reserved byte [7] must cause rejection */
    for (int val = 1; val < 256; val++) {
        eb.buf[7] = (uint8_t)val;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k, NULL);
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
    compression_algo_t algos[] = {ALGO_LZ4};
    size_t algo_count = sizeof(algos) / sizeof(algos[0]);
    uint8_t kinds[] = {STREAM_KIND_RDB, STREAM_KIND_REPL};

    /* Use random() seeded by the test harness for reproducibility via --seed */
    for (int i = 0; i < iterations; i++) {
        compression_algo_t algo = algos[random() % algo_count];
        uint8_t kind = kinds[random() % 2];

        emit_buf_t eb = {.pos = 0};
        int wret = writeVkcsEnvelope(emitToBuf, &eb, algo, kind, 0);
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
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &got_algo, &got_kind, NULL);
        if (ret == 0) {
            TEST_ASSERT_MESSAGE("parsed algo must be LZ4", got_algo == ALGO_LZ4);
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

    int ret = writeVkcsEnvelope(emitAlwaysFail, NULL, ALGO_LZ4, STREAM_KIND_RDB, 0);
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

    const char *payload = "decompress sticky error";
    uint8_t buf[64];
    uint8_t out[128];
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
    TEST_ASSERT_MESSAGE("decompressor should not be errored by NULL bookkeeping arg",
                        sd.errored == false);

    /* Zero output capacity should return -1 (no-progress prevention) */
    TEST_ASSERT_MESSAGE("zero output capacity should return -1",
                        streamDecompressFeed(&sd, buf, 0,
                                             (const uint8_t *)"x", 1, &consumed) == -1);
    TEST_ASSERT_MESSAGE("decompressor should enter sticky errored state",
                        sd.errored == true);

    /* Once errored, all subsequent feeds fail immediately. */
    stream_compressor_t sc;
    TEST_ASSERT(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);
    size_t bound = streamCompressOutputBound(ALGO_LZ4, strlen(payload), 0, FLUSH_END);
    uint8_t *compressed = zmalloc(bound);
    TEST_ASSERT(compressed != NULL);
    uint8_t *out_ptr = compressed;
    ssize_t compressed_len = streamCompressFeed(&sc, &out_ptr, bound,
                                                (const uint8_t *)payload, strlen(payload),
                                                FLUSH_END);
    TEST_ASSERT(compressed_len > 0);
    streamCompressorDestroy(&sc);

    TEST_ASSERT_MESSAGE("errored decompressor should fail even with valid input",
                        streamDecompressFeed(&sd, out, sizeof(out),
                                             compressed, (size_t)compressed_len,
                                             &consumed) == -1);
    zfree(compressed);

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

/* --- Test: stream_reader passes through truncated non-VKCS input. --- */
int test_streamReaderTruncatedPassthrough(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    const uint8_t input[] = {'H', 'E', 'L', 'L', 'O'};
    mem_reader_t mr = {.data = input, .len = sizeof(input), .pos = 0, .max_chunk = 2};
    stream_reader_config_t cfg = {
        .algo = ALGO_NONE,
        .expected_stream_kind = STREAM_KIND_ANY,
        .raw_frame = 0,
        .allow_passthrough = 1,
        .batch_size = 0,
    };

    stream_reader_t *t = stream_reader_create(&cfg, memReaderRead, &mr);
    TEST_ASSERT_MESSAGE("stream_reader_create should succeed", t != NULL);

    stream_reader_info_t info;
    TEST_ASSERT_MESSAGE("stream_reader_get_info should succeed", stream_reader_get_info(t, &info) == 0);
    TEST_ASSERT_MESSAGE("truncated input should be treated as passthrough", info.compressed == 0);

    uint8_t out[16];
    size_t out_len = 0;
    while (out_len < sizeof(input)) {
        ssize_t n = stream_reader_read(t, out + out_len, sizeof(out) - out_len);
        TEST_ASSERT_MESSAGE("stream_reader_read should produce bytes", n > 0);
        out_len += (size_t)n;
    }
    TEST_ASSERT_MESSAGE("passthrough bytes should match",
                        out_len == sizeof(input) &&
                            memcmp(out, input, sizeof(input)) == 0);
    TEST_ASSERT_MESSAGE("stream_reader_read should return EOF after payload",
                        stream_reader_read(t, out, sizeof(out)) == 0);

    stream_reader_destroy(t);
    return 0;
}

/* --- Test: stream_reader rejects malformed VKCS envelope. --- */
int test_streamReaderRejectsInvalidVkcs(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Valid magic + invalid version (0) */
    const uint8_t input[VKCS_ENVELOPE_SIZE] = {
        VKCS_MAGIC_0, VKCS_MAGIC_1, VKCS_MAGIC_2, VKCS_MAGIC_3,
        0, ALGO_LZ4, STREAM_KIND_RDB, 0};
    mem_reader_t mr = {.data = input, .len = sizeof(input), .pos = 0, .max_chunk = 0};
    stream_reader_config_t cfg = {
        .algo = ALGO_NONE,
        .expected_stream_kind = STREAM_KIND_ANY,
        .raw_frame = 0,
        .allow_passthrough = 1,
        .batch_size = 0,
    };

    stream_reader_t *t = stream_reader_create(&cfg, memReaderRead, &mr);
    TEST_ASSERT_MESSAGE("stream_reader_create should succeed", t != NULL);

    TEST_ASSERT_MESSAGE("stream_reader_probe should fail on malformed VKCS",
                        stream_reader_probe(t) == -1);
    stream_reader_info_t info;
    TEST_ASSERT_MESSAGE("stream_reader_get_info should fail after malformed VKCS",
                        stream_reader_get_info(t, &info) == -1);

    stream_reader_destroy(t);
    return 0;
}

/* --- Test: stream_reader rejects non-VKCS input when passthrough is disabled. --- */
int test_streamReaderRejectsNonVkcsWhenPassthroughDisabled(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    const uint8_t input[VKCS_ENVELOPE_SIZE] = {'R', 'E', 'D', 'I', 'S', '0', '0', '1'};
    mem_reader_t mr = {.data = input, .len = sizeof(input), .pos = 0, .max_chunk = 0};
    stream_reader_config_t cfg = {
        .algo = ALGO_NONE,
        .expected_stream_kind = STREAM_KIND_RDB,
        .raw_frame = 0,
        .allow_passthrough = 0,
        .batch_size = 0,
    };

    stream_reader_t *t = stream_reader_create(&cfg, memReaderRead, &mr);
    TEST_ASSERT_MESSAGE("stream_reader_create should succeed", t != NULL);

    TEST_ASSERT_MESSAGE("stream_reader_probe should reject non-VKCS when passthrough disabled",
                        stream_reader_probe(t) == -1);
    TEST_ASSERT_MESSAGE("stream_reader_read should fail after probe error",
                        stream_reader_read(t, (uint8_t[8]){0}, 8) == -1);

    stream_reader_destroy(t);
    return 0;
}

/* ===================================================================
 * Tests for stream writer API and rio decorators (Tasks 3.1-3.6)
 * =================================================================== */

#include "../compression_rio.h"
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len);

/* --- Emit callback that appends to a dynamically growing buffer --- */
typedef struct {
    uint8_t *data;
    size_t len;
    size_t capacity;
} dynamic_buf_t;

static void dynamicBufInit(dynamic_buf_t *db) {
    db->data = zmalloc(4096);
    db->len = 0;
    db->capacity = 4096;
}

static void dynamicBufFree(dynamic_buf_t *db) {
    if (db->data) zfree(db->data);
    db->data = NULL;
    db->len = 0;
    db->capacity = 0;
}

static int emitToDynamicBuf(void *ctx, const uint8_t *data, size_t len) {
    dynamic_buf_t *db = (dynamic_buf_t *)ctx;
    while (db->len + len > db->capacity) {
        db->capacity *= 2;
        db->data = zrealloc(db->data, db->capacity);
    }
    memcpy(db->data + db->len, data, len);
    db->len += len;
    return 0;
}

static int initRawLz4DecompressRio(decompress_rio_t *dr, rio *inner) {
    stream_reader_config_t cfg = {
        .algo = ALGO_LZ4,
        .expected_stream_kind = STREAM_KIND_ANY,
        .raw_frame = 1,
        .allow_passthrough = 0,
        .batch_size = 0,
    };
    return decompress_rio_init_with_config(dr, inner, &cfg);
}

/* --- Test: stream_reader marks errored on partial output + read error.
 * Regression for direct-path error accounting (partial bytes were returned
 * without sticky errored state). */
int test_streamReaderPartialThenErrorSetsErrored(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    const size_t payload_len = 256 * 1024;
    uint8_t *payload = zmalloc(payload_len);
    uint32_t x = 0x12345678u;
    for (size_t i = 0; i < payload_len; i++) {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        payload[i] = (uint8_t)(x & 0xFF);
    }

    dynamic_buf_t db;
    dynamicBufInit(&db);
    stream_writer_config_t wcfg = {
        .algo = ALGO_LZ4,
        .level = 0,
        .stream_kind = STREAM_KIND_RDB,
    };
    stream_writer_t *w = stream_writer_create(&wcfg, emitToDynamicBuf, &db);
    TEST_ASSERT_MESSAGE("stream_writer_create should succeed", w != NULL);
    TEST_ASSERT(stream_writer_write(w, payload, payload_len) == 0);
    TEST_ASSERT(stream_writer_finish(w) == 0);
    stream_writer_destroy(w);

    flaky_reader_t fr = {
        .data = db.data,
        .len = db.len,
        .pos = 0,
        .max_chunk = 4096,
        .fail_after_success_reads = 2, /* probe + one payload read, then error */
        .success_reads = 0,
    };
    stream_reader_config_t rcfg = {
        .algo = ALGO_NONE,
        .expected_stream_kind = STREAM_KIND_RDB,
        .raw_frame = 0,
        .allow_passthrough = 0,
        .batch_size = 64 * 1024,
    };
    stream_reader_t *r = stream_reader_create(&rcfg, flakyReaderRead, &fr);
    TEST_ASSERT_MESSAGE("stream_reader_create should succeed", r != NULL);

    uint8_t out[128 * 1024];
    ssize_t n1 = stream_reader_read(r, out, sizeof(out));
    TEST_ASSERT_MESSAGE("first read should return partial output", n1 > 0);
    TEST_ASSERT_MESSAGE("second read should fail immediately",
                        stream_reader_read(r, out, sizeof(out)) == -1);

    stream_reader_destroy(r);

    /* Passthrough mode should also preserve partial bytes when backend read
     * fails after probe/prefix buffering, then latch sticky error state. */
    const uint8_t plain[] = "NOTVKCS-passthrough-regression";
    flaky_reader_t fr_passthrough = {
        .data = plain,
        .len = sizeof(plain) - 1,
        .pos = 0,
        .max_chunk = 0,
        .fail_after_success_reads = 1, /* probe succeeds, next read fails */
        .success_reads = 0,
    };
    stream_reader_config_t pass_cfg = {
        .algo = ALGO_NONE,
        .expected_stream_kind = STREAM_KIND_ANY,
        .raw_frame = 0,
        .allow_passthrough = 1,
        .batch_size = 0,
    };
    stream_reader_t *rp = stream_reader_create(&pass_cfg, flakyReaderRead, &fr_passthrough);
    TEST_ASSERT_MESSAGE("passthrough reader create should succeed", rp != NULL);

    uint8_t pass_out[64];
    ssize_t p1 = stream_reader_read(rp, pass_out, sizeof(pass_out));
    TEST_ASSERT_MESSAGE("passthrough first read should return partial output", p1 > 0);
    TEST_ASSERT_MESSAGE("passthrough partial bytes should match input prefix",
                        memcmp(pass_out, plain, (size_t)p1) == 0);
    TEST_ASSERT_MESSAGE("passthrough second read should fail immediately",
                        stream_reader_read(rp, pass_out, sizeof(pass_out)) == -1);
    stream_reader_destroy(rp);

    dynamicBufFree(&db);
    zfree(payload);
    return 0;
}

/* --- Test: stream_writer_create/destroy (Task 3.1) --- */
int test_streamWriterCreateDestroy(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    TEST_ASSERT_MESSAGE("create should succeed for LZ4", t != NULL);
    TEST_ASSERT_MESSAGE("should not be errored", stream_writer_is_errored(t) == 0);

    stream_writer_destroy(t);
    dynamicBufFree(&db);

    /* NULL config should fail */
    TEST_ASSERT_MESSAGE("NULL config should return NULL",
                        stream_writer_create(NULL, emitToDynamicBuf, &db) == NULL);

    /* NULL emit_cb should fail */
    TEST_ASSERT_MESSAGE("NULL emit_cb should return NULL",
                        stream_writer_create(&cfg, NULL, NULL) == NULL);

    /* ALGO_NONE should fail */
    stream_writer_config_t bad_cfg = {.algo = ALGO_NONE, .level = 0, .stream_kind = STREAM_KIND_RDB};
    TEST_ASSERT_MESSAGE("ALGO_NONE should return NULL",
                        stream_writer_create(&bad_cfg, emitToDynamicBuf, &db) == NULL);

    /* Invalid stream_kind should fail when envelope is enabled. */
    stream_writer_config_t bad_kind_cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = 0x7f, .raw_frame = 0};
    TEST_ASSERT_MESSAGE("invalid stream_kind should fail with envelope",
                        stream_writer_create(&bad_kind_cfg, emitToDynamicBuf, &db) == NULL);

    /* For raw frame mode, stream_kind is ignored. */
    stream_writer_config_t raw_cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = 0x7f, .raw_frame = 1};
    stream_writer_t *raw_t = stream_writer_create(&raw_cfg, emitToDynamicBuf, &db);
    TEST_ASSERT_MESSAGE("raw frame should allow ignored stream_kind", raw_t != NULL);
    stream_writer_destroy(raw_t);

    /* destroy NULL should be safe */
    stream_writer_destroy(NULL);

    return 0;
}

/* --- Test: stream_writer write + finish round-trip (Tasks 3.2, 3.3) --- */
int test_streamWriterRoundTrip(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    TEST_ASSERT(t != NULL);

    /* Write some data */
    const char *test_data = "Hello, compression world! This is a test of the stream writer API.";
    size_t data_len = strlen(test_data);
    TEST_ASSERT(stream_writer_write(t, test_data, data_len) == 0);
    TEST_ASSERT_MESSAGE("should not be errored after write", stream_writer_is_errored(t) == 0);
    TEST_ASSERT_MESSAGE("write should emit envelope", db.len >= VKCS_ENVELOPE_SIZE);

    /* Finalize */
    TEST_ASSERT(stream_writer_finish(t) == 0);
    TEST_ASSERT_MESSAGE("should not be errored after finish", stream_writer_is_errored(t) == 0);

    /* Verify output starts with VKCS envelope */
    TEST_ASSERT_MESSAGE("output should have at least envelope size", db.len >= VKCS_ENVELOPE_SIZE);
    TEST_ASSERT_MESSAGE("magic byte 0", db.data[0] == VKCS_MAGIC_0);
    TEST_ASSERT_MESSAGE("magic byte 1", db.data[1] == VKCS_MAGIC_1);
    TEST_ASSERT_MESSAGE("magic byte 2", db.data[2] == VKCS_MAGIC_2);
    TEST_ASSERT_MESSAGE("magic byte 3", db.data[3] == VKCS_MAGIC_3);
    TEST_ASSERT_MESSAGE("version", db.data[4] == VKCS_VERSION);
    TEST_ASSERT_MESSAGE("algo_id", db.data[5] == ALGO_LZ4);
    TEST_ASSERT_MESSAGE("stream_kind RDB", (db.data[6] & 0x01) == STREAM_KIND_RDB);

    /* Decompress and verify round-trip */
    stream_decompressor_t sd;
    TEST_ASSERT(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[256];
    size_t total_decompressed = 0;
    uint8_t *compressed_data = db.data + VKCS_ENVELOPE_SIZE;
    size_t compressed_len = db.len - VKCS_ENVELOPE_SIZE;
    size_t src_offset = 0;

    while (src_offset < compressed_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            compressed_data + src_offset,
            compressed_len - src_offset, &consumed);
        TEST_ASSERT_MESSAGE("decompression should not fail", produced >= 0);
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    TEST_ASSERT_MESSAGE("decompressed length should match original",
                        total_decompressed == data_len);
    TEST_ASSERT_MESSAGE("decompressed data should match original",
                        memcmp(decompressed, test_data, data_len) == 0);

    streamDecompressorDestroy(&sd);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return 0;
}

/* --- Test: stream_writer_flush semantics (no-op before writes, valid mid-stream) --- */
int test_streamWriterFlushBehavior(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    TEST_ASSERT(t != NULL);

    TEST_ASSERT_MESSAGE("flush before write should be no-op success", stream_writer_flush(t) == 0);
    TEST_ASSERT_MESSAGE("flush before write should not emit bytes", db.len == 0);

    TEST_ASSERT(stream_writer_write(t, "first chunk", 11) == 0);
    TEST_ASSERT(stream_writer_flush(t) == 0);
    TEST_ASSERT(stream_writer_write(t, "second chunk", 12) == 0);
    TEST_ASSERT(stream_writer_finish(t) == 0);

    TEST_ASSERT(db.len > VKCS_ENVELOPE_SIZE);
    stream_decompressor_t sd;
    TEST_ASSERT(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[256];
    size_t total_decompressed = 0;
    uint8_t *comp_data = db.data + VKCS_ENVELOPE_SIZE;
    size_t comp_len = db.len - VKCS_ENVELOPE_SIZE;
    size_t src_offset = 0;

    while (src_offset < comp_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            comp_data + src_offset,
            comp_len - src_offset, &consumed);
        TEST_ASSERT(produced >= 0);
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    TEST_ASSERT(total_decompressed == 23);
    TEST_ASSERT(memcmp(decompressed, "first chunksecond chunk", 23) == 0);

    streamDecompressorDestroy(&sd);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return 0;
}

/* --- Test: raw_frame mode emits only codec frame (no VKCS envelope). --- */
int test_streamWriterRawFrameRoundTrip(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = {
        .algo = ALGO_LZ4,
        .level = 0,
        .stream_kind = STREAM_KIND_RDB,
        .raw_frame = 1,
    };
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    TEST_ASSERT(t != NULL);

    const char *test_data = "raw frame payload";
    size_t data_len = strlen(test_data);
    TEST_ASSERT(stream_writer_write(t, test_data, data_len) == 0);
    TEST_ASSERT(stream_writer_finish(t) == 0);

    TEST_ASSERT_MESSAGE("raw frame output should not start with VKCS magic",
                        !(db.len >= 4 &&
                          db.data[0] == VKCS_MAGIC_0 &&
                          db.data[1] == VKCS_MAGIC_1 &&
                          db.data[2] == VKCS_MAGIC_2 &&
                          db.data[3] == VKCS_MAGIC_3));

    stream_decompressor_t sd;
    TEST_ASSERT(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[128];
    size_t total_decompressed = 0;
    size_t src_offset = 0;
    while (src_offset < db.len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            db.data + src_offset,
            db.len - src_offset, &consumed);
        TEST_ASSERT(produced >= 0);
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    TEST_ASSERT(total_decompressed == data_len);
    TEST_ASSERT(memcmp(decompressed, test_data, data_len) == 0);

    streamDecompressorDestroy(&sd);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return 0;
}

static int lz4FrameIntegrityChecksumFlagFromBlob(const uint8_t *data, size_t len, int *has_checksum) {
    if (!data || !has_checksum) return -1;
    if (len < 7) return -1;

    uint32_t magic = ((uint32_t)data[0]) |
                     ((uint32_t)data[1] << 8) |
                     ((uint32_t)data[2] << 16) |
                     ((uint32_t)data[3] << 24);
    if (magic != 0x184D2204U) return -1;

    uint8_t flg = data[4];
    size_t header_len = 7;                /* magic + FLG + BD + HC */
    if (flg & (1u << 3)) header_len += 8; /* content size */
    if (flg & 1u) header_len += 4;        /* dict ID */
    if (len < header_len) return -1;

    *has_checksum = ((flg & (1u << 4)) != 0) || /* FLG bit 4 */
                    ((flg & (1u << 2)) != 0);   /* FLG bit 2 */
    return 0;
}

/* --- Test: block_checksum config toggles integrity flag in LZ4 frame. --- */
int test_streamWriterContentChecksumToggle(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    const char *payload = "block checksum toggle payload for LZ4 frame";
    size_t payload_len = strlen(payload);

    for (int checksum_on = 0; checksum_on <= 1; checksum_on++) {
        dynamic_buf_t raw_db;
        dynamicBufInit(&raw_db);

        stream_writer_config_t raw_cfg = {
            .algo = ALGO_LZ4,
            .level = 0,
            .stream_kind = STREAM_KIND_RDB,
            .raw_frame = 1, /* make frame start at byte 0 for parser helper */
            .block_checksum = checksum_on,
        };
        stream_writer_t *t = stream_writer_create(&raw_cfg, emitToDynamicBuf, &raw_db);
        TEST_ASSERT(t != NULL);
        TEST_ASSERT(stream_writer_write(t, payload, payload_len) == 0);
        TEST_ASSERT(stream_writer_finish(t) == 0);

        int has_checksum = -1;
        TEST_ASSERT_MESSAGE("frame parser should succeed",
                            lz4FrameIntegrityChecksumFlagFromBlob(raw_db.data, raw_db.len, &has_checksum) == 0);
        TEST_ASSERT_MESSAGE("frame integrity checksum flag should match config",
                            has_checksum == checksum_on);

        stream_writer_destroy(t);
        dynamicBufFree(&raw_db);

        dynamic_buf_t env_db;
        dynamicBufInit(&env_db);
        stream_writer_config_t env_cfg = {
            .algo = ALGO_LZ4,
            .level = 0,
            .stream_kind = STREAM_KIND_RDB,
            .raw_frame = 0,
            .block_checksum = checksum_on,
        };
        stream_writer_t *env_t = stream_writer_create(&env_cfg, emitToDynamicBuf, &env_db);
        TEST_ASSERT(env_t != NULL);
        TEST_ASSERT(stream_writer_write(env_t, payload, payload_len) == 0);
        TEST_ASSERT(stream_writer_finish(env_t) == 0);
        TEST_ASSERT(env_db.len > VKCS_ENVELOPE_SIZE);
        int envelope_checksum = (env_db.data[6] & VKCS_FLAG_CODEC_CHECKSUM) != 0;
        TEST_ASSERT_MESSAGE("VKCS checksum flag should match config",
                            envelope_checksum == checksum_on);
        stream_writer_destroy(env_t);
        dynamicBufFree(&env_db);
    }

    return 0;
}

/* --- Test: compress_rio_t write + finish round-trip (Task 3.4) --- */
int test_compressRioRoundTrip(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Use a buffer rio as the inner rio */
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    compress_rio_t cr;
    TEST_ASSERT(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);
    const char *test_data = "The quick brown fox jumps over the lazy dog. "
                            "Pack my box with five dozen liquor jugs.";
    size_t data_len = strlen(test_data);
    TEST_ASSERT_MESSAGE("rioWrite should succeed",
                        rioWrite((rio *)&cr, test_data, data_len) != 0);

    /* Finalize and destroy */
    compress_rio_finish(&cr);

    /* Get the compressed output from the buffer rio */
    sds compressed = buffer_rio.io.buffer.ptr;
    size_t compressed_len = sdslen(compressed);
    TEST_ASSERT_MESSAGE("compressed output should exist", compressed_len > VKCS_ENVELOPE_SIZE);

    /* Verify VKCS envelope */
    TEST_ASSERT_MESSAGE("magic V", compressed[0] == (char)VKCS_MAGIC_0);
    TEST_ASSERT_MESSAGE("magic K", compressed[1] == (char)VKCS_MAGIC_1);
    TEST_ASSERT_MESSAGE("magic C", compressed[2] == (char)VKCS_MAGIC_2);
    TEST_ASSERT_MESSAGE("magic S", compressed[3] == (char)VKCS_MAGIC_3);

    /* Decompress and verify */
    stream_decompressor_t sd;
    TEST_ASSERT(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[512];
    size_t total_decompressed = 0;
    uint8_t *comp_data = (uint8_t *)compressed + VKCS_ENVELOPE_SIZE;
    size_t comp_len = compressed_len - VKCS_ENVELOPE_SIZE;
    size_t src_offset = 0;

    while (src_offset < comp_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            comp_data + src_offset,
            comp_len - src_offset, &consumed);
        TEST_ASSERT_MESSAGE("decompression should not fail", produced >= 0);
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    TEST_ASSERT_MESSAGE("decompressed length should match",
                        total_decompressed == data_len);
    TEST_ASSERT_MESSAGE("decompressed data should match",
                        memcmp(decompressed, test_data, data_len) == 0);

    streamDecompressorDestroy(&sd);
    compress_rio_destroy(&cr);
    sdsfree(compressed);
    return 0;
}

/* --- Test: decompress_rio_t read round-trip (Task 3.5) --- */
int test_decompressRioRoundTrip(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* First, produce compressed data using stream writer */
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    TEST_ASSERT(t != NULL);

    const char *test_data = "Decompression rio test data. "
                            "This should round-trip through compress then decompress.";
    size_t data_len = strlen(test_data);
    stream_writer_write(t, test_data, data_len);
    stream_writer_finish(t);
    stream_writer_destroy(t);

    /* Skip the VKCS envelope — decompress_rio expects it already consumed */
    TEST_ASSERT(db.len > VKCS_ENVELOPE_SIZE);
    uint8_t *compressed_data = db.data + VKCS_ENVELOPE_SIZE;
    size_t compressed_len = db.len - VKCS_ENVELOPE_SIZE;

    /* Create a buffer rio with the compressed data (no envelope) */
    sds comp_sds = sdsnewlen(compressed_data, compressed_len);
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    /* Create decompress rio */
    decompress_rio_t dr;
    TEST_ASSERT(initRawLz4DecompressRio(&dr, &buffer_rio) == 0);

    /* Read decompressed data */
    char result[256];
    memset(result, 0, sizeof(result));
    TEST_ASSERT_MESSAGE("rioRead should succeed",
                        rioRead((rio *)&dr, result, data_len) != 0);
    TEST_ASSERT_MESSAGE("decompressed data should match original",
                        memcmp(result, test_data, data_len) == 0);

    decompress_rio_destroy(&dr);
    sdsfree(comp_sds);
    dynamicBufFree(&db);
    return 0;
}

/* --- Test: passthrough stream mode (non-VKCS input) --- */
int test_prefixReplayRio(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    const char *payload = "REDIS001remaining data after prefix";
    size_t payload_len = strlen(payload);
    sds buf = sdsnewlen(payload, payload_len);
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_reader_config_t cfg = {
        .algo = ALGO_NONE,
        .expected_stream_kind = STREAM_KIND_ANY,
        .raw_frame = 0,
        .allow_passthrough = 1,
        .batch_size = 0,
    };
    decompress_rio_t dr;
    TEST_ASSERT(decompress_rio_init_with_config(&dr, &buffer_rio, &cfg) == 0);

    stream_reader_info_t info;
    TEST_ASSERT(decompress_rio_get_info(&dr, &info) == 0);
    TEST_ASSERT_MESSAGE("passthrough stream should not be compressed", info.compressed == 0);

    char result[64];
    memset(result, 0, sizeof(result));
    TEST_ASSERT_MESSAGE("rioRead should succeed",
                        rioRead((rio *)&dr, result, payload_len) != 0);
    TEST_ASSERT_MESSAGE("payload should be replayed exactly",
                        memcmp(result, payload, payload_len) == 0);

    decompress_rio_destroy(&dr);
    sdsfree(buf);
    return 0;
}

/* --- Test: compress_rio_finish is idempotent (Task 3.4) --- */
int test_compressRioFinishIdempotent(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    compress_rio_t cr;
    TEST_ASSERT(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);

    rioWrite((rio *)&cr, "test", 4);
    compress_rio_finish(&cr);
    size_t len_after_first = sdslen(buffer_rio.io.buffer.ptr);

    /* Second finish should be a no-op */
    compress_rio_finish(&cr);
    size_t len_after_second = sdslen(buffer_rio.io.buffer.ptr);
    TEST_ASSERT_MESSAGE("second finish should not produce more output",
                        len_after_first == len_after_second);

    compress_rio_destroy(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
    return 0;
}

/* --- Test: compress_rio flush mid-stream does not end frame (Task 3.4) --- */
int test_compressRioFlushMidStream(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    compress_rio_t cr;
    TEST_ASSERT(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);

    /* Write some data */
    TEST_ASSERT(rioWrite((rio *)&cr, "first chunk", 11) != 0);

    /* Flush mid-stream — should NOT end the frame */
    TEST_ASSERT_MESSAGE("flush should succeed", rioFlush((rio *)&cr) != 0);

    /* Write more data — should succeed because frame is still open */
    TEST_ASSERT_MESSAGE("write after flush should succeed",
                        rioWrite((rio *)&cr, "second chunk", 12) != 0);

    /* Now finalize */
    compress_rio_finish(&cr);

    /* Verify the entire stream decompresses correctly */
    sds compressed = buffer_rio.io.buffer.ptr;
    size_t compressed_len = sdslen(compressed);
    TEST_ASSERT(compressed_len > VKCS_ENVELOPE_SIZE);

    stream_decompressor_t sd;
    TEST_ASSERT(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[256];
    size_t total_decompressed = 0;
    uint8_t *comp_data = (uint8_t *)compressed + VKCS_ENVELOPE_SIZE;
    size_t comp_len = compressed_len - VKCS_ENVELOPE_SIZE;
    size_t src_offset = 0;

    while (src_offset < comp_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            comp_data + src_offset,
            comp_len - src_offset, &consumed);
        TEST_ASSERT_MESSAGE("decompression should not fail", produced >= 0);
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    TEST_ASSERT_MESSAGE("total decompressed should be 23 bytes",
                        total_decompressed == 23);
    TEST_ASSERT_MESSAGE("decompressed should match concatenated input",
                        memcmp(decompressed, "first chunksecond chunk", 23) == 0);

    streamDecompressorDestroy(&sd);
    compress_rio_destroy(&cr);
    sdsfree(compressed);
    return 0;
}

/* --- Test: rdbLoadProgressCallback does not cast write-side streaming rios
 * to decompress_rio_t. This protects save/async paths that also use
 * RIO_FLAG_STREAMING_COMPRESSION. --- */
int test_rdbLoadProgressCallbackStreamingGuard(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    sds buf = sdsempty();
    rio inner;
    rioInitWithBuffer(&inner, buf);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    compress_rio_t cr;
    TEST_ASSERT(rioInitWithCompress(&cr, &inner, &cfg) == 0);

    const char sample[] = "progress-guard";
    rdbLoadProgressCallback((rio *)&cr, sample, sizeof(sample) - 1);

    TEST_ASSERT_MESSAGE("write-side streaming rio must not set read error",
                        (cr.base.flags & RIO_FLAG_READ_ERROR) == 0);

    compress_rio_destroy(&cr);
    sdsfree(inner.io.buffer.ptr);
    return 0;
}

/* --- Test: decompress_rio with large payload (>64KB) exercises partial
 * consume in the large-chunk read path. Before the fix, unconsumed
 * compressed bytes were dropped between iterations, causing false EOF
 * or data corruption. (P1 regression test) --- */
int test_decompressRioLargePayload(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Generate a large payload (256KB) with a repeating pattern so
     * it's compressible but large enough to require multiple
     * decompression iterations. */
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i % 251); /* prime modulus for variety */
    }

    /* Compress via stream_writer */
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    TEST_ASSERT(t != NULL);

    stream_writer_write(t, payload, payload_len);
    stream_writer_finish(t);
    stream_writer_destroy(t);

    /* Strip VKCS envelope */
    TEST_ASSERT(db.len > VKCS_ENVELOPE_SIZE);
    uint8_t *comp_data = db.data + VKCS_ENVELOPE_SIZE;
    size_t comp_len = db.len - VKCS_ENVELOPE_SIZE;

    /* Decompress via decompress_rio */
    sds comp_sds = sdsnewlen(comp_data, comp_len);
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    decompress_rio_t dr;
    TEST_ASSERT(initRawLz4DecompressRio(&dr, &buffer_rio) == 0);

    /* Read in small chunks (4KB) to force multiple iterations through
     * the decompression state machine. */
    uint8_t *result = zmalloc(payload_len);
    size_t total_read = 0;
    while (total_read < payload_len) {
        size_t chunk = 4096;
        if (chunk > payload_len - total_read) chunk = payload_len - total_read;
        size_t ret = rioRead((rio *)&dr, result + total_read, chunk);
        TEST_ASSERT_MESSAGE("rioRead should succeed for large payload", ret != 0);
        total_read += chunk;
    }

    TEST_ASSERT_MESSAGE("decompressed data should match original",
                        memcmp(result, payload, payload_len) == 0);

    decompress_rio_destroy(&dr);
    sdsfree(comp_sds);
    zfree(result);
    zfree(payload);
    dynamicBufFree(&db);
    return 0;
}

/* --- Test: decompressRioRead handles a large single rioRead request.
 * Verifies correctness for a single 128KB read through the buffered path. --- */
int test_decompressRioDirectPath(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Generate a large payload (128KB). */
    const size_t payload_len = 128 * 1024;
    uint8_t *payload = zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 7 + 13) % 256);
    }

    /* Compress */
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    TEST_ASSERT(t != NULL);

    stream_writer_write(t, payload, payload_len);
    stream_writer_finish(t);
    stream_writer_destroy(t);

    /* Strip VKCS envelope */
    TEST_ASSERT(db.len > VKCS_ENVELOPE_SIZE);
    uint8_t *comp_data = db.data + VKCS_ENVELOPE_SIZE;
    size_t comp_len = db.len - VKCS_ENVELOPE_SIZE;

    /* Decompress via decompress_rio with a single large read */
    sds comp_sds = sdsnewlen(comp_data, comp_len);
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    decompress_rio_t dr;
    TEST_ASSERT(initRawLz4DecompressRio(&dr, &buffer_rio) == 0);

    uint8_t *result = zmalloc(payload_len);
    size_t ret = rioRead((rio *)&dr, result, payload_len);
    TEST_ASSERT_MESSAGE("single large rioRead should succeed", ret != 0);
    TEST_ASSERT_MESSAGE("decompressed data should match original",
                        memcmp(result, payload, payload_len) == 0);

    decompress_rio_destroy(&dr);
    sdsfree(comp_sds);
    zfree(result);
    zfree(payload);
    dynamicBufFree(&db);
    return 0;
}

/* --- Test: stream_writer_write after finish is rejected.
 * Writes after finish must fail and must not emit bytes. --- */
int test_streamWriterWriteAfterFinish(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    TEST_ASSERT(t != NULL);

    stream_writer_write(t, "hello", 5);
    stream_writer_finish(t);
    size_t len_after_finish = db.len;

    /* Write after finish must fail and emit no output. */
    TEST_ASSERT(stream_writer_write(t, "world", 5) != 0);
    TEST_ASSERT_MESSAGE("write after finish should not produce output",
                        db.len == len_after_finish);

    /* Second finish — should also be a no-op */
    stream_writer_finish(t);
    TEST_ASSERT_MESSAGE("second finish should not produce output",
                        db.len == len_after_finish);

    /* Verify the stream is still valid: one envelope + one frame */
    TEST_ASSERT(db.len > VKCS_ENVELOPE_SIZE);
    stream_decompressor_t sd;
    TEST_ASSERT(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[64];
    size_t total = 0;
    uint8_t *cdata = db.data + VKCS_ENVELOPE_SIZE;
    size_t clen = db.len - VKCS_ENVELOPE_SIZE;
    size_t off = 0;
    while (off < clen) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &sd, decompressed + total, sizeof(decompressed) - total,
            cdata + off, clen - off, &consumed);
        TEST_ASSERT(produced >= 0);
        total += (size_t)produced;
        off += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    TEST_ASSERT_MESSAGE("should decompress to 'hello' only",
                        total == 5 && memcmp(decompressed, "hello", 5) == 0);

    streamDecompressorDestroy(&sd);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return 0;
}

/* Test that two independent compress/decompress streams can coexist
 * without interfering with each other. Verifies no shared mutable state. */
int test_independentStreamsCoexist(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Create two independent compress streams with different data */
    dynamic_buf_t db1, db2;
    dynamicBufInit(&db1);
    dynamicBufInit(&db2);

    stream_writer_config_t cfg = {.algo = ALGO_LZ4, .level = 0, .stream_kind = STREAM_KIND_RDB};
    stream_writer_t *t1 = stream_writer_create(&cfg, emitToDynamicBuf, &db1);
    stream_writer_t *t2 = stream_writer_create(&cfg, emitToDynamicBuf, &db2);
    TEST_ASSERT(t1 != NULL && t2 != NULL);

    const char *data1 = "Stream one data - unique content for first stream AAAA";
    const char *data2 = "Stream two data - different content for second stream BBBB";

    /* Interleave writes to both streams */
    stream_writer_write(t1, data1, strlen(data1));
    stream_writer_write(t2, data2, strlen(data2));
    stream_writer_write(t1, data1, strlen(data1)); /* write again to stream 1 */
    stream_writer_write(t2, data2, strlen(data2)); /* write again to stream 2 */

    stream_writer_finish(t1);
    stream_writer_finish(t2);

    /* Decompress both and verify independently */
    for (int i = 0; i < 2; i++) {
        dynamic_buf_t *db = (i == 0) ? &db1 : &db2;
        const char *expected = (i == 0) ? data1 : data2;
        size_t expected_len = strlen(expected) * 2; /* written twice */

        TEST_ASSERT(db->len > VKCS_ENVELOPE_SIZE);
        sds comp = sdsnewlen(db->data + VKCS_ENVELOPE_SIZE,
                             db->len - VKCS_ENVELOPE_SIZE);
        rio buf_rio;
        rioInitWithBuffer(&buf_rio, comp);

        decompress_rio_t dr;
        TEST_ASSERT(initRawLz4DecompressRio(&dr, &buf_rio) == 0);

        char result[256];
        memset(result, 0, sizeof(result));
        TEST_ASSERT_MESSAGE("rioRead should succeed for coexisting stream",
                            rioRead((rio *)&dr, result, expected_len) != 0);
        TEST_ASSERT_MESSAGE("first half should match",
                            memcmp(result, expected, strlen(expected)) == 0);
        TEST_ASSERT_MESSAGE("second half should match",
                            memcmp(result + strlen(expected), expected, strlen(expected)) == 0);

        decompress_rio_destroy(&dr);
        sdsfree(comp);
    }

    stream_writer_destroy(t1);
    stream_writer_destroy(t2);
    dynamicBufFree(&db1);
    dynamicBufFree(&db2);
    return 0;
}

int test_streamWriterBlockModeLinked(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    /* Verify that block_mode=COMPRESS_BLOCK_LINKED produces a valid
     * LZ4 frame that decompresses correctly. Linked mode should yield
     * better compression ratio for repetitive data. */
    dynamic_buf_t db_ind, db_linked;
    dynamicBufInit(&db_ind);
    dynamicBufInit(&db_linked);

    /* Independent mode (default) */
    stream_writer_config_t cfg_ind = {
        .algo = ALGO_LZ4,
        .level = 0,
        .stream_kind = STREAM_KIND_RDB,
        .block_mode = COMPRESS_BLOCK_INDEPENDENT,
    };
    stream_writer_t *t_ind = stream_writer_create(&cfg_ind, emitToDynamicBuf, &db_ind);
    TEST_ASSERT(t_ind != NULL);

    /* Linked mode */
    stream_writer_config_t cfg_linked = {
        .algo = ALGO_LZ4,
        .level = 0,
        .stream_kind = STREAM_KIND_RDB,
        .block_mode = COMPRESS_BLOCK_LINKED,
    };
    stream_writer_t *t_linked = stream_writer_create(&cfg_linked, emitToDynamicBuf, &db_linked);
    TEST_ASSERT(t_linked != NULL);

    /* Write identical repetitive data to both streams */
    char pattern[4096];
    memset(pattern, 'X', sizeof(pattern));
    for (int i = 0; i < 32; i++) {
        TEST_ASSERT(stream_writer_write(t_ind, pattern, sizeof(pattern)) == 0);
        TEST_ASSERT(stream_writer_write(t_linked, pattern, sizeof(pattern)) == 0);
    }
    TEST_ASSERT(stream_writer_finish(t_ind) == 0);
    TEST_ASSERT(stream_writer_finish(t_linked) == 0);

    /* Both should produce valid output */
    TEST_ASSERT(db_ind.len > VKCS_ENVELOPE_SIZE);
    TEST_ASSERT(db_linked.len > VKCS_ENVELOPE_SIZE);

    /* Decompress linked-mode output and verify round-trip */
    sds comp = sdsnewlen(db_linked.data + VKCS_ENVELOPE_SIZE,
                         db_linked.len - VKCS_ENVELOPE_SIZE);
    rio buf_rio;
    rioInitWithBuffer(&buf_rio, comp);

    decompress_rio_t dr;
    TEST_ASSERT(initRawLz4DecompressRio(&dr, &buf_rio) == 0);

    size_t total_len = sizeof(pattern) * 32;
    char *result = zmalloc(total_len);
    TEST_ASSERT_MESSAGE("linked-mode decompression should succeed",
                        rioRead((rio *)&dr, result, total_len) != 0);

    /* Verify all bytes match */
    for (size_t i = 0; i < total_len; i++) {
        TEST_ASSERT(result[i] == 'X');
    }

    decompress_rio_destroy(&dr);
    sdsfree(comp);
    zfree(result);
    stream_writer_destroy(t_ind);
    stream_writer_destroy(t_linked);
    dynamicBufFree(&db_ind);
    dynamicBufFree(&db_linked);
    return 0;
}
