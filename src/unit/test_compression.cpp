/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Property-based tests for the compression module envelope.
 *
 * **Property: Envelope Format Compliance**
 * **Validates: Requirements 2.2, 2.3, 2.4, 2.15, 2.19** */

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include <random>

extern "C" {
#include "compression.h"
#include "compression_rio.h"
#include "compression_stream.h"
#include "zmalloc.h"
}

/* zmalloc.h defines helper macros (__str/__xstr) that collide with libstdc++ internals.
 * Keep them local to C headers in this C++ translation unit. */
#ifdef __xstr
#undef __xstr
#endif
#ifdef __str
#undef __str
#endif

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

static emit_buf_t makeEmitBuf(size_t pos) {
    emit_buf_t eb = {};
    eb.pos = pos;
    return eb;
}

static mem_reader_t makeMemReader(const uint8_t *data, size_t len, size_t max_chunk) {
    mem_reader_t r = {};
    r.data = data;
    r.len = len;
    r.max_chunk = max_chunk;
    return r;
}

static flaky_reader_t makeFlakyReader(const uint8_t *data,
                                      size_t len,
                                      size_t max_chunk,
                                      int fail_after_success_reads) {
    flaky_reader_t r = {};
    r.data = data;
    r.len = len;
    r.max_chunk = max_chunk;
    r.fail_after_success_reads = fail_after_success_reads;
    return r;
}

static stream_reader_config_t makeReaderConfig(compression_algo_t algo,
                                               uint8_t expected_stream_kind,
                                               bool raw_frame,
                                               bool allow_passthrough,
                                               size_t batch_size) {
    stream_reader_config_t cfg = {};
    cfg.algo = algo;
    cfg.expected_stream_kind = expected_stream_kind;
    cfg.raw_frame = raw_frame;
    cfg.allow_passthrough = allow_passthrough;
    cfg.batch_size = batch_size;
    return cfg;
}

static stream_writer_config_t makeWriterConfig(compression_algo_t algo,
                                               int level,
                                               uint8_t stream_kind,
                                               bool raw_frame = false,
                                               bool block_checksum = false,
                                               bool stable_src = false,
                                               compress_block_mode_t block_mode = COMPRESS_BLOCK_INDEPENDENT) {
    stream_writer_config_t cfg = {};
    cfg.algo = algo;
    cfg.level = level;
    cfg.stream_kind = stream_kind;
    cfg.raw_frame = raw_frame;
    cfg.block_checksum = block_checksum;
    cfg.stable_src = stable_src;
    cfg.block_mode = block_mode;
    return cfg;
}

static int randomInt(int upper_exclusive) {
    static thread_local std::mt19937 rng(123456789u);
    std::uniform_int_distribution<int> dist(0, upper_exclusive - 1);
    return dist(rng);
}

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
TEST(compression, envelopeRoundTrip) {
    compression_algo_t algos[] = {ALGO_LZ4};
    size_t algo_count = sizeof(algos) / sizeof(algos[0]);
    uint8_t kinds[] = {STREAM_KIND_RDB, STREAM_KIND_REPL};

    for (size_t a = 0; a < algo_count; a++) {
        for (int k = 0; k < 2; k++) {
            for (int checksum_enabled = 0; checksum_enabled <= 1; checksum_enabled++) {
                emit_buf_t eb = makeEmitBuf(0);
                int wret = writeVkcsEnvelope(emitToBuf, &eb, algos[a], kinds[k], checksum_enabled);
                ASSERT_TRUE(wret == 0) << "writeVkcsEnvelope should succeed for valid params";
                ASSERT_TRUE(eb.pos == VKCS_ENVELOPE_SIZE) << "envelope should be exactly 8 bytes";

                compression_algo_t got_algo = ALGO_NONE;
                uint8_t got_kind = 0xFF;
                int got_checksum_enabled = -1;
                int rret = readVkcsEnvelope(eb.buf, eb.pos, &got_algo, &got_kind, &got_checksum_enabled);
                ASSERT_TRUE(rret == 0) << "readVkcsEnvelope should succeed";
                ASSERT_TRUE(got_algo == algos[a]) << "round-trip algo must match";
                ASSERT_TRUE(got_kind == kinds[k]) << "round-trip stream_kind must match";
                ASSERT_TRUE(got_checksum_enabled == checksum_enabled) << "round-trip checksum flag must match";
            }
        }
    }
    return;
}

/* --- Property: Envelope magic bytes are "VKCS" (Req 2.3) --- */
TEST(compression, envelopeMagicBytes) {
    emit_buf_t eb = makeEmitBuf(0);
    int ret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    ASSERT_TRUE(ret == 0) << "write must succeed";

    ASSERT_TRUE(eb.buf[0] == 0x56) << "magic[0] == 'V'";
    ASSERT_TRUE(eb.buf[1] == 0x4B) << "magic[1] == 'K'";
    ASSERT_TRUE(eb.buf[2] == 0x43) << "magic[2] == 'C'";
    ASSERT_TRUE(eb.buf[3] == 0x53) << "magic[3] == 'S'";
    ASSERT_TRUE(eb.buf[4] == VKCS_VERSION) << "version == VKCS_VERSION";
    ASSERT_TRUE(eb.buf[7] == 0) << "reserved == 0";
    return;
}

/* --- Property: Envelope flags encode stream_kind in bit 0 (Req 2.4) --- */
TEST(compression, envelopeStreamKindFlag) {
    /* RDB: bit 0 = 0 */
    emit_buf_t eb_rdb = makeEmitBuf(0);
    int ret = writeVkcsEnvelope(emitToBuf, &eb_rdb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    ASSERT_TRUE(ret == 0) << "write RDB must succeed";
    ASSERT_TRUE((eb_rdb.buf[6] & 0x01) == 0) << "RDB stream_kind: flags bit 0 == 0";
    ASSERT_TRUE((eb_rdb.buf[6] & (uint8_t)~VKCS_FLAG_STREAM_KIND) == 0) << "RDB stream_kind: no extra bits set";

    /* REPL: bit 0 = 1 */
    emit_buf_t eb_repl = makeEmitBuf(0);
    ret = writeVkcsEnvelope(emitToBuf, &eb_repl, ALGO_LZ4, STREAM_KIND_REPL, 0);
    ASSERT_TRUE(ret == 0) << "write REPL must succeed";
    ASSERT_TRUE((eb_repl.buf[6] & 0x01) == 1) << "REPL stream_kind: flags bit 0 == 1";
    ASSERT_TRUE((eb_repl.buf[6] & (uint8_t)~VKCS_FLAG_STREAM_KIND) == 0) << "REPL stream_kind: no extra bits set";
    return;
}

/* --- Property: Unrecognized algo_id is rejected (Req 2.15) --- */
TEST(compression, envelopeRejectsUnknownAlgo) {
    /* Build a valid envelope, then corrupt the algo_id byte */
    emit_buf_t eb = makeEmitBuf(0);
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    ASSERT_TRUE(wret == 0) << "write must succeed";

    /* Try every invalid algo_id value 0..255 except ALGO_LZ4 */
    for (int i = 0; i < 256; i++) {
        if (i == ALGO_LZ4) continue;
        eb.buf[5] = (uint8_t)i;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k, NULL);
        ASSERT_TRUE(ret == -1) << "readVkcsEnvelope must reject unknown algo_id";
    }
    return;
}

/* --- Property: write rejects non-streaming algorithms --- */
TEST(compression, envelopeRejectsNonStreamingAlgo) {
    emit_buf_t eb = makeEmitBuf(0);
    ASSERT_TRUE(writeVkcsEnvelope(emitToBuf, &eb, ALGO_NONE, STREAM_KIND_RDB, 0) == -1) << "ALGO_NONE rejected";
    ASSERT_TRUE(writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZF, STREAM_KIND_RDB, 0) == -1) << "ALGO_LZF rejected";
    return;
}

/* --- Property: readVkcsEnvelope rejects truncated input --- */
TEST(compression, envelopeRejectsTruncated) {
    emit_buf_t eb = makeEmitBuf(0);
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    ASSERT_TRUE(wret == 0) << "write must succeed";

    /* Every length < 8 must fail */
    for (size_t l = 0; l < VKCS_ENVELOPE_SIZE; l++) {
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, l, &a, &k, NULL);
        ASSERT_TRUE(ret == -1) << "truncated envelope must be rejected";
    }
    return;
}

/* --- Property: readVkcsEnvelope rejects bad magic --- */
TEST(compression, envelopeRejectsBadMagic) {
    emit_buf_t eb = makeEmitBuf(0);
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    ASSERT_TRUE(wret == 0) << "write must succeed";

    /* Flip each magic byte and verify rejection */
    for (int i = 0; i < 4; i++) {
        uint8_t orig = eb.buf[i];
        eb.buf[i] = ~orig;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k, NULL);
        ASSERT_TRUE(ret == -1) << "bad magic must be rejected";
        eb.buf[i] = orig;
    }
    return;
}

/* --- Property: readVkcsEnvelope rejects reserved bits set (Req 2.19) --- */
TEST(compression, envelopeRejectsReservedBits) {
    emit_buf_t eb = makeEmitBuf(0);
    int wret = writeVkcsEnvelope(emitToBuf, &eb, ALGO_LZ4, STREAM_KIND_RDB, 0);
    ASSERT_TRUE(wret == 0) << "write must succeed";

    /* Setting any reserved flag bit (2-7) must cause rejection */
    for (int bit = 2; bit < 8; bit++) {
        uint8_t orig = eb.buf[6];
        eb.buf[6] = orig | (1 << bit);
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k, NULL);
        ASSERT_TRUE(ret == -1) << "reserved flag bits must be rejected";
        eb.buf[6] = orig;
    }

    /* Non-zero reserved byte [7] must cause rejection */
    for (int val = 1; val < 256; val++) {
        eb.buf[7] = (uint8_t)val;
        compression_algo_t a;
        uint8_t k;
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &a, &k, NULL);
        ASSERT_TRUE(ret == -1) << "non-zero reserved byte must be rejected";
    }
    eb.buf[7] = 0;

    return;
}

/* --- Property: Bit-flip fuzz — write valid envelope, flip random bit,
 * verify readVkcsEnvelope exercises rejection paths.
 * Replaces the previous random round-trip test which only covered 4
 * combinations and added no coverage beyond the exhaustive test. */
TEST(compression, envelopeBitFlipFuzz) {
    const int iterations = 1000;
    compression_algo_t algos[] = {ALGO_LZ4};
    size_t algo_count = sizeof(algos) / sizeof(algos[0]);
    uint8_t kinds[] = {STREAM_KIND_RDB, STREAM_KIND_REPL};

    /* Deterministic RNG for reproducible fuzz coverage. */
    for (int i = 0; i < iterations; i++) {
        compression_algo_t algo = algos[randomInt((int)algo_count)];
        uint8_t kind = kinds[randomInt(2)];

        emit_buf_t eb = makeEmitBuf(0);
        int wret = writeVkcsEnvelope(emitToBuf, &eb, algo, kind, 0);
        ASSERT_TRUE(wret == 0) << "write must succeed";
        ASSERT_TRUE(eb.pos == VKCS_ENVELOPE_SIZE) << "size must be 8";

        /* Flip a random bit in the envelope */
        int bit = randomInt((int)(VKCS_ENVELOPE_SIZE * 8));
        eb.buf[bit / 8] ^= (1 << (bit % 8));

        compression_algo_t got_algo;
        uint8_t got_kind;
        /* Most flips should cause rejection; some may land on don't-care
         * bits and still parse. When parse succeeds, validate the returned
         * values are valid enum members. */
        int ret = readVkcsEnvelope(eb.buf, eb.pos, &got_algo, &got_kind, NULL);
        if (ret == 0) {
            ASSERT_TRUE(got_algo == ALGO_LZ4) << "parsed algo must be LZ4";
            ASSERT_TRUE(got_kind == STREAM_KIND_RDB || got_kind == STREAM_KIND_REPL) << "parsed kind must be RDB or REPL";
        }
    }
    return;
}

/* --- Property: emit_cb failure propagates through writeVkcsEnvelope --- */
static int emitAlwaysFail(void *ctx, const uint8_t *data, size_t len) {
    (void)ctx;
    (void)data;
    (void)len;
    return -1;
}

TEST(compression, envelopeEmitFailure) {
    int ret = writeVkcsEnvelope(emitAlwaysFail, NULL, ALGO_LZ4, STREAM_KIND_RDB, 0);
    ASSERT_TRUE(ret == -1) << "writeVkcsEnvelope must propagate emit_cb failure";
    return;
}

/* ===================================================================
 * Streaming compression/decompression tests
 * =================================================================== */

/* --- Test: LZ4 compressor init/destroy lifecycle --- */
TEST(compression, streamCompressorInitDestroy) {
    stream_compressor_t sc;
    ASSERT_TRUE(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0) << "LZ4 init should succeed";
    ASSERT_TRUE(sc.algo == ALGO_LZ4) << "algo should be LZ4";
    ASSERT_TRUE(sc.frame_started == false) << "frame_started should be false";
    ASSERT_TRUE(sc.ctx.lz4f != NULL) << "ctx should be non-NULL";
    streamCompressorDestroy(&sc);
    ASSERT_TRUE(sc.ctx.lz4f == NULL) << "ctx should be NULL after destroy";
    ASSERT_TRUE(sc.algo == ALGO_NONE) << "algo should be NONE after destroy";

    /* ALGO_NONE should fail */
    stream_compressor_t sc3;
    ASSERT_TRUE(streamCompressorInit(&sc3, ALGO_NONE, 0) == -1) << "NONE init should fail";

    /* NULL should fail */
    ASSERT_TRUE(streamCompressorInit(NULL, ALGO_LZ4, 0) == -1) << "NULL init should fail";

    return;
}

/* --- Test: LZ4 decompressor init/destroy lifecycle --- */
TEST(compression, streamDecompressorInitDestroy) {
    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0) << "LZ4 decomp init should succeed";
    ASSERT_TRUE(sd.algo == ALGO_LZ4) << "algo should be LZ4";
    ASSERT_TRUE(sd.ctx.lz4f != NULL) << "ctx should be non-NULL";
    streamDecompressorDestroy(&sd);
    ASSERT_TRUE(sd.ctx.lz4f == NULL) << "ctx should be NULL after destroy";
    ASSERT_TRUE(sd.algo == ALGO_NONE) << "algo should be NONE after destroy";

    return;
}

/* --- Test: LZ4 compress → decompress round-trip --- */
TEST(compression, streamCompressDecompressRoundTrip) {
    const char *input = "Hello, Valkey compression module! This is a test payload.";
    size_t input_len = strlen(input);

    /* Compress */
    stream_compressor_t sc;
    ASSERT_TRUE(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);

    size_t bound = streamCompressOutputBound(ALGO_LZ4, input_len, 0, FLUSH_END);
    ASSERT_TRUE(bound > 0) << "bound should be > 0";

    uint8_t *compressed = (uint8_t *)zmalloc(bound);
    ASSERT_TRUE(compressed != NULL);
    ssize_t compressed_len = streamCompressFeed(&sc, compressed, bound,
                                                (const uint8_t *)input, input_len,
                                                FLUSH_END);
    ASSERT_TRUE(compressed_len > 0) << "compress should succeed";
    ASSERT_TRUE(sc.frame_started == false) << "frame should be closed after FLUSH_END";
    streamCompressorDestroy(&sc);

    /* Decompress */
    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[256];
    size_t input_consumed = 0;
    ssize_t decompressed_len = streamDecompressFeed(&sd, decompressed, sizeof(decompressed),
                                                    compressed, (size_t)compressed_len,
                                                    &input_consumed);
    ASSERT_TRUE(decompressed_len > 0) << "decompress should succeed";
    ASSERT_TRUE((size_t)decompressed_len == input_len) << "decompressed length should match input";
    ASSERT_TRUE(memcmp(decompressed, input, input_len) == 0) << "decompressed content should match input";
    ASSERT_TRUE(input_consumed == (size_t)compressed_len) << "all compressed input should be consumed";

    streamDecompressorDestroy(&sd);
    zfree(compressed);
    return;
}

/* --- Test: streamCompressOutputBound returns sane values --- */
TEST(compression, streamCompressOutputBound) {
    /* Basic: bound for 1KB input should be > 0 */
    size_t b1 = streamCompressOutputBound(ALGO_LZ4, 1024, 0, FLUSH_CONTINUE);
    ASSERT_TRUE(b1 > 0) << "bound for 1KB continue should be > 0";

    /* Bound with frame header should be larger than without */
    size_t b_no_frame = streamCompressOutputBound(ALGO_LZ4, 1024, 1, FLUSH_CONTINUE);
    size_t b_with_frame = streamCompressOutputBound(ALGO_LZ4, 1024, 0, FLUSH_CONTINUE);
    ASSERT_TRUE(b_with_frame >= b_no_frame) << "bound with frame header should be >= without frame";

    /* Flush bound should be >= continue bound */
    size_t b_flush = streamCompressOutputBound(ALGO_LZ4, 1024, 1, FLUSH_SYNC);
    size_t b_cont = streamCompressOutputBound(ALGO_LZ4, 1024, 1, FLUSH_CONTINUE);
    ASSERT_TRUE(b_flush >= b_cont) << "flush bound should be >= continue bound";

    /* End bound should be >= flush bound */
    size_t b_end = streamCompressOutputBound(ALGO_LZ4, 1024, 1, FLUSH_END);
    ASSERT_TRUE(b_end >= b_flush) << "end bound should be >= flush bound";

    /* Zero input should still return > 0 for flush/end (internal buffering) */
    size_t b_zero_flush = streamCompressOutputBound(ALGO_LZ4, 0, 1, FLUSH_SYNC);
    ASSERT_TRUE(b_zero_flush > 0) << "zero input flush bound should be > 0";

    return;
}

/* --- Test: streamCompressFeed error paths --- */
TEST(compression, streamCompressFeedErrors) {
    uint8_t buf[64];
    /* NULL compressor */
    ASSERT_TRUE(streamCompressFeed(NULL, buf, sizeof(buf),
                                   (const uint8_t *)"x", 1, FLUSH_CONTINUE) == -1)
        << "NULL sc should return -1";

    /* NULL output buffer */
    stream_compressor_t sc;
    ASSERT_TRUE(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);
    ASSERT_TRUE(streamCompressFeed(&sc, NULL, sizeof(buf),
                                   (const uint8_t *)"x", 1, FLUSH_CONTINUE) == -1)
        << "NULL output should return -1";
    streamCompressorDestroy(&sc);

    return;
}

/* --- Test: streamDecompressFeed error paths --- */
TEST(compression, streamDecompressFeedErrors) {
    const char *payload = "decompress sticky error";
    uint8_t buf[64];
    uint8_t out[128];
    size_t consumed = 0;

    /* NULL decompressor */
    ASSERT_TRUE(streamDecompressFeed(NULL, buf, sizeof(buf),
                                     (const uint8_t *)"x", 1, &consumed) == -1)
        << "NULL sd should return -1";

    /* NULL input_consumed */
    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0);
    ASSERT_TRUE(streamDecompressFeed(&sd, buf, sizeof(buf),
                                     (const uint8_t *)"x", 1, NULL) == -1)
        << "NULL input_consumed should return -1";
    ASSERT_TRUE(sd.errored == false) << "decompressor should not be errored by NULL bookkeeping arg";

    /* Zero output capacity should return -1 (no-progress prevention) */
    ASSERT_TRUE(streamDecompressFeed(&sd, buf, 0,
                                     (const uint8_t *)"x", 1, &consumed) == -1)
        << "zero output capacity should return -1";
    ASSERT_TRUE(sd.errored == true) << "decompressor should enter sticky errored state";

    /* Once errored, all subsequent feeds fail immediately. */
    stream_compressor_t sc;
    ASSERT_TRUE(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);
    size_t bound = streamCompressOutputBound(ALGO_LZ4, strlen(payload), 0, FLUSH_END);
    uint8_t *compressed = (uint8_t *)zmalloc(bound);
    ASSERT_TRUE(compressed != NULL);
    ssize_t compressed_len = streamCompressFeed(&sc, compressed, bound,
                                                (const uint8_t *)payload, strlen(payload),
                                                FLUSH_END);
    ASSERT_TRUE(compressed_len > 0);
    streamCompressorDestroy(&sc);

    ASSERT_TRUE(streamDecompressFeed(&sd, out, sizeof(out),
                                     compressed, (size_t)compressed_len,
                                     &consumed) == -1)
        << "errored decompressor should fail even with valid input";
    zfree(compressed);

    streamDecompressorDestroy(&sd);
    return;
}

/* --- Test: pre-frame errors are recoverable, mid-frame errors are permanent --- */
TEST(compression, streamCompressFeedErrorRecovery) {
    stream_compressor_t sc;
    ASSERT_TRUE(streamCompressorInit(&sc, ALGO_LZ4, 0) == 0);

    /* Pre-frame error: compressBegin fails with tiny buffer, but no frame
     * bytes have been emitted yet — this is recoverable. */
    uint8_t tiny[1];
    ssize_t ret = streamCompressFeed(&sc, tiny, 1,
                                     (const uint8_t *)"test data", 9, FLUSH_END);
    ASSERT_TRUE(ret == -1) << "should fail with tiny buffer";
    ASSERT_TRUE(sc.errored == false) << "errored should NOT be set (pre-frame failure)";
    ASSERT_TRUE(sc.frame_started == false) << "frame_started should still be false";

    /* Retry with a proper buffer — should succeed */
    size_t bound = streamCompressOutputBound(ALGO_LZ4, 9, 0, FLUSH_END);
    uint8_t *buf = (uint8_t *)zmalloc(bound);
    ssize_t ret2 = streamCompressFeed(&sc, buf, bound,
                                      (const uint8_t *)"test data", 9, FLUSH_END);
    ASSERT_TRUE(ret2 > 0) << "retry after pre-frame error should succeed";
    zfree(buf);
    streamCompressorDestroy(&sc);

    /* Mid-frame error: start a frame, then force an error — this is permanent. */
    stream_compressor_t sc2;
    ASSERT_TRUE(streamCompressorInit(&sc2, ALGO_LZ4, 0) == 0);

    /* First call with enough space to start the frame */
    size_t bound2 = streamCompressOutputBound(ALGO_LZ4, 5, 0, FLUSH_CONTINUE);
    uint8_t *buf2 = (uint8_t *)zmalloc(bound2);
    ssize_t ret3 = streamCompressFeed(&sc2, buf2, bound2,
                                      (const uint8_t *)"hello", 5, FLUSH_CONTINUE);
    ASSERT_TRUE(ret3 >= 0) << "first write should succeed";
    ASSERT_TRUE(sc2.frame_started == true) << "frame should be started";

    /* Now force a mid-frame error with a tiny buffer */
    uint8_t tiny2[1];
    ssize_t ret4 = streamCompressFeed(&sc2, tiny2, 1,
                                      (const uint8_t *)"more data to compress", 21,
                                      FLUSH_END);
    ASSERT_TRUE(ret4 == -1) << "mid-frame error should fail";
    ASSERT_TRUE(sc2.errored == true) << "errored should be set (mid-frame failure)";

    /* Subsequent calls must fail immediately */
    size_t bound3 = streamCompressOutputBound(ALGO_LZ4, 5, 0, FLUSH_END);
    uint8_t *buf3 = (uint8_t *)zmalloc(bound3);
    ssize_t ret5 = streamCompressFeed(&sc2, buf3, bound3,
                                      (const uint8_t *)"hello", 5, FLUSH_END);
    ASSERT_TRUE(ret5 == -1) << "must fail on errored compressor";
    zfree(buf3);
    zfree(buf2);
    streamCompressorDestroy(&sc2);

    return;
}

/* --- Test: stream_reader passes through truncated non-VKCS input. --- */
TEST(compression, streamReaderTruncatedPassthrough) {
    const uint8_t input[] = {'H', 'E', 'L', 'L', 'O'};
    mem_reader_t mr = makeMemReader(input, sizeof(input), 2);
    stream_reader_config_t cfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_ANY, false, true, 0);

    stream_reader_t *t = stream_reader_create(&cfg, memReaderRead, &mr);
    ASSERT_TRUE(t != NULL) << "stream_reader_create should succeed";

    stream_reader_info_t info;
    ASSERT_TRUE(stream_reader_get_info(t, &info) == 0) << "stream_reader_get_info should succeed";
    ASSERT_TRUE(info.compressed == 0) << "truncated input should be treated as passthrough";

    uint8_t out[16];
    size_t out_len = 0;
    while (out_len < sizeof(input)) {
        ssize_t n = stream_reader_read(t, out + out_len, sizeof(out) - out_len);
        ASSERT_TRUE(n > 0) << "stream_reader_read should produce bytes";
        out_len += (size_t)n;
    }
    ASSERT_TRUE(out_len == sizeof(input) &&
                memcmp(out, input, sizeof(input)) == 0)
        << "passthrough bytes should match";
    ASSERT_TRUE(stream_reader_read(t, out, sizeof(out)) == 0) << "stream_reader_read should return EOF after payload";

    stream_reader_destroy(t);
    return;
}

/* --- Test: stream_reader rejects malformed VKCS envelope. --- */
TEST(compression, streamReaderRejectsInvalidVkcs) {
    /* Valid magic + invalid version (0) */
    const uint8_t input[VKCS_ENVELOPE_SIZE] = {
        VKCS_MAGIC_0, VKCS_MAGIC_1, VKCS_MAGIC_2, VKCS_MAGIC_3,
        0, ALGO_LZ4, STREAM_KIND_RDB, 0};
    mem_reader_t mr = makeMemReader(input, sizeof(input), 0);
    stream_reader_config_t cfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_ANY, false, true, 0);

    stream_reader_t *t = stream_reader_create(&cfg, memReaderRead, &mr);
    ASSERT_TRUE(t != NULL) << "stream_reader_create should succeed";

    ASSERT_TRUE(stream_reader_probe(t) == -1) << "stream_reader_probe should fail on malformed VKCS";
    stream_reader_info_t info;
    ASSERT_TRUE(stream_reader_get_info(t, &info) == -1) << "stream_reader_get_info should fail after malformed VKCS";

    stream_reader_destroy(t);
    return;
}

/* --- Test: stream_reader rejects non-VKCS input when passthrough is disabled. --- */
TEST(compression, streamReaderRejectsNonVkcsWhenPassthroughDisabled) {
    const uint8_t input[VKCS_ENVELOPE_SIZE] = {'R', 'E', 'D', 'I', 'S', '0', '0', '1'};
    mem_reader_t mr = makeMemReader(input, sizeof(input), 0);
    stream_reader_config_t cfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_RDB, false, false, 0);

    stream_reader_t *t = stream_reader_create(&cfg, memReaderRead, &mr);
    ASSERT_TRUE(t != NULL) << "stream_reader_create should succeed";

    ASSERT_TRUE(stream_reader_probe(t) == -1) << "stream_reader_probe should reject non-VKCS when passthrough disabled";
    uint8_t small_out[8] = {0};
    ASSERT_TRUE(stream_reader_read(t, small_out, sizeof(small_out)) == -1) << "stream_reader_read should fail after probe error";

    stream_reader_destroy(t);
    return;
}

/* ===================================================================
 * Tests for stream writer API and rio decorators (Tasks 3.1-3.6)
 * =================================================================== */

extern "C" {
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len);
}

/* --- Emit callback that appends to a dynamically growing buffer --- */
typedef struct {
    uint8_t *data;
    size_t len;
    size_t capacity;
} dynamic_buf_t;

static void dynamicBufInit(dynamic_buf_t *db) {
    db->data = (uint8_t *)zmalloc(4096);
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
        db->data = (uint8_t *)zrealloc(db->data, db->capacity);
    }
    memcpy(db->data + db->len, data, len);
    db->len += len;
    return 0;
}

static int initRawLz4DecompressRio(decompress_rio_t *dr, rio *inner) {
    stream_reader_config_t cfg = makeReaderConfig(ALGO_LZ4, STREAM_KIND_ANY, true, false, 0);
    return decompress_rio_init_with_config(dr, inner, &cfg);
}

/* --- Test: stream_reader rejects VKCS streams with unexpected stream_kind. --- */
TEST(compression, streamReaderRejectsUnexpectedStreamKind) {
    const char *payload = "stream-kind mismatch";
    size_t payload_len = strlen(payload);

    struct {
        uint8_t writer_kind;
        uint8_t expected_kind;
    } cases[] = {
        {STREAM_KIND_REPL, STREAM_KIND_RDB},
        {STREAM_KIND_RDB, STREAM_KIND_REPL},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        dynamic_buf_t db;
        dynamicBufInit(&db);

        stream_writer_config_t wcfg = makeWriterConfig(ALGO_LZ4, 0, cases[i].writer_kind);
        stream_writer_t *w = stream_writer_create(&wcfg, emitToDynamicBuf, &db);
        ASSERT_TRUE(w != NULL) << "stream_writer_create should succeed";
        ASSERT_TRUE(stream_writer_write(w, payload, payload_len) >= 0);
        ASSERT_TRUE(stream_writer_finish(w) == 0);
        stream_writer_destroy(w);

        mem_reader_t mr = makeMemReader(db.data, db.len, 0);
        stream_reader_config_t rcfg = makeReaderConfig(ALGO_NONE,
                                                       cases[i].expected_kind,
                                                       false,
                                                       true,
                                                       0);
        stream_reader_t *r = stream_reader_create(&rcfg, memReaderRead, &mr);
        ASSERT_TRUE(r != NULL) << "stream_reader_create should succeed";
        ASSERT_TRUE(stream_reader_probe(r) == -1) << "unexpected stream_kind must fail probe";

        stream_reader_info_t info;
        ASSERT_TRUE(stream_reader_get_info(r, &info) == -1) << "metadata lookup should fail after stream_kind mismatch";

        uint8_t out[32] = {0};
        ASSERT_TRUE(stream_reader_read(r, out, sizeof(out)) == -1) << "reads should fail after stream_kind mismatch";

        stream_reader_destroy(r);
        dynamicBufFree(&db);
    }
    return;
}

/* --- Test: stream_reader marks errored on partial output + read error.
 * Regression for direct-path error accounting (partial bytes were returned
 * without sticky errored state). */
TEST(compression, streamReaderPartialThenErrorSetsErrored) {
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    uint32_t x = 0x12345678u;
    for (size_t i = 0; i < payload_len; i++) {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        payload[i] = (uint8_t)(x & 0xFF);
    }

    dynamic_buf_t db;
    dynamicBufInit(&db);
    stream_writer_config_t wcfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *w = stream_writer_create(&wcfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(w != NULL) << "stream_writer_create should succeed";
    ASSERT_TRUE(stream_writer_write(w, payload, payload_len) >= 0);
    ASSERT_TRUE(stream_writer_finish(w) == 0);
    stream_writer_destroy(w);

    flaky_reader_t fr = makeFlakyReader(db.data, db.len, 4096, 2); /* probe + one payload read, then error */
    stream_reader_config_t rcfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_RDB, false, false, 64 * 1024);
    stream_reader_t *r = stream_reader_create(&rcfg, flakyReaderRead, &fr);
    ASSERT_TRUE(r != NULL) << "stream_reader_create should succeed";

    const size_t out_len = 128 * 1024;
    uint8_t *out = (uint8_t *)zmalloc(out_len);
    ASSERT_TRUE(out != NULL);
    ssize_t n1 = stream_reader_read(r, out, out_len);
    ASSERT_TRUE(n1 > 0) << "first read should return partial output";
    ASSERT_TRUE(stream_reader_read(r, out, out_len) == -1) << "second read should fail immediately";

    stream_reader_destroy(r);

    /* Passthrough mode should also preserve partial bytes when backend read
     * fails after probe/prefix buffering, then latch sticky error state. */
    const uint8_t plain[] = "NOTVKCS-passthrough-regression";
    flaky_reader_t fr_passthrough = makeFlakyReader(plain, sizeof(plain) - 1, 0, 1); /* probe succeeds, next read fails */
    stream_reader_config_t pass_cfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_ANY, false, true, 0);
    stream_reader_t *rp = stream_reader_create(&pass_cfg, flakyReaderRead, &fr_passthrough);
    ASSERT_TRUE(rp != NULL) << "passthrough reader create should succeed";

    uint8_t pass_out[64];
    ssize_t p1 = stream_reader_read(rp, pass_out, sizeof(pass_out));
    ASSERT_TRUE(p1 > 0) << "passthrough first read should return partial output";
    ASSERT_TRUE(memcmp(pass_out, plain, (size_t)p1) == 0) << "passthrough partial bytes should match input prefix";
    ASSERT_TRUE(stream_reader_read(rp, pass_out, sizeof(pass_out)) == -1) << "passthrough second read should fail immediately";
    stream_reader_destroy(rp);
    zfree(out);

    dynamicBufFree(&db);
    zfree(payload);
    return;
}

/* --- Test: stream_writer_create/destroy (Task 3.1) --- */
TEST(compression, streamWriterCreateDestroy) {
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL) << "create should succeed for LZ4";
    ASSERT_TRUE(stream_writer_is_errored(t) == 0) << "should not be errored";
    ASSERT_TRUE(stream_writer_bytes_emitted(t) == 0) << "new writer should have zero emitted bytes";

    stream_writer_destroy(t);
    dynamicBufFree(&db);

    /* NULL config should fail */
    ASSERT_TRUE(stream_writer_create(NULL, emitToDynamicBuf, &db) == NULL) << "NULL config should return NULL";

    /* NULL emit_cb should fail */
    ASSERT_TRUE(stream_writer_create(&cfg, NULL, NULL) == NULL) << "NULL emit_cb should return NULL";

    /* ALGO_NONE should fail */
    stream_writer_config_t bad_cfg = makeWriterConfig(ALGO_NONE, 0, STREAM_KIND_RDB);
    ASSERT_TRUE(stream_writer_create(&bad_cfg, emitToDynamicBuf, &db) == NULL) << "ALGO_NONE should return NULL";

    /* Invalid stream_kind should fail when envelope is enabled. */
    stream_writer_config_t bad_kind_cfg = makeWriterConfig(ALGO_LZ4, 0, 0x7f, false);
    ASSERT_TRUE(stream_writer_create(&bad_kind_cfg, emitToDynamicBuf, &db) == NULL) << "invalid stream_kind should fail with envelope";

    /* For raw frame mode, stream_kind is ignored. */
    stream_writer_config_t raw_cfg = makeWriterConfig(ALGO_LZ4, 0, 0x7f, true);
    stream_writer_t *raw_t = stream_writer_create(&raw_cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(raw_t != NULL) << "raw frame should allow ignored stream_kind";
    stream_writer_destroy(raw_t);

    /* destroy NULL should be safe */
    stream_writer_destroy(NULL);

    return;
}

/* --- Test: stream_writer write + finish round-trip (Tasks 3.2, 3.3) --- */
TEST(compression, streamWriterRoundTrip) {
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);

    /* Write some data */
    const char *test_data = "Hello, compression world! This is a test of the stream writer API.";
    size_t data_len = strlen(test_data);
    ASSERT_TRUE(stream_writer_write(t, test_data, data_len) >= 0);
    ASSERT_TRUE(stream_writer_is_errored(t) == 0) << "should not be errored after write";
    ASSERT_TRUE(db.len >= VKCS_ENVELOPE_SIZE) << "write should emit envelope";
    ASSERT_TRUE(stream_writer_bytes_emitted(t) == db.len) << "bytes_emitted should track emit callback output";

    /* Finalize */
    ASSERT_TRUE(stream_writer_finish(t) == 0);
    ASSERT_TRUE(stream_writer_is_errored(t) == 0) << "should not be errored after finish";
    ASSERT_TRUE(stream_writer_bytes_emitted(t) == db.len) << "bytes_emitted should include finish output";

    /* Verify output starts with VKCS envelope */
    ASSERT_TRUE(db.len >= VKCS_ENVELOPE_SIZE) << "output should have at least envelope size";
    ASSERT_TRUE(db.data[0] == VKCS_MAGIC_0) << "magic byte 0";
    ASSERT_TRUE(db.data[1] == VKCS_MAGIC_1) << "magic byte 1";
    ASSERT_TRUE(db.data[2] == VKCS_MAGIC_2) << "magic byte 2";
    ASSERT_TRUE(db.data[3] == VKCS_MAGIC_3) << "magic byte 3";
    ASSERT_TRUE(db.data[4] == VKCS_VERSION) << "version";
    ASSERT_TRUE(db.data[5] == ALGO_LZ4) << "algo_id";
    ASSERT_TRUE((db.data[6] & 0x01) == STREAM_KIND_RDB) << "stream_kind RDB";

    /* Decompress and verify round-trip */
    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

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
        ASSERT_TRUE(produced >= 0) << "decompression should not fail";
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_TRUE(total_decompressed == data_len) << "decompressed length should match original";
    ASSERT_TRUE(memcmp(decompressed, test_data, data_len) == 0) << "decompressed data should match original";

    streamDecompressorDestroy(&sd);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return;
}

/* --- Test: stream_writer_flush semantics (no-op before writes, valid mid-stream) --- */
TEST(compression, streamWriterFlushBehavior) {
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);

    ASSERT_TRUE(stream_writer_flush(t) == 0) << "flush before write should be no-op success";
    ASSERT_TRUE(db.len == 0) << "flush before write should not emit bytes";

    ASSERT_TRUE(stream_writer_write(t, "first chunk", 11) >= 0);
    ASSERT_TRUE(stream_writer_flush(t) == 0);
    ASSERT_TRUE(stream_writer_write(t, "second chunk", 12) >= 0);
    ASSERT_TRUE(stream_writer_finish(t) == 0);

    ASSERT_TRUE(db.len > VKCS_ENVELOPE_SIZE);
    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

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
        ASSERT_TRUE(produced >= 0);
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_TRUE(total_decompressed == 23);
    ASSERT_TRUE(memcmp(decompressed, "first chunksecond chunk", 23) == 0);

    streamDecompressorDestroy(&sd);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return;
}

/* --- Test: raw_frame mode emits only codec frame (no VKCS envelope). --- */
TEST(compression, streamWriterRawFrameRoundTrip) {
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB, true);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);

    const char *test_data = "raw frame payload";
    size_t data_len = strlen(test_data);
    ASSERT_TRUE(stream_writer_write(t, test_data, data_len) >= 0);
    ASSERT_TRUE(stream_writer_finish(t) == 0);

    ASSERT_TRUE(!(db.len >= 4 &&
                  db.data[0] == VKCS_MAGIC_0 &&
                  db.data[1] == VKCS_MAGIC_1 &&
                  db.data[2] == VKCS_MAGIC_2 &&
                  db.data[3] == VKCS_MAGIC_3))
        << "raw frame output should not start with VKCS magic";

    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

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
        ASSERT_TRUE(produced >= 0);
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_TRUE(total_decompressed == data_len);
    ASSERT_TRUE(memcmp(decompressed, test_data, data_len) == 0);

    streamDecompressorDestroy(&sd);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return;
}

static int lz4FrameChecksumFlagsFromBlob(const uint8_t *data,
                                         size_t len,
                                         int *has_block_checksum,
                                         int *has_content_checksum) {
    if (!data || !has_block_checksum || !has_content_checksum) return -1;
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

    *has_block_checksum = (flg & (1u << 4)) != 0;   /* FLG bit 4 */
    *has_content_checksum = (flg & (1u << 2)) != 0; /* FLG bit 2 */
    return 0;
}

/* --- Test: block_checksum config toggles integrity flag in LZ4 frame. --- */
TEST(compression, streamWriterBlockChecksumToggle) {
    const char *payload = "block checksum toggle payload for LZ4 frame";
    size_t payload_len = strlen(payload);

    for (int checksum_on = 0; checksum_on <= 1; checksum_on++) {
        dynamic_buf_t raw_db;
        dynamicBufInit(&raw_db);

        stream_writer_config_t raw_cfg = makeWriterConfig(ALGO_LZ4,
                                                          0,
                                                          STREAM_KIND_RDB,
                                                          true, /* make frame start at byte 0 for parser helper */
                                                          (bool)checksum_on);
        stream_writer_t *t = stream_writer_create(&raw_cfg, emitToDynamicBuf, &raw_db);
        ASSERT_TRUE(t != NULL);
        ASSERT_TRUE(stream_writer_write(t, payload, payload_len) >= 0);
        ASSERT_TRUE(stream_writer_finish(t) == 0);

        int has_block_checksum = -1;
        int has_content_checksum = -1;
        ASSERT_TRUE(lz4FrameChecksumFlagsFromBlob(raw_db.data,
                                                  raw_db.len,
                                                  &has_block_checksum,
                                                  &has_content_checksum) == 0)
            << "frame parser should succeed";
        ASSERT_TRUE(has_block_checksum == checksum_on) << "LZ4 block checksum flag should match config";
        ASSERT_TRUE(has_content_checksum == 0) << "content checksum should remain disabled";

        stream_writer_destroy(t);
        dynamicBufFree(&raw_db);

        dynamic_buf_t env_db;
        dynamicBufInit(&env_db);
        stream_writer_config_t env_cfg = makeWriterConfig(ALGO_LZ4,
                                                          0,
                                                          STREAM_KIND_RDB,
                                                          false,
                                                          (bool)checksum_on);
        stream_writer_t *env_t = stream_writer_create(&env_cfg, emitToDynamicBuf, &env_db);
        ASSERT_TRUE(env_t != NULL);
        ASSERT_TRUE(stream_writer_write(env_t, payload, payload_len) >= 0);
        ASSERT_TRUE(stream_writer_finish(env_t) == 0);
        ASSERT_TRUE(env_db.len > VKCS_ENVELOPE_SIZE);
        int envelope_checksum = (env_db.data[6] & VKCS_FLAG_CODEC_CHECKSUM) != 0;
        ASSERT_TRUE(envelope_checksum == checksum_on) << "VKCS checksum flag should match config";
        stream_writer_destroy(env_t);
        dynamicBufFree(&env_db);
    }

    return;
}

/* --- Test: linked block mode and stable_src config round-trip across
 * multiple LZ4 blocks. This keeps the replication-oriented knobs exercised
 * even before the replication path is wired. --- */
TEST(compression, streamWriterLinkedBlocksWithStableSrcRoundTrip) {
    const size_t payload_len = 192 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    ASSERT_TRUE(payload != NULL);

    uint32_t x = 0x9e3779b9u;
    for (size_t i = 0; i < payload_len; i++) {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        payload[i] = (uint8_t)(x & 0xFF);
    }

    dynamic_buf_t db;
    dynamicBufInit(&db);
    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4,
                                                  0,
                                                  STREAM_KIND_RDB,
                                                  true,
                                                  false,
                                                  true,
                                                  COMPRESS_BLOCK_LINKED);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);
    ASSERT_TRUE(stream_writer_write(t, payload, payload_len) >= 0);
    ASSERT_TRUE(stream_writer_finish(t) == 0);

    sds comp = sdsnewlen(db.data, db.len);
    rio buf_rio;
    rioInitWithBuffer(&buf_rio, comp);
    decompress_rio_t dr;
    ASSERT_TRUE(initRawLz4DecompressRio(&dr, &buf_rio) == 0);

    uint8_t *decoded = (uint8_t *)zmalloc(payload_len);
    ASSERT_TRUE(decoded != NULL);
    ASSERT_TRUE(rioRead((rio *)&dr, decoded, payload_len) != 0);
    ASSERT_TRUE(memcmp(decoded, payload, payload_len) == 0);

    zfree(decoded);
    decompress_rio_destroy(&dr);
    sdsfree(comp);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    zfree(payload);
    return;
}

/* --- Test: compress_rio_t write + finish round-trip (Task 3.4) --- */
TEST(compression, compressRioRoundTrip) {
    /* Use a buffer rio as the inner rio */
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    compress_rio_t cr;
    ASSERT_TRUE(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);
    const char *test_data = "The quick brown fox jumps over the lazy dog. "
                            "Pack my box with five dozen liquor jugs.";
    size_t data_len = strlen(test_data);
    ASSERT_TRUE(rioWrite((rio *)&cr, test_data, data_len) != 0) << "rioWrite should succeed";

    /* Finalize and destroy */
    compress_rio_finish(&cr);

    /* Get the compressed output from the buffer rio */
    sds compressed = buffer_rio.io.buffer.ptr;
    size_t compressed_len = sdslen(compressed);
    ASSERT_TRUE(compressed_len > VKCS_ENVELOPE_SIZE) << "compressed output should exist";

    /* Verify VKCS envelope */
    ASSERT_TRUE(compressed[0] == (char)VKCS_MAGIC_0) << "magic V";
    ASSERT_TRUE(compressed[1] == (char)VKCS_MAGIC_1) << "magic K";
    ASSERT_TRUE(compressed[2] == (char)VKCS_MAGIC_2) << "magic C";
    ASSERT_TRUE(compressed[3] == (char)VKCS_MAGIC_3) << "magic S";

    /* Decompress and verify */
    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

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
        ASSERT_TRUE(produced >= 0) << "decompression should not fail";
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_TRUE(total_decompressed == data_len) << "decompressed length should match";
    ASSERT_TRUE(memcmp(decompressed, test_data, data_len) == 0) << "decompressed data should match";

    streamDecompressorDestroy(&sd);
    compress_rio_destroy(&cr);
    sdsfree(compressed);
    return;
}

TEST(compression, compressRioTracksUncompressedChecksum) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB, false, true);
    compress_rio_t cr;
    ASSERT_TRUE(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);

    const char *payload = "checksum-payload-for-compressed-rio";
    size_t payload_len = strlen(payload);
    ASSERT_TRUE(rioWrite((rio *)&cr, payload, payload_len) != 0);

    rio expected = {};
    rioGenericUpdateChecksum(&expected, payload, payload_len);
    ASSERT_TRUE(cr.base.cksum == expected.cksum) << "compress_rio should track the checksum of uncompressed bytes";

    ASSERT_TRUE(compress_rio_finish(&cr) == 0);
    compress_rio_destroy(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
    return;
}

TEST(compression, compressRioPreservesSkipRdbChecksumFlag) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);
    buffer_rio.flags |= RIO_FLAG_SKIP_RDB_CHECKSUM;

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB, false, true);
    compress_rio_t cr;
    ASSERT_TRUE(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);
    ASSERT_TRUE((((rio *)&cr)->flags & RIO_FLAG_SKIP_RDB_CHECKSUM) != 0);

    const char *payload = "skip-checksum-payload";
    ASSERT_TRUE(rioWrite((rio *)&cr, payload, strlen(payload)) != 0);
    ASSERT_TRUE(cr.base.cksum == 0) << "skip-checksum should disable uncompressed CRC64 tracking";

    ASSERT_TRUE(compress_rio_finish(&cr) == 0);
    compress_rio_destroy(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
    return;
}

TEST(compression, compressRioWithoutCodecChecksumDoesNotTrackChecksum) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    compress_rio_t cr;
    ASSERT_TRUE(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);

    const char *payload = "no-codec-checksum";
    ASSERT_TRUE(rioWrite((rio *)&cr, payload, strlen(payload)) != 0);
    ASSERT_TRUE(cr.base.cksum == 0) << "compressed rio should not hash data when checksums are disabled";

    ASSERT_TRUE(compress_rio_finish(&cr) == 0);
    compress_rio_destroy(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
    return;
}

TEST(compression, rioDecoratorsPreserveInnerType) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t wcfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    compress_rio_t cr;
    ASSERT_TRUE(rioInitWithCompress(&cr, &buffer_rio, &wcfg) == 0);
    ASSERT_TRUE(rioCheckType((rio *)&cr) == RIO_TYPE_BUFFER);
    compress_rio_destroy(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);

    sds raw = sdsnew("plain-rdb-prefix");
    rio raw_rio;
    rioInitWithBuffer(&raw_rio, raw);
    stream_reader_config_t rcfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_ANY, false, true, 0);
    decompress_rio_t dr;
    ASSERT_TRUE(decompress_rio_init_with_config(&dr, &raw_rio, &rcfg) == 0);
    ASSERT_TRUE(rioCheckType((rio *)&dr) == RIO_TYPE_BUFFER);
    decompress_rio_destroy(&dr);
    sdsfree(raw_rio.io.buffer.ptr);
}

/* --- Test: decompress_rio_t read round-trip (Task 3.5) --- */
TEST(compression, decompressRioRoundTrip) {
    /* First, produce compressed data using stream writer */
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);

    const char *test_data = "Decompression rio test data. "
                            "This should round-trip through compress then decompress.";
    size_t data_len = strlen(test_data);
    stream_writer_write(t, test_data, data_len);
    stream_writer_finish(t);
    stream_writer_destroy(t);

    /* Skip the VKCS envelope — decompress_rio expects it already consumed */
    ASSERT_TRUE(db.len > VKCS_ENVELOPE_SIZE);
    uint8_t *compressed_data = db.data + VKCS_ENVELOPE_SIZE;
    size_t compressed_len = db.len - VKCS_ENVELOPE_SIZE;

    /* Create a buffer rio with the compressed data (no envelope) */
    sds comp_sds = sdsnewlen(compressed_data, compressed_len);
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    /* Create decompress rio */
    decompress_rio_t dr;
    ASSERT_TRUE(initRawLz4DecompressRio(&dr, &buffer_rio) == 0);

    /* Read decompressed data */
    char result[256];
    memset(result, 0, sizeof(result));
    ASSERT_TRUE(rioRead((rio *)&dr, result, data_len) != 0) << "rioRead should succeed";
    ASSERT_TRUE(memcmp(result, test_data, data_len) == 0) << "decompressed data should match original";

    decompress_rio_destroy(&dr);
    sdsfree(comp_sds);
    dynamicBufFree(&db);
    return;
}

/* --- Test: decompress_rio passthrough mode replays non-VKCS prefix exactly. --- */
TEST(compression, decompressRioPassthroughReplay) {
    const char *payload = "REDIS001remaining data after prefix";
    size_t payload_len = strlen(payload);
    sds buf = sdsnewlen(payload, payload_len);
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_reader_config_t cfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_ANY, false, true, 0);
    decompress_rio_t dr;
    ASSERT_TRUE(decompress_rio_init_with_config(&dr, &buffer_rio, &cfg) == 0);

    stream_reader_info_t info;
    ASSERT_TRUE(decompress_rio_get_info(&dr, &info) == 0);
    ASSERT_TRUE(info.compressed == 0) << "passthrough stream should not be compressed";

    char result[64];
    memset(result, 0, sizeof(result));
    ASSERT_TRUE(rioRead((rio *)&dr, result, payload_len) != 0) << "rioRead should succeed";
    ASSERT_TRUE(memcmp(result, payload, payload_len) == 0) << "payload should be replayed exactly";

    decompress_rio_destroy(&dr);
    sdsfree(buf);
    return;
}

/* --- Test: compress_rio_finish is idempotent (Task 3.4) --- */
TEST(compression, compressRioFinishIdempotent) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    compress_rio_t cr;
    ASSERT_TRUE(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);

    rioWrite((rio *)&cr, "test", 4);
    compress_rio_finish(&cr);
    size_t len_after_first = sdslen(buffer_rio.io.buffer.ptr);

    /* Second finish should be a no-op */
    compress_rio_finish(&cr);
    size_t len_after_second = sdslen(buffer_rio.io.buffer.ptr);
    ASSERT_TRUE(len_after_first == len_after_second) << "second finish should not produce more output";

    compress_rio_destroy(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
    return;
}

/* --- Test: compress_rio flush mid-stream does not end frame (Task 3.4) --- */
TEST(compression, compressRioFlushMidStream) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    compress_rio_t cr;
    ASSERT_TRUE(rioInitWithCompress(&cr, &buffer_rio, &cfg) == 0);

    /* Write some data */
    ASSERT_TRUE(rioWrite((rio *)&cr, "first chunk", 11) != 0);

    /* Flush mid-stream — should NOT end the frame */
    ASSERT_TRUE(rioFlush((rio *)&cr) != 0) << "flush should succeed";

    /* Write more data — should succeed because frame is still open */
    ASSERT_TRUE(rioWrite((rio *)&cr, "second chunk", 12) != 0) << "write after flush should succeed";

    /* Now finalize */
    compress_rio_finish(&cr);

    /* Verify the entire stream decompresses correctly */
    sds compressed = buffer_rio.io.buffer.ptr;
    size_t compressed_len = sdslen(compressed);
    ASSERT_TRUE(compressed_len > VKCS_ENVELOPE_SIZE);

    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

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
        ASSERT_TRUE(produced >= 0) << "decompression should not fail";
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_TRUE(total_decompressed == 23) << "total decompressed should be 23 bytes";
    ASSERT_TRUE(memcmp(decompressed, "first chunksecond chunk", 23) == 0) << "decompressed should match concatenated input";

    streamDecompressorDestroy(&sd);
    compress_rio_destroy(&cr);
    sdsfree(compressed);
    return;
}

/* --- Test: rdbLoadProgressCallback does not cast write-side streaming rios
 * to decompress_rio_t. This protects save/async paths that also use
 * RIO_FLAG_STREAMING_COMPRESSION. --- */
TEST(compression, rdbLoadProgressCallbackStreamingGuard) {
    sds buf = sdsempty();
    rio inner;
    rioInitWithBuffer(&inner, buf);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    compress_rio_t cr;
    ASSERT_TRUE(rioInitWithCompress(&cr, &inner, &cfg) == 0);

    const char sample[] = "progress-guard";
    rdbLoadProgressCallback((rio *)&cr, sample, sizeof(sample) - 1);

    ASSERT_TRUE((cr.base.flags & RIO_FLAG_READ_ERROR) == 0) << "write-side streaming rio must not set read error";

    compress_rio_destroy(&cr);
    sdsfree(inner.io.buffer.ptr);
    return;
}

/* --- Test: decompress_rio with large payload (>64KB) exercises partial
 * consume in the large-chunk read path. Before the fix, unconsumed
 * compressed bytes were dropped between iterations, causing false EOF
 * or data corruption. (P1 regression test) --- */
TEST(compression, decompressRioLargePayload) {
    /* Generate a large payload (256KB) with a repeating pattern so
     * it's compressible but large enough to require multiple
     * decompression iterations. */
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i % 251); /* prime modulus for variety */
    }

    /* Compress via stream_writer */
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);

    stream_writer_write(t, payload, payload_len);
    stream_writer_finish(t);
    stream_writer_destroy(t);

    /* Strip VKCS envelope */
    ASSERT_TRUE(db.len > VKCS_ENVELOPE_SIZE);
    uint8_t *comp_data = db.data + VKCS_ENVELOPE_SIZE;
    size_t comp_len = db.len - VKCS_ENVELOPE_SIZE;

    /* Decompress via decompress_rio */
    sds comp_sds = sdsnewlen(comp_data, comp_len);
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    decompress_rio_t dr;
    ASSERT_TRUE(initRawLz4DecompressRio(&dr, &buffer_rio) == 0);

    /* Read in small chunks (4KB) to force multiple iterations through
     * the decompression state machine. */
    uint8_t *result = (uint8_t *)zmalloc(payload_len);
    size_t total_read = 0;
    while (total_read < payload_len) {
        size_t chunk = 4096;
        if (chunk > payload_len - total_read) chunk = payload_len - total_read;
        size_t ret = rioRead((rio *)&dr, result + total_read, chunk);
        ASSERT_TRUE(ret != 0) << "rioRead should succeed for large payload";
        total_read += chunk;
    }

    ASSERT_TRUE(memcmp(result, payload, payload_len) == 0) << "decompressed data should match original";

    decompress_rio_destroy(&dr);
    sdsfree(comp_sds);
    zfree(result);
    zfree(payload);
    dynamicBufFree(&db);
    return;
}

/* --- Test: decompressRioRead handles a large single rioRead request.
 * Verifies correctness for a single 128KB read through the buffered path. --- */
TEST(compression, decompressRioDirectPath) {
    /* Generate a large payload (128KB). */
    const size_t payload_len = 128 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 7 + 13) % 256);
    }

    /* Compress */
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);

    stream_writer_write(t, payload, payload_len);
    stream_writer_finish(t);
    stream_writer_destroy(t);

    /* Strip VKCS envelope */
    ASSERT_TRUE(db.len > VKCS_ENVELOPE_SIZE);
    uint8_t *comp_data = db.data + VKCS_ENVELOPE_SIZE;
    size_t comp_len = db.len - VKCS_ENVELOPE_SIZE;

    /* Decompress via decompress_rio with a single large read */
    sds comp_sds = sdsnewlen(comp_data, comp_len);
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    decompress_rio_t dr;
    ASSERT_TRUE(initRawLz4DecompressRio(&dr, &buffer_rio) == 0);

    uint8_t *result = (uint8_t *)zmalloc(payload_len);
    size_t ret = rioRead((rio *)&dr, result, payload_len);
    ASSERT_TRUE(ret != 0) << "single large rioRead should succeed";
    ASSERT_TRUE(memcmp(result, payload, payload_len) == 0) << "decompressed data should match original";

    decompress_rio_destroy(&dr);
    sdsfree(comp_sds);
    zfree(result);
    zfree(payload);
    dynamicBufFree(&db);
    return;
}

/* --- Test: stream_reader stops at the end of the compressed frame even if
 * the underlying source still has trailing bytes. This keeps caller-managed
 * framing viable on long-lived streams. --- */
TEST(compression, streamReaderStopsAtFrameEndBeforeTrailingBytes) {
    const char *payload = "stream-reader-frame-end";
    const size_t payload_len = strlen(payload);
    const char *trailer = "TRAILER-BYTES-AFTER-FRAME";
    const size_t trailer_len = strlen(trailer);

    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *w = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(w != NULL);
    ASSERT_TRUE(stream_writer_write(w, payload, payload_len) >= 0);
    ASSERT_TRUE(stream_writer_finish(w) == 0);
    stream_writer_destroy(w);

    sds input = sdsnewlen(db.data, db.len);
    input = sdscatlen(input, trailer, trailer_len);

    mem_reader_t mr = makeMemReader((const uint8_t *)input, sdslen(input), 3);
    stream_reader_config_t rcfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_RDB, false, false, 8);
    stream_reader_t *r = stream_reader_create(&rcfg, memReaderRead, &mr);
    ASSERT_TRUE(r != NULL);

    char out[128];
    memset(out, 0, sizeof(out));
    ASSERT_TRUE(stream_reader_read(r, out, payload_len) == (ssize_t)payload_len);
    ASSERT_TRUE(memcmp(out, payload, payload_len) == 0);

    /* The reader must stop cleanly at frame end instead of trying to decode
     * trailing bytes as part of the same compressed frame. */
    ASSERT_TRUE(stream_reader_read(r, out, sizeof(out)) == 0);

    stream_reader_destroy(r);
    sdsfree(input);
    dynamicBufFree(&db);
    return;
}

TEST(compression, streamReaderRejectsTruncatedFrameTrailer) {
    const size_t payload_len = 256;
    uint8_t payload[payload_len];
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i & 0xFF);
    }

    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB, false, true);
    stream_writer_t *w = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(w != NULL);
    ASSERT_TRUE(stream_writer_write(w, payload, payload_len) >= 0);
    ASSERT_TRUE(stream_writer_finish(w) == 0);
    stream_writer_destroy(w);

    ASSERT_TRUE(db.len > VKCS_ENVELOPE_SIZE + 1);
    mem_reader_t mr = makeMemReader(db.data, db.len - 1, 7);
    stream_reader_config_t rcfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_RDB, false, false, 8);
    stream_reader_t *r = stream_reader_create(&rcfg, memReaderRead, &mr);
    ASSERT_TRUE(r != NULL);

    uint8_t out[payload_len];
    ASSERT_TRUE(stream_reader_read(r, out, payload_len) == (ssize_t)payload_len);
    ASSERT_TRUE(memcmp(out, payload, payload_len) == 0);
    ASSERT_TRUE(stream_reader_read(r, out, 1) < 0) << "EOF before frame end should be treated as corruption";

    stream_reader_destroy(r);
    dynamicBufFree(&db);
    return;
}

TEST(compression, decompressRioDestroyPreservesTrailingBytes) {
    const char *payload = "preserve-trailing-bytes-after-frame";
    const size_t payload_len = strlen(payload);
    const char *trailer = "*1\r\n$4\r\nPING\r\n";
    const size_t trailer_len = strlen(trailer);

    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *w = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(w != NULL);
    ASSERT_TRUE(stream_writer_write(w, payload, payload_len) >= 0);
    ASSERT_TRUE(stream_writer_finish(w) == 0);
    stream_writer_destroy(w);

    sds input = sdsnewlen(db.data, db.len);
    input = sdscatlen(input, trailer, trailer_len);

    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, input);

    stream_reader_config_t rcfg = makeReaderConfig(ALGO_NONE, STREAM_KIND_RDB, false, true, 8);
    decompress_rio_t dr;
    ASSERT_TRUE(decompress_rio_init_with_config(&dr, &buffer_rio, &rcfg) == 0);

    char out[128];
    memset(out, 0, sizeof(out));
    ASSERT_TRUE(rioRead((rio *)&dr, out, payload_len) != 0);
    ASSERT_TRUE(memcmp(out, payload, payload_len) == 0);

    decompress_rio_destroy(&dr);

    char raw_trailer[64];
    memset(raw_trailer, 0, sizeof(raw_trailer));
    ASSERT_TRUE(rioRead(&buffer_rio, raw_trailer, trailer_len) != 0);
    ASSERT_TRUE(memcmp(raw_trailer, trailer, trailer_len) == 0);

    sdsfree(buffer_rio.io.buffer.ptr);
    dynamicBufFree(&db);
    return;
}

/* --- Test: stream_writer_write after finish is rejected.
 * Writes after finish must fail and must not emit bytes. --- */
TEST(compression, streamWriterWriteAfterFinish) {
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);

    stream_writer_write(t, "hello", 5);
    stream_writer_finish(t);
    size_t len_after_finish = db.len;

    /* Write after finish must fail and emit no output. */
    ASSERT_TRUE(stream_writer_write(t, "world", 5) < 0);
    ASSERT_TRUE(db.len == len_after_finish) << "write after finish should not produce output";

    /* Second finish — should also be a no-op */
    stream_writer_finish(t);
    ASSERT_TRUE(db.len == len_after_finish) << "second finish should not produce output";

    /* Verify the stream is still valid: one envelope + one frame */
    ASSERT_TRUE(db.len > VKCS_ENVELOPE_SIZE);
    stream_decompressor_t sd;
    ASSERT_TRUE(streamDecompressorInit(&sd, ALGO_LZ4) == 0);

    uint8_t decompressed[64];
    size_t total = 0;
    uint8_t *cdata = db.data + VKCS_ENVELOPE_SIZE;
    size_t comp_len = db.len - VKCS_ENVELOPE_SIZE;
    size_t off = 0;
    while (off < comp_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &sd, decompressed + total, sizeof(decompressed) - total,
            cdata + off, comp_len - off, &consumed);
        ASSERT_TRUE(produced >= 0);
        total += (size_t)produced;
        off += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_TRUE(total == 5 && memcmp(decompressed, "hello", 5) == 0) << "should decompress to 'hello' only";

    streamDecompressorDestroy(&sd);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return;
}

/* Test that two independent compress/decompress streams can coexist
 * without interfering with each other. Verifies no shared mutable state. */
TEST(compression, independentStreamsCoexist) {
    /* Create two independent compress streams with different data */
    dynamic_buf_t db1, db2;
    dynamicBufInit(&db1);
    dynamicBufInit(&db2);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t1 = stream_writer_create(&cfg, emitToDynamicBuf, &db1);
    stream_writer_t *t2 = stream_writer_create(&cfg, emitToDynamicBuf, &db2);
    ASSERT_TRUE(t1 != NULL && t2 != NULL);

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

        ASSERT_TRUE(db->len > VKCS_ENVELOPE_SIZE);
        sds comp = sdsnewlen(db->data + VKCS_ENVELOPE_SIZE,
                             db->len - VKCS_ENVELOPE_SIZE);
        rio buf_rio;
        rioInitWithBuffer(&buf_rio, comp);

        decompress_rio_t dr;
        ASSERT_TRUE(initRawLz4DecompressRio(&dr, &buf_rio) == 0);

        char result[256];
        memset(result, 0, sizeof(result));
        ASSERT_TRUE(rioRead((rio *)&dr, result, expected_len) != 0) << "rioRead should succeed for coexisting stream";
        ASSERT_TRUE(memcmp(result, expected, strlen(expected)) == 0) << "first half should match";
        ASSERT_TRUE(memcmp(result + strlen(expected), expected, strlen(expected)) == 0) << "second half should match";

        decompress_rio_destroy(&dr);
        sdsfree(comp);
    }

    stream_writer_destroy(t1);
    stream_writer_destroy(t2);
    dynamicBufFree(&db1);
    dynamicBufFree(&db2);
    return;
}

TEST(compression, streamWriterRepetitivePayloadRoundTrip) {
    /* Default block mode is independent.
     * Verify repetitive data still round-trips correctly. */
    dynamic_buf_t db;
    dynamicBufInit(&db);

    stream_writer_config_t cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    stream_writer_t *t = stream_writer_create(&cfg, emitToDynamicBuf, &db);
    ASSERT_TRUE(t != NULL);

    /* Write repetitive data to a single stream */
    char pattern[4096];
    memset(pattern, 'X', sizeof(pattern));
    for (int i = 0; i < 32; i++) {
        ASSERT_TRUE(stream_writer_write(t, pattern, sizeof(pattern)) >= 0);
    }
    ASSERT_TRUE(stream_writer_finish(t) == 0);

    ASSERT_TRUE(db.len > VKCS_ENVELOPE_SIZE);
    sds comp = sdsnewlen(db.data + VKCS_ENVELOPE_SIZE,
                         db.len - VKCS_ENVELOPE_SIZE);
    rio buf_rio;
    rioInitWithBuffer(&buf_rio, comp);

    decompress_rio_t dr;
    ASSERT_TRUE(initRawLz4DecompressRio(&dr, &buf_rio) == 0);

    size_t total_len = sizeof(pattern) * 32;
    char *result = (char *)zmalloc(total_len);
    ASSERT_TRUE(rioRead((rio *)&dr, result, total_len) != 0) << "repetitive payload decompression should succeed";

    /* Verify all bytes match */
    for (size_t i = 0; i < total_len; i++) {
        ASSERT_TRUE(result[i] == 'X');
    }

    decompress_rio_destroy(&dr);
    sdsfree(comp);
    zfree(result);
    stream_writer_destroy(t);
    dynamicBufFree(&db);
    return;
}
