/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Tests for the compression streaming and rio decorator layers. */

#include "generated_wrappers.hpp"

#include <cstring>
#include <limits>
#include <string>

extern "C" {
#include "../../deps/lz4/lz4frame.h"
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

class CompressionTest : public ::testing::Test {};

typedef struct {
    const uint8_t *data;
    size_t len;
    size_t pos;
    size_t max_chunk; /* 0 => unbounded */
} MemReader;

typedef struct {
    const uint8_t *data;
    size_t len;
    size_t pos;
    size_t max_chunk;
    size_t fail_after_pos;
    int fail_after_success_reads;
    int success_reads;
} FlakyReader;

static streamReaderConfig makeReaderConfig(uint8_t expected_stream_kind,
                                           bool allow_passthrough,
                                           size_t buffer_size = STREAM_READER_BUFFER_SIZE_DEFAULT) {
    streamReaderConfig cfg = {};
    cfg.expected_stream_kind = expected_stream_kind;
    cfg.allow_passthrough = allow_passthrough;
    cfg.buffer_size = buffer_size;
    return cfg;
}

static streamWriterConfig makeWriterConfig(compressionAlgo algo,
                                           int level,
                                           uint8_t stream_kind,
                                           bool codec_checksum_enabled = false) {
    streamWriterConfig cfg = {};
    cfg.algo = algo;
    cfg.level = level;
    cfg.stream_kind = stream_kind;
    cfg.codec_checksum_enabled = codec_checksum_enabled;
    return cfg;
}

static bool lz4FrameHasBlockChecksum(const uint8_t *data, size_t len) {
    LZ4F_dctx *dctx = nullptr;
    EXPECT_FALSE(LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION)));
    if (dctx == nullptr) return false;

    LZ4F_frameInfo_t frame_info = {};
    size_t src_size = len;
    size_t ret = LZ4F_getFrameInfo(dctx, &frame_info, data, &src_size);
    bool has_block_checksum = !LZ4F_isError(ret) &&
                              frame_info.blockChecksumFlag == LZ4F_blockChecksumEnabled;
    LZ4F_freeDecompressionContext(dctx);
    return has_block_checksum;
}

static bool lz4FrameHasContentChecksum(const uint8_t *data, size_t len) {
    LZ4F_dctx *dctx = nullptr;
    EXPECT_FALSE(LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION)));
    if (dctx == nullptr) return false;

    LZ4F_frameInfo_t frame_info = {};
    size_t src_size = len;
    size_t ret = LZ4F_getFrameInfo(dctx, &frame_info, data, &src_size);
    bool has_content_checksum = !LZ4F_isError(ret) &&
                                frame_info.contentChecksumFlag == LZ4F_contentChecksumEnabled;
    LZ4F_freeDecompressionContext(dctx);
    return has_content_checksum;
}

static bool lz4FrameUsesLinkedBlocks(const uint8_t *data, size_t len) {
    LZ4F_dctx *dctx = nullptr;
    EXPECT_FALSE(LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION)));
    if (dctx == nullptr) return false;

    LZ4F_frameInfo_t frame_info = {};
    size_t src_size = len;
    size_t ret = LZ4F_getFrameInfo(dctx, &frame_info, data, &src_size);
    bool has_linked_blocks = !LZ4F_isError(ret) &&
                             frame_info.blockMode == LZ4F_blockLinked;
    LZ4F_freeDecompressionContext(dctx);
    return has_linked_blocks;
}

static ssize_t memReaderRead(void *ctx, void *buf, size_t len) {
    MemReader *r = (MemReader *)ctx;
    if (r->pos >= r->len) return 0;

    size_t avail = r->len - r->pos;
    size_t n = len < avail ? len : avail;
    if (r->max_chunk && n > r->max_chunk) n = r->max_chunk;

    memcpy(buf, r->data + r->pos, n);
    r->pos += n;
    return (ssize_t)n;
}

static ssize_t flakyReaderRead(void *ctx, void *buf, size_t len) {
    FlakyReader *r = (FlakyReader *)ctx;
    if (r->success_reads >= r->fail_after_success_reads) return -1;
    if (r->fail_after_pos > 0 && r->pos >= r->fail_after_pos) return -1;
    if (r->pos >= r->len) return 0;

    size_t avail = r->len - r->pos;
    size_t n = len < avail ? len : avail;
    if (r->max_chunk && n > r->max_chunk) n = r->max_chunk;
    if (r->fail_after_pos > 0) {
        size_t until_failure = r->fail_after_pos - r->pos;
        if (n > until_failure) n = until_failure;
    }

    memcpy(buf, r->data + r->pos, n);
    r->pos += n;
    r->success_reads++;
    return (ssize_t)n;
}

static size_t discardRioWrite(rio *r, const void *buf, size_t len) {
    (void)r;
    (void)buf;
    (void)len;
    return 1;
}

static off_t discardRioTell(rio *r) {
    return (off_t)r->processed_bytes;
}

static int failRioFlush(rio *r) {
    (void)r;
    return 0;
}

static void initFailingFlushRio(rio *r) {
    memset(r, 0, sizeof(*r));
    r->write = discardRioWrite;
    r->tell = discardRioTell;
    r->flush = failRioFlush;
}

/* ===================================================================
 * Streaming compression/decompression tests
 * =================================================================== */

/* --- Test: LZ4 compressor init/free lifecycle --- */
TEST_F(CompressionTest, streamCompressorInitFree) {
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0), 0) << "LZ4 init should succeed";
    ASSERT_EQ(sc.algo, ALGO_LZ4) << "algo should be LZ4";
    ASSERT_EQ(sc.stream_started, false) << "stream_started should be false";
    ASSERT_NE(sc.ctx, nullptr) << "ctx should be non-nullptr";
    streamCompressorFree(&sc);
    ASSERT_EQ(sc.ctx, nullptr) << "ctx should be nullptr after free";

    /* ALGO_NONE should fail */
    streamCompressor sc3;
    ASSERT_EQ(streamCompressorInit(&sc3, ALGO_NONE, 0), -1) << "NONE init should fail";
}

/* --- Test: LZ4 decompressor init/free lifecycle --- */
TEST_F(CompressionTest, streamDecompressorInitFree) {
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0) << "LZ4 decomp init should succeed";
    ASSERT_EQ(sd.algo, ALGO_LZ4) << "algo should be LZ4";
    ASSERT_NE(sd.ctx, nullptr) << "ctx should be non-nullptr";
    streamDecompressorFree(&sd);
    ASSERT_EQ(sd.ctx, nullptr) << "ctx should be nullptr after free";
}

/* --- Test: LZ4 compress → decompress round-trip --- */
TEST_F(CompressionTest, streamCompressorDecompressorRoundTrip) {
    const char *input = "Hello, Valkey compression module! This is a test payload.";
    size_t input_len = strlen(input);

    /* Compress */
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0), 0);

    size_t bound = streamCompressorOutputBound(&sc, input_len);
    ASSERT_GT(bound, 0u) << "bound should be > 0";

    uint8_t *compressed = (uint8_t *)zmalloc(bound);
    ASSERT_NE(compressed, nullptr);
    ssize_t compressed_len = streamCompressorFeed(&sc, compressed, bound,
                                                  (const uint8_t *)input, input_len,
                                                  FLUSH_END);
    ASSERT_GT(compressed_len, 0) << "compress should succeed";
    ASSERT_EQ(sc.stream_started, false) << "frame should be closed after FLUSH_END";
    streamCompressorFree(&sc);

    /* Decompress */
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0);

    uint8_t decompressed[256];
    size_t input_consumed = 0;
    ssize_t decompressed_len = streamDecompressorFeed(&sd, decompressed, sizeof(decompressed),
                                                      compressed, (size_t)compressed_len,
                                                      &input_consumed);
    ASSERT_GT(decompressed_len, 0) << "decompress should succeed";
    ASSERT_EQ((size_t)decompressed_len, input_len) << "decompressed length should match input";
    ASSERT_EQ(memcmp(decompressed, input, input_len), 0) << "decompressed content should match input";
    ASSERT_EQ(input_consumed, (size_t)compressed_len) << "all compressed input should be consumed";

    streamDecompressorFree(&sd);
    zfree(compressed);
}

/* --- Test: streamCompressorOutputBound returns sane values --- */
TEST_F(CompressionTest, streamCompressorOutputBound) {
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0), 0);

    /* Basic: bound for 1KB input should be > 0 */
    size_t b1 = streamCompressorOutputBound(&sc, 1024);
    ASSERT_GT(b1, 0u) << "bound for 1KB should be > 0";

    /* Bound is always conservative (includes frame header + flush overhead),
     * so it should be stable regardless of frame state. */
    size_t b_before = streamCompressorOutputBound(&sc, 1024);
    uint8_t *seed_buf = (uint8_t *)zmalloc(b_before);
    ASSERT_NE(seed_buf, nullptr);
    ASSERT_GE(streamCompressorFeed(&sc, seed_buf, b_before,
                                   (const uint8_t *)"x", 1, FLUSH_CONTINUE),
              0)
        << "seed write should start the frame";
    size_t b_after = streamCompressorOutputBound(&sc, 1024);
    ASSERT_EQ(b_before, b_after) << "bound should be the same before and after frame start";

    /* Zero input should still return > 0 (frame header + flush overhead) */
    size_t b_zero = streamCompressorOutputBound(&sc, 0);
    ASSERT_GT(b_zero, 0u) << "zero input bound should be > 0";

    zfree(seed_buf);
    streamCompressorFree(&sc);
}

/* --- Test: corrupt compressed input is a sticky decompressor error. --- */
TEST_F(CompressionTest, streamDecompressorFeedCorruptInputSetsStickyError) {
    const char *payload = "decompress sticky error";
    uint8_t buf[64];
    uint8_t out[128];
    size_t consumed = 0;

    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0);
    ASSERT_EQ(streamDecompressorFeed(&sd, buf, sizeof(buf),
                                     (const uint8_t *)"not an lz4 frame", 16, &consumed),
              -1);
    ASSERT_EQ(sd.errored, true) << "corrupt input should enter sticky errored state";

    /* Once errored, all subsequent feeds fail immediately. */
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0), 0);
    size_t bound = streamCompressorOutputBound(&sc, strlen(payload));
    uint8_t *compressed = (uint8_t *)zmalloc(bound);
    ASSERT_NE(compressed, nullptr);
    ssize_t compressed_len = streamCompressorFeed(&sc, compressed, bound,
                                                  (const uint8_t *)payload, strlen(payload),
                                                  FLUSH_END);
    ASSERT_GT(compressed_len, 0);
    streamCompressorFree(&sc);

    ASSERT_EQ(streamDecompressorFeed(&sd, out, sizeof(out),
                                     compressed, (size_t)compressed_len,
                                     &consumed),
              -1)
        << "errored decompressor should fail even with valid input";
    zfree(compressed);

    streamDecompressorFree(&sd);
}

/* --- Test: pre-frame errors are recoverable, mid-frame errors are permanent --- */
TEST_F(CompressionTest, streamCompressorFeedErrorRecovery) {
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0), 0);

    /* Pre-frame error: compressBegin fails with tiny buffer, but no frame
     * bytes have been emitted yet — this is recoverable. */
    uint8_t tiny[1];
    ssize_t ret = streamCompressorFeed(&sc, tiny, 1,
                                       (const uint8_t *)"test data", 9, FLUSH_END);
    ASSERT_EQ(ret, -1) << "should fail with tiny buffer";
    ASSERT_EQ(sc.stream_started, false) << "stream_started should still be false";

    /* Retry with a proper buffer — should succeed */
    size_t bound = streamCompressorOutputBound(&sc, 9);
    uint8_t *buf = (uint8_t *)zmalloc(bound);
    ssize_t ret2 = streamCompressorFeed(&sc, buf, bound,
                                        (const uint8_t *)"test data", 9, FLUSH_END);
    ASSERT_GT(ret2, 0) << "retry after pre-frame error should succeed";
    zfree(buf);
    streamCompressorFree(&sc);

    /* Mid-frame error: start a frame, then force an error with a tiny buffer. */
    streamCompressor sc2;
    ASSERT_EQ(streamCompressorInit(&sc2, ALGO_LZ4, 0), 0);

    size_t bound2 = streamCompressorOutputBound(&sc2, 5);
    uint8_t *buf2 = (uint8_t *)zmalloc(bound2);
    ssize_t ret3 = streamCompressorFeed(&sc2, buf2, bound2,
                                        (const uint8_t *)"hello", 5, FLUSH_CONTINUE);
    ASSERT_GE(ret3, 0) << "first write should succeed";
    ASSERT_EQ(sc2.stream_started, true) << "stream should be started";

    uint8_t tiny2[1];
    ssize_t ret4 = streamCompressorFeed(&sc2, tiny2, 1,
                                        (const uint8_t *)"more data to compress", 21,
                                        FLUSH_END);
    ASSERT_EQ(ret4, -1) << "mid-frame error should fail";

    zfree(buf2);
    streamCompressorFree(&sc2);
}

TEST_F(CompressionTest, streamReaderClassifiesProbeInputs) {
    static const uint8_t plain_input[] = {'H', 'E', 'L', 'L', 'O'};
    static const uint8_t truncated_vcs[] = {'V', 'C'};
    static const uint8_t invalid_vcs[VCS_ENVELOPE_SIZE] = {
        VCS_MAGIC_0, VCS_MAGIC_1, VCS_MAGIC_2, 0, ALGO_LZ4, 0, STREAM_KIND_RDB};
    static const uint8_t strict_non_vcs[VCS_ENVELOPE_SIZE] = {'R', 'E', 'D', 'I', 'S', '0', '0'};

    struct {
        const char *name;
        const uint8_t *input;
        size_t input_len;
        size_t max_chunk;
        bool allow_passthrough;
        bool expect_probe_ok;
        streamReaderError expected_error;
        size_t expected_read_len;
    } cases[] = {
        {"plain passthrough",
         plain_input,
         sizeof(plain_input),
         2,
         true,
         true,
         STREAM_READER_ERROR_NONE,
         sizeof(plain_input)},
        {"truncated VCS prefix",
         truncated_vcs,
         sizeof(truncated_vcs),
         2,
         true,
         false,
         STREAM_READER_ERROR_INCOMPATIBLE,
         0},
        {"invalid VCS envelope",
         invalid_vcs,
         sizeof(invalid_vcs),
         0,
         true,
         false,
         STREAM_READER_ERROR_INCOMPATIBLE,
         0},
        {"non-VCS with passthrough disabled",
         strict_non_vcs,
         sizeof(strict_non_vcs),
         0,
         false,
         false,
         STREAM_READER_ERROR_INCOMPATIBLE,
         0},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        MemReader mr = {};
        mr.data = cases[i].input;
        mr.len = cases[i].input_len;
        mr.max_chunk = cases[i].max_chunk;
        streamReaderConfig cfg = makeReaderConfig(STREAM_KIND_RDB, cases[i].allow_passthrough);
        streamReader t;
        ASSERT_EQ(streamReaderInit(&t, &cfg, memReaderRead, &mr), 0);

        streamReaderInfo info;
        if (cases[i].expect_probe_ok) {
            ASSERT_EQ(streamReaderGetInfo(&t, &info), 0) << cases[i].name;
            ASSERT_FALSE(info.compressed) << cases[i].name;

            uint8_t out[16] = {0};
            ASSERT_EQ(streamReaderRead(&t, out, cases[i].expected_read_len), (ssize_t)cases[i].expected_read_len)
                << cases[i].name;
            ASSERT_EQ(memcmp(out, cases[i].input, cases[i].expected_read_len), 0) << cases[i].name;
            ASSERT_EQ(streamReaderRead(&t, out, sizeof(out)), 0) << cases[i].name;
        } else {
            ASSERT_EQ(streamReaderGetInfo(&t, &info), -1) << cases[i].name;
            ASSERT_EQ(t.error_kind, cases[i].expected_error) << cases[i].name;

            uint8_t out[8] = {0};
            ASSERT_EQ(streamReaderRead(&t, out, sizeof(out)), -1) << cases[i].name;
        }

        streamReaderFree(&t);
    }
}

TEST_F(CompressionTest, streamReaderRejectsOversizedReadRequest) {
    const uint8_t input[] = {'H', 'E', 'L', 'L', 'O'};
    MemReader mr = {};
    mr.data = input;
    mr.len = sizeof(input);
    mr.max_chunk = 2;
    streamReaderConfig cfg = makeReaderConfig(STREAM_KIND_RDB, true);

    streamReader t;

    ASSERT_EQ(streamReaderInit(&t, &cfg, memReaderRead, &mr), 0);

    uint8_t out[8] = {0};
    size_t oversized = (size_t)(std::numeric_limits<ssize_t>::max)() + 1;
    ASSERT_EQ(streamReaderRead(&t, out, oversized), -1)
        << "oversized reads should fail before touching stream state";

    streamReaderInfo info;
    ASSERT_EQ(streamReaderGetInfo(&t, &info), 0)
        << "oversized read failure should not poison the reader";
    ASSERT_FALSE(info.compressed) << "plain input should still probe as passthrough";

    ASSERT_EQ(streamReaderRead(&t, out, sizeof(input)), (ssize_t)sizeof(input));
    ASSERT_EQ(memcmp(out, input, sizeof(input)), 0) << "subsequent valid read should still succeed";

    streamReaderFree(&t);
}

/* ===================================================================
 * Tests for stream writer API and rio decorators
 * =================================================================== */

extern "C" {
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len);
}

/* --- Emit callback that appends to a dynamically growing buffer --- */
typedef struct {
    uint8_t *data;
} DynamicBuf;

static void dynamicBufInit(DynamicBuf *db) {
    db->data = (uint8_t *)sdsempty();
}

static void dynamicBufFree(DynamicBuf *db) {
    if (db->data) sdsfree((sds)db->data);
    db->data = nullptr;
}

static int emitToDynamicBuf(void *ctx, const uint8_t *data, size_t len) {
    DynamicBuf *db = (DynamicBuf *)ctx;
    db->data = (uint8_t *)sdscatlen((sds)db->data, data, len);
    return db->data != nullptr ? 0 : -1;
}

static int initVcsRdbDecompressRio(decompressRio *dr, rio *inner) {
    return rioInitWithRdbDecompression(dr, inner, nullptr) == DECOMPRESS_RIO_INIT_OK ? 0 : -1;
}

TEST_F(CompressionTest, streamReaderValidatesCompressedStreamKinds) {
    struct {
        const char *name;
        const char *payload;
        uint8_t writer_kind;
        uint8_t expected_kind;
        size_t max_chunk;
        size_t buffer_size;
        bool expect_ok;
    } cases[] = {
        {"incremental RDB stream",
         "incremental probe payload",
         STREAM_KIND_RDB,
         STREAM_KIND_RDB,
         3,
         8,
         true},
        {"custom stream kind",
         "custom stream kind",
         0x7f,
         0x7f,
         0,
         STREAM_READER_BUFFER_SIZE_DEFAULT,
         true},
        {"custom stream when RDB expected",
         "stream-kind mismatch",
         0x7f,
         STREAM_KIND_RDB,
         0,
         STREAM_READER_BUFFER_SIZE_DEFAULT,
         false},
        {"RDB stream when custom expected",
         "stream-kind mismatch",
         STREAM_KIND_RDB,
         0x7f,
         0,
         STREAM_READER_BUFFER_SIZE_DEFAULT,
         false},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        size_t payload_len = strlen(cases[i].payload);
        DynamicBuf db;
        dynamicBufInit(&db);

        streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, cases[i].writer_kind);
        streamWriter w;
        ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
        ASSERT_GE(streamWriterWrite(&w, cases[i].payload, payload_len), 0) << cases[i].name;
        ASSERT_EQ(streamWriterFinish(&w), 0) << cases[i].name;
        streamWriterFree(&w);

        MemReader mr = {};
        mr.data = db.data;
        mr.len = sdslen((const char *)db.data);
        mr.max_chunk = cases[i].max_chunk;
        streamReaderConfig rcfg = makeReaderConfig(cases[i].expected_kind, true, cases[i].buffer_size);
        streamReader r;
        ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr), 0);

        streamReaderInfo info;
        if (cases[i].expect_ok) {
            ASSERT_EQ(streamReaderGetInfo(&r, &info), 0) << cases[i].name;
            ASSERT_TRUE(info.compressed) << cases[i].name;
            ASSERT_EQ(info.algo, ALGO_LZ4) << cases[i].name;
            ASSERT_EQ(info.stream_kind, cases[i].expected_kind) << cases[i].name;

            uint8_t out[64] = {0};
            ASSERT_EQ(streamReaderRead(&r, out, payload_len), (ssize_t)payload_len) << cases[i].name;
            ASSERT_EQ(memcmp(out, cases[i].payload, payload_len), 0) << cases[i].name;
            ASSERT_EQ(streamReaderRead(&r, out, sizeof(out)), 0) << cases[i].name;
        } else {
            ASSERT_EQ(streamReaderGetInfo(&r, &info), -1) << cases[i].name;
            ASSERT_EQ(r.error_kind, STREAM_READER_ERROR_INCOMPATIBLE) << cases[i].name;

            uint8_t out[32] = {0};
            ASSERT_EQ(streamReaderRead(&r, out, sizeof(out)), -1) << cases[i].name;
        }

        streamReaderFree(&r);
        dynamicBufFree(&db);
    }
}

/* --- Test: streamReadEnvelopeInfo rejects envelopes this build cannot accept.
 * Builds a valid envelope, then flips one field at a time and asserts the
 * parse fails. Guards the version, codec-id, and reserved-flag-bit branches
 * that the format relies on for forward compatibility. */
TEST_F(CompressionTest, streamReadEnvelopeInfoRejectsBadEnvelopes) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&w, "negative envelope", 17), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    ASSERT_GE(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE);

    uint8_t good[VCS_ENVELOPE_SIZE];
    memcpy(good, db.data, VCS_ENVELOPE_SIZE);
    dynamicBufFree(&db);

    streamReaderInfo info;

    /* Sanity: the unmodified envelope parses. */
    ASSERT_EQ(streamReadEnvelopeInfo(good, VCS_ENVELOPE_SIZE, STREAM_KIND_RDB, &info), 0);

    /* Unknown version. */
    uint8_t bad_version[VCS_ENVELOPE_SIZE];
    memcpy(bad_version, good, VCS_ENVELOPE_SIZE);
    bad_version[VCS_OFFSET_VERSION] = VCS_VERSION + 1;
    ASSERT_EQ(streamReadEnvelopeInfo(bad_version, VCS_ENVELOPE_SIZE, STREAM_KIND_RDB, &info), -1)
        << "future version must be rejected";

    /* Unknown / non-streaming codec id. */
    uint8_t bad_algo[VCS_ENVELOPE_SIZE];
    memcpy(bad_algo, good, VCS_ENVELOPE_SIZE);
    bad_algo[VCS_OFFSET_ALGO] = 0x7f;
    ASSERT_EQ(streamReadEnvelopeInfo(bad_algo, VCS_ENVELOPE_SIZE, STREAM_KIND_RDB, &info), -1)
        << "unknown codec id must be rejected";

    /* LZF is a real algorithm but has no streaming codec. */
    uint8_t lzf_algo[VCS_ENVELOPE_SIZE];
    memcpy(lzf_algo, good, VCS_ENVELOPE_SIZE);
    lzf_algo[VCS_OFFSET_ALGO] = (uint8_t)ALGO_LZF;
    ASSERT_EQ(streamReadEnvelopeInfo(lzf_algo, VCS_ENVELOPE_SIZE, STREAM_KIND_RDB, &info), -1)
        << "non-streaming algorithm must be rejected";

    /* Reserved flag bit set. */
    uint8_t bad_flags[VCS_ENVELOPE_SIZE];
    memcpy(bad_flags, good, VCS_ENVELOPE_SIZE);
    bad_flags[VCS_OFFSET_FLAGS] |= (1 << 1);
    ASSERT_EQ(streamReadEnvelopeInfo(bad_flags, VCS_ENVELOPE_SIZE, STREAM_KIND_RDB, &info), -1)
        << "reserved flag bits must be rejected";
}

/* --- Test: stream_reader marks errored on partial output + read error.
 * Regression for direct-path error accounting (partial bytes were returned
 * without sticky errored state). */
TEST_F(CompressionTest, streamReaderPartialThenErrorSetsErrored) {
    const size_t payload_len = 64 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    uint32_t x = 0x12345678u;
    for (size_t i = 0; i < payload_len; i++) {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        payload[i] = (uint8_t)(x & 0xFF);
    }

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    const size_t first_chunk_len = 4 * 1024;
    ASSERT_GE(streamWriterWrite(&w, payload, first_chunk_len), 0);
    ASSERT_EQ(streamWriterFlush(&w), 0);
    size_t fail_after_pos = sdslen((const char *)db.data);
    ASSERT_GE(streamWriterWrite(&w, payload + first_chunk_len, payload_len - first_chunk_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    FlakyReader fr = {};
    fr.data = db.data;
    fr.len = sdslen((const char *)db.data);
    fr.max_chunk = 4096;
    fr.fail_after_pos = fail_after_pos;
    fr.fail_after_success_reads = (std::numeric_limits<int>::max)();
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false, 8 * 1024);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, flakyReaderRead, &fr), 0);

    const size_t out_len = 16 * 1024;
    uint8_t *out = (uint8_t *)zmalloc(out_len);
    ASSERT_NE(out, nullptr);
    ssize_t n1 = streamReaderRead(&r, out, out_len);
    ASSERT_GT(n1, 0) << "first read should return partial output";
    ASSERT_LT(n1, (ssize_t)out_len) << "injected read error should stop the first read early";
    ASSERT_EQ(memcmp(out, payload, (size_t)n1), 0);
    ASSERT_EQ(streamReaderRead(&r, out, out_len), -1) << "second read should fail immediately";

    streamReaderFree(&r);

    /* Passthrough mode should also preserve partial bytes when source read
     * fails after probe/prefix buffering, then latch sticky error state. */
    const uint8_t plain[] = "NOTVCS-passthrough-regression";
    FlakyReader fr_passthrough = {};
    fr_passthrough.data = plain;
    fr_passthrough.len = sizeof(plain) - 1;
    fr_passthrough.max_chunk = 0;
    fr_passthrough.fail_after_success_reads = 1; /* probe succeeds, next read fails */
    streamReaderConfig pass_cfg = makeReaderConfig(STREAM_KIND_RDB, true);
    streamReader rp;
    ASSERT_EQ(streamReaderInit(&rp, &pass_cfg, flakyReaderRead, &fr_passthrough), 0);

    uint8_t pass_out[64];
    ssize_t p1 = streamReaderRead(&rp, pass_out, sizeof(pass_out));
    ASSERT_GT(p1, 0) << "passthrough first read should return partial output";
    ASSERT_EQ(memcmp(pass_out, plain, (size_t)p1), 0) << "passthrough partial bytes should match input prefix";
    ASSERT_EQ(streamReaderRead(&rp, pass_out, sizeof(pass_out)), -1)
        << "passthrough second read should fail immediately";
    streamReaderFree(&rp);
    zfree(out);

    dynamicBufFree(&db);
    zfree(payload);
}

/* --- Test: streamWriterInit/free --- */
TEST_F(CompressionTest, streamWriterInitFree) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(t.errored, false) << "should not be errored";

    streamWriterFree(&t);
    dynamicBufFree(&db);

    /* ALGO_NONE should fail */
    streamWriterConfig bad_cfg = makeWriterConfig(ALGO_NONE, 0, STREAM_KIND_RDB);
    streamWriter bad_writer;
    ASSERT_EQ(streamWriterInit(&bad_writer, &bad_cfg, emitToDynamicBuf, &db), -1) << "ALGO_NONE should fail init";
    bad_cfg = makeWriterConfig(ALGO_LZF, 0, STREAM_KIND_RDB);
    ASSERT_EQ(streamWriterInit(&bad_writer, &bad_cfg, emitToDynamicBuf, &db), -1) << "ALGO_LZF should fail init";

    /* Concrete stream kinds outside the currently named ones are valid. */
    streamWriterConfig future_kind_cfg = makeWriterConfig(ALGO_LZ4, 0, 0x7f);
    streamWriter future_t;
    ASSERT_EQ(streamWriterInit(&future_t, &future_kind_cfg, emitToDynamicBuf, &db), 0);
    streamWriterFree(&future_t);
}

/* --- Test: stream_writer write + finish round-trip --- */
TEST_F(CompressionTest, streamWriterRoundTrip) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    /* Write some data */
    const char *test_data = "Hello, compression world! This is a test of the stream writer API.";
    size_t data_len = strlen(test_data);
    ASSERT_GE(streamWriterWrite(&t, test_data, data_len), 0);
    ASSERT_EQ(t.errored, false) << "should not be errored after write";
    ASSERT_GE(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE) << "write should emit envelope";

    /* Finalize */
    ASSERT_EQ(streamWriterFinish(&t), 0);
    ASSERT_EQ(t.errored, false) << "should not be errored after finish";

    /* Verify output starts with VCS envelope */
    ASSERT_GE(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE)
        << "output should have at least envelope size";
    ASSERT_EQ(db.data[0], VCS_MAGIC_0) << "magic byte 0";
    ASSERT_EQ(db.data[1], VCS_MAGIC_1) << "magic byte 1";
    ASSERT_EQ(db.data[2], VCS_MAGIC_2) << "magic byte 2";
    ASSERT_EQ(db.data[3], VCS_VERSION) << "version";
    ASSERT_EQ(db.data[4], ALGO_LZ4) << "algo id";
    ASSERT_EQ(db.data[5], (uint8_t)0) << "flags should be clear when checksum is disabled";
    ASSERT_EQ(db.data[6], STREAM_KIND_RDB) << "stream_kind RDB";

    /* Decompress and verify round-trip */
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0);

    uint8_t decompressed[256];
    size_t total_decompressed = 0;
    uint8_t *compressed_data = db.data + VCS_ENVELOPE_SIZE;
    size_t compressed_len = sdslen((const char *)db.data) - VCS_ENVELOPE_SIZE;
    size_t src_offset = 0;

    while (src_offset < compressed_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressorFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            compressed_data + src_offset,
            compressed_len - src_offset, &consumed);
        ASSERT_GE(produced, 0) << "decompression should not fail";
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_EQ(total_decompressed, data_len) << "decompressed length should match original";
    ASSERT_EQ(memcmp(decompressed, test_data, data_len), 0) << "decompressed data should match original";

    streamDecompressorFree(&sd);
    streamWriterFree(&t);
    dynamicBufFree(&db);
}

/* --- Test: a single large write is chunked internally without changing
 * the logical stream. This exercises the bounded scratch-buffer path. --- */
TEST_F(CompressionTest, streamWriterLargeSingleWrite) {
    const size_t payload_len = (1024 * 1024) + 4096;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 17 + 11) % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&t, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data);
    mr.max_chunk = 0;
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false, 64 * 1024);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr), 0);

    uint8_t *out = (uint8_t *)zmalloc(payload_len);
    size_t total = 0;
    while (total < payload_len) {
        ssize_t nread = streamReaderRead(&r, out + total, payload_len - total);
        ASSERT_GT(nread, 0) << "streamReaderRead should keep making progress";
        total += (size_t)nread;
    }
    ASSERT_EQ(memcmp(out, payload, payload_len), 0);
    ASSERT_EQ(streamReaderRead(&r, out, 1), 0) << "reader should stop at frame end";

    zfree(out);
    zfree(payload);
    streamReaderFree(&r);
    dynamicBufFree(&db);
}

/* --- Test: small caller reads should still drain a compressed stream
 * correctly. This exercises the buffered decompressed window path. --- */
TEST_F(CompressionTest, streamReaderSmallReadsRoundTrip) {
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    ASSERT_NE(payload, nullptr);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 29 + 7) % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, -5, STREAM_KIND_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data);
    mr.max_chunk = 4096;
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false, 64 * 1024);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr), 0);

    uint8_t *out = (uint8_t *)zmalloc(payload_len);
    ASSERT_NE(out, nullptr);
    size_t total = 0;
    while (total < payload_len) {
        size_t step = payload_len - total;
        if (step > 17) step = 17;
        ssize_t nread = streamReaderRead(&r, out + total, step);
        ASSERT_GT(nread, 0) << "streamReaderRead should keep making progress";
        total += (size_t)nread;
    }

    ASSERT_EQ(memcmp(out, payload, payload_len), 0);
    ASSERT_EQ(streamReaderRead(&r, out, 1), 0) << "reader should stop at frame end";

    zfree(out);
    zfree(payload);
    streamReaderFree(&r);
    dynamicBufFree(&db);
}

/* --- Test: streamWriterFlush semantics (no-op before writes, valid mid-stream) --- */
TEST_F(CompressionTest, streamWriterFlushBehavior) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterFlush(&t), 0) << "flush before write should be no-op success";
    ASSERT_EQ(sdslen((const char *)db.data), 0u) << "flush before write should not emit bytes";

    ASSERT_GE(streamWriterWrite(&t, "first chunk", 11), 0);
    ASSERT_EQ(streamWriterFlush(&t), 0);
    ASSERT_GE(streamWriterWrite(&t, "second chunk", 12), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);

    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE);
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0);

    uint8_t decompressed[256];
    size_t total_decompressed = 0;
    uint8_t *comp_data = db.data + VCS_ENVELOPE_SIZE;
    size_t comp_len = sdslen((const char *)db.data) - VCS_ENVELOPE_SIZE;
    size_t src_offset = 0;

    while (src_offset < comp_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressorFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            comp_data + src_offset,
            comp_len - src_offset, &consumed);
        ASSERT_GE(produced, 0);
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_EQ(total_decompressed, 23u);
    ASSERT_EQ(memcmp(decompressed, "first chunksecond chunk", 23), 0);

    streamDecompressorFree(&sd);
    streamWriterFree(&t);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamWriterFlushAfterFinishIsNoop) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_GE(streamWriterWrite(&t, "payload", 7), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    size_t len_after_finish = sdslen((const char *)db.data);

    ASSERT_EQ(streamWriterFlush(&t), 0) << "flush after finish should be a no-op success";
    ASSERT_EQ(sdslen((const char *)db.data), len_after_finish) << "flush after finish should not emit bytes";

    streamWriterFree(&t);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamWriterCodecChecksumToggle) {
    const char *payload = "codec checksum payload codec checksum payload";

    for (bool codec_checksum : {false, true}) {
        DynamicBuf db;
        dynamicBufInit(&db);

        streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB,
                                                  codec_checksum);
        streamWriter t;
        ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);
        ASSERT_GE(streamWriterWrite(&t, payload, strlen(payload)), 0);
        ASSERT_EQ(streamWriterFinish(&t), 0);

        ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE);
        ASSERT_EQ(lz4FrameHasBlockChecksum(db.data + VCS_ENVELOPE_SIZE,
                                           sdslen((const char *)db.data) - VCS_ENVELOPE_SIZE),
                  codec_checksum)
            << "LZ4 block checksum should reflect configured codec checksum setting";
        ASSERT_EQ(lz4FrameHasContentChecksum(db.data + VCS_ENVELOPE_SIZE,
                                             sdslen((const char *)db.data) - VCS_ENVELOPE_SIZE),
                  codec_checksum)
            << "LZ4 content checksum should reflect configured codec checksum setting";

        streamWriterFree(&t);
        dynamicBufFree(&db);
    }
}

TEST_F(CompressionTest, streamReaderValidateEndAcceptsClosedFrame) {
    const char *payload = "validate frame end payload";
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB, true);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&w, payload, strlen(payload)), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);

    MemReader reader_ctx = {db.data, sdslen((const char *)db.data), 0, 7};
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &reader_ctx), 0);

    char out[64];
    ASSERT_EQ(streamReaderRead(&reader, out, strlen(payload)), (ssize_t)strlen(payload));
    ASSERT_EQ(memcmp(out, payload, strlen(payload)), 0);
    ASSERT_EQ(streamReaderValidateEnd(&reader), 0);

    streamReaderFree(&reader);
    streamWriterFree(&w);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamReaderValidateEndRejectsTrailingBytes) {
    const char *payload = "payload with trailing compressed bytes";
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB, true);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&w, payload, strlen(payload)), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    db.data = (uint8_t *)sdscatlen((sds)db.data, "x", 1);

    MemReader reader_ctx = {db.data, sdslen((const char *)db.data), 0, 0};
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &reader_ctx), 0);

    char out[64];
    ASSERT_EQ(streamReaderRead(&reader, out, strlen(payload)), (ssize_t)strlen(payload));
    ASSERT_EQ(streamReaderValidateEnd(&reader), -1);
    ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_CORRUPT);

    streamReaderFree(&reader);
    streamWriterFree(&w);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamReaderValidateEndRejectsUnreadDecodedBytes) {
    const char *payload = "payload with unread decoded suffix";
    const size_t payload_len = strlen(payload);
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB, true);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);

    MemReader reader_ctx = {db.data, sdslen((const char *)db.data), 0, 0};
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &reader_ctx), 0);

    char out[8];
    ASSERT_EQ(streamReaderRead(&reader, out, sizeof(out)), (ssize_t)sizeof(out));
    ASSERT_EQ(memcmp(out, payload, sizeof(out)), 0);
    ASSERT_EQ(streamReaderValidateEnd(&reader), -1);
    ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_CORRUPT);

    streamReaderFree(&reader);
    streamWriterFree(&w);
    dynamicBufFree(&db);
}

/* --- Test: compressRio write + finish round-trip --- */
TEST_F(CompressionTest, compressRioRoundTrip) {
    /* Use a buffer rio as the inner rio */
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &buffer_rio, ALGO_LZ4, false), 0);
    const char *test_data = "The quick brown fox jumps over the lazy dog. "
                            "Pack my box with five dozen liquor jugs.";
    size_t data_len = strlen(test_data);
    ASSERT_NE(rioWrite((rio *)&cr, test_data, data_len), 0u) << "rioWrite should succeed";

    /* Finalize and free */
    ASSERT_EQ(compressRioFinish(&cr), 0);

    /* Get the compressed output from the buffer rio */
    sds compressed = buffer_rio.io.buffer.ptr;
    size_t compressed_len = sdslen(compressed);
    ASSERT_GT(compressed_len, (size_t)VCS_ENVELOPE_SIZE) << "compressed output should exist";

    /* Verify VCS envelope */
    ASSERT_EQ(compressed[0], (char)VCS_MAGIC_0) << "magic V";
    ASSERT_EQ(compressed[1], (char)VCS_MAGIC_1) << "magic C";
    ASSERT_EQ(compressed[2], (char)VCS_MAGIC_2) << "magic S";
    ASSERT_EQ(compressed[3], (char)VCS_VERSION) << "version";

    /* Decompress and verify */
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0);

    uint8_t decompressed[512];
    size_t total_decompressed = 0;
    uint8_t *comp_data = (uint8_t *)compressed + VCS_ENVELOPE_SIZE;
    size_t comp_len = compressed_len - VCS_ENVELOPE_SIZE;
    size_t src_offset = 0;

    while (src_offset < comp_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressorFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            comp_data + src_offset,
            comp_len - src_offset, &consumed);
        ASSERT_GE(produced, 0) << "decompression should not fail";
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_EQ(total_decompressed, data_len) << "decompressed length should match";
    ASSERT_EQ(memcmp(decompressed, test_data, data_len), 0) << "decompressed data should match";

    streamDecompressorFree(&sd);
    compressRioFree(&cr);
    sdsfree(compressed);
}

TEST_F(CompressionTest, compressRioDoesNotInstallRdbChecksum) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &buffer_rio, ALGO_LZ4, false), 0);
    ASSERT_EQ(cr.base.update_cksum, nullptr);

    const char *payload = "checksum-payload-for-compressed-rio";
    size_t payload_len = strlen(payload);
    ASSERT_NE(rioWrite((rio *)&cr, payload, payload_len), 0u);

    ASSERT_EQ(cr.base.cksum, 0u) << "compress_rio should not own RDB checksum policy";

    ASSERT_EQ(compressRioFinish(&cr), 0);
    compressRioFree(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
}

TEST_F(CompressionTest, compressRioDoesNotCopyRdbChecksumFlags) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);
    buffer_rio.flags |= RIO_FLAG_SKIP_RDB_CHECKSUM;

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &buffer_rio, ALGO_LZ4, false), 0);
    ASSERT_FALSE(((rio *)&cr)->flags & RIO_FLAG_SKIP_RDB_CHECKSUM);

    const char *payload = "skip-checksum-payload";
    ASSERT_NE(rioWrite((rio *)&cr, payload, strlen(payload)), 0u);
    ASSERT_EQ(cr.base.cksum, 0u) << "compress_rio should not inspect RDB checksum flags";

    ASSERT_EQ(compressRioFinish(&cr), 0);
    compressRioFree(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
}

TEST_F(CompressionTest, compressRioCodecChecksumDoesNotInstallRdbChecksum) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &buffer_rio, ALGO_LZ4, true), 0);

    const char *payload = "codec-checksum-enabled";
    ASSERT_NE(rioWrite((rio *)&cr, payload, strlen(payload)), 0u);
    ASSERT_EQ(cr.base.cksum, 0u)
        << "codec checksums should not control standard RDB checksum tracking";

    ASSERT_EQ(compressRioFinish(&cr), 0);
    compressRioFree(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
}

TEST_F(CompressionTest, compressRioFlushFailureSetsWriteError) {
    rio inner;
    initFailingFlushRio(&inner);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &inner, ALGO_LZ4, false), 0);

    const char *payload = "flush failure should latch rio write error";
    ASSERT_NE(rioWrite((rio *)&cr, payload, strlen(payload)), 0u);
    ASSERT_EQ(rioFlush((rio *)&cr), 0);
    ASSERT_TRUE(((rio *)&cr)->flags & RIO_FLAG_WRITE_ERROR);

    compressRioFree(&cr);
}

TEST_F(CompressionTest, compressRioFinishFailureSetsWriteError) {
    rio inner;
    initFailingFlushRio(&inner);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &inner, ALGO_LZ4, false), 0);

    const char *payload = "finish failure should latch rio write error";
    ASSERT_NE(rioWrite((rio *)&cr, payload, strlen(payload)), 0u);
    ASSERT_EQ(compressRioFinish(&cr), -1);
    ASSERT_TRUE(((rio *)&cr)->flags & RIO_FLAG_WRITE_ERROR);

    compressRioFree(&cr);
}

TEST_F(CompressionTest, rioDecoratorsPreserveConnBackedFlag) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &buffer_rio, ALGO_LZ4, false), 0);
    ASSERT_FALSE(rioIsConnBacked((rio *)&cr));
    compressRioFree(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);

    sds raw = sdsnew("plain-rdb-prefix");
    rio raw_rio;
    rioInitWithBuffer(&raw_rio, raw);
    decompressRio dr;
    ASSERT_EQ(rioInitWithRdbDecompression(&dr, &raw_rio, nullptr), DECOMPRESS_RIO_INIT_OK);
    ASSERT_FALSE(rioIsConnBacked((rio *)&dr));
    decompressRioFree(&dr);
    sdsfree(raw_rio.io.buffer.ptr);

    sds conn_backed_buf = sdsempty();
    rio conn_backed_rio;
    rioInitWithBuffer(&conn_backed_rio, conn_backed_buf);
    conn_backed_rio.flags |= RIO_FLAG_CONN_BACKED;

    ASSERT_EQ(rioInitWithRdbCompression(&cr, &conn_backed_rio, ALGO_LZ4, false), 0);
    ASSERT_TRUE(rioIsConnBacked((rio *)&cr));
    compressRioFree(&cr);

    sds conn_backed_raw = sdsnew("plain-rdb-prefix");
    rio conn_backed_raw_rio;
    rioInitWithBuffer(&conn_backed_raw_rio, conn_backed_raw);
    conn_backed_raw_rio.flags |= RIO_FLAG_CONN_BACKED;
    ASSERT_EQ(rioInitWithRdbDecompression(&dr, &conn_backed_raw_rio, nullptr), DECOMPRESS_RIO_INIT_OK);
    ASSERT_TRUE(rioIsConnBacked((rio *)&dr));
    decompressRioFree(&dr);

    sdsfree(conn_backed_rio.io.buffer.ptr);
    sdsfree(conn_backed_raw_rio.io.buffer.ptr);
}

/* --- Test: decompressRio read round-trip --- */
TEST_F(CompressionTest, decompressRioRoundTrip) {
    /* First, produce compressed data using stream writer */
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    const char *test_data = "Decompression rio test data. "
                            "This should round-trip through compress then decompress.";
    size_t data_len = strlen(test_data);
    ASSERT_GE(streamWriterWrite(&t, test_data, data_len), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    /* Create a buffer rio with the full VCS-wrapped stream. */
    sds comp_sds = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    /* Create decompress rio */
    decompressRio dr;
    ASSERT_EQ(initVcsRdbDecompressRio(&dr, &buffer_rio), 0);

    /* Read decompressed data */
    char result[256];
    memset(result, 0, sizeof(result));
    ASSERT_NE(rioRead((rio *)&dr, result, data_len), 0u) << "rioRead should succeed";
    ASSERT_EQ(memcmp(result, test_data, data_len), 0) << "decompressed data should match original";

    decompressRioFree(&dr);
    sdsfree(comp_sds);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, decompressRioTellTracksSourceProgress) {
    DynamicBuf db;
    dynamicBufInit(&db);

    std::string payload(4096, 'A');
    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&t, payload.data(), payload.size()), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    sds comp_sds = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    decompressRio dr;
    ASSERT_EQ(initVcsRdbDecompressRio(&dr, &buffer_rio), 0);

    char out[2048];
    ASSERT_NE(rioRead((rio *)&dr, out, sizeof(out)), 0u);
    ASSERT_EQ(rioTell((rio *)&dr), rioTell(&buffer_rio));
    ASSERT_LT((size_t)rioTell((rio *)&dr), sizeof(out))
        << "decompress rio tell should track source bytes, not logical output bytes";

    decompressRioFree(&dr);
    sdsfree(comp_sds);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, decompressRioClassifiesInput) {
    {
        const char *payload = "REDIS001remaining data after prefix";
        size_t payload_len = strlen(payload);
        sds buf = sdsnewlen(payload, payload_len);
        rio buffer_rio;
        rioInitWithBuffer(&buffer_rio, buf);

        decompressRio dr;
        compressionAlgo algo = ALGO_NONE;
        ASSERT_EQ(rioInitWithRdbDecompression(&dr, &buffer_rio, &algo), DECOMPRESS_RIO_INIT_OK);
        ASSERT_EQ(algo, ALGO_NONE) << "passthrough stream should not be compressed";

        char result[64];
        memset(result, 0, sizeof(result));
        ASSERT_NE(rioRead((rio *)&dr, result, payload_len), 0u) << "rioRead should succeed";
        ASSERT_EQ(memcmp(result, payload, payload_len), 0) << "payload should be replayed exactly";

        decompressRioFree(&dr);
        sdsfree(buf);
    }

    {
        const uint8_t malformed[VCS_ENVELOPE_SIZE] = {
            VCS_MAGIC_0, VCS_MAGIC_1, VCS_MAGIC_2, 0, ALGO_LZ4, 0, STREAM_KIND_RDB};
        sds buf = sdsnewlen(malformed, sizeof(malformed));
        rio buffer_rio;
        rioInitWithBuffer(&buffer_rio, buf);

        decompressRio dr;
        ASSERT_EQ(rioInitWithRdbDecompression(&dr, &buffer_rio, nullptr), DECOMPRESS_RIO_INIT_INCOMPATIBLE);

        sdsfree(buf);
    }
}

/* --- Test: compressRioFinish is idempotent --- */
TEST_F(CompressionTest, compressRioFinishIdempotent) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &buffer_rio, ALGO_LZ4, false), 0);

    ASSERT_NE(rioWrite((rio *)&cr, "test", 4), 0u);
    ASSERT_EQ(compressRioFinish(&cr), 0);
    size_t len_after_first = sdslen(buffer_rio.io.buffer.ptr);

    /* Second finish should be a no-op */
    ASSERT_EQ(compressRioFinish(&cr), 0);
    size_t len_after_second = sdslen(buffer_rio.io.buffer.ptr);
    ASSERT_EQ(len_after_first, len_after_second) << "second finish should not produce more output";

    compressRioFree(&cr);
    sdsfree(buffer_rio.io.buffer.ptr);
}

/* --- Test: compress_rio flush mid-stream does not end frame --- */
TEST_F(CompressionTest, compressRioFlushMidStream) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &buffer_rio, ALGO_LZ4, false), 0);

    /* Write some data */
    ASSERT_NE(rioWrite((rio *)&cr, "first chunk", 11), 0u);

    /* Flush mid-stream — should NOT end the frame */
    ASSERT_NE(rioFlush((rio *)&cr), 0) << "flush should succeed";

    /* Write more data — should succeed because frame is still open */
    ASSERT_NE(rioWrite((rio *)&cr, "second chunk", 12), 0u) << "write after flush should succeed";

    /* Now finalize */
    ASSERT_EQ(compressRioFinish(&cr), 0);

    /* Verify the entire stream decompresses correctly */
    sds compressed = buffer_rio.io.buffer.ptr;
    size_t compressed_len = sdslen(compressed);
    ASSERT_GT(compressed_len, (size_t)VCS_ENVELOPE_SIZE);

    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0);

    uint8_t decompressed[256];
    size_t total_decompressed = 0;
    uint8_t *comp_data = (uint8_t *)compressed + VCS_ENVELOPE_SIZE;
    size_t comp_len = compressed_len - VCS_ENVELOPE_SIZE;
    size_t src_offset = 0;

    while (src_offset < comp_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressorFeed(
            &sd, decompressed + total_decompressed,
            sizeof(decompressed) - total_decompressed,
            comp_data + src_offset,
            comp_len - src_offset, &consumed);
        ASSERT_GE(produced, 0) << "decompression should not fail";
        total_decompressed += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_EQ(total_decompressed, 23u) << "total decompressed should be 23 bytes";
    ASSERT_EQ(memcmp(decompressed, "first chunksecond chunk", 23), 0)
        << "decompressed should match concatenated input";

    streamDecompressorFree(&sd);
    compressRioFree(&cr);
    sdsfree(compressed);
}

/* --- Test: rdbLoadProgressCallback does not cast write-side streaming rios
 * to decompressRio. This protects save/async paths that also use
 * RIO_FLAG_STREAMING_COMPRESSION. --- */
TEST_F(CompressionTest, rdbLoadProgressCallbackStreamingGuard) {
    sds buf = sdsempty();
    rio inner;
    rioInitWithBuffer(&inner, buf);

    compressRio cr;
    ASSERT_EQ(rioInitWithRdbCompression(&cr, &inner, ALGO_LZ4, false), 0);

    const char sample[] = "progress-guard";
    rdbLoadProgressCallback((rio *)&cr, sample, sizeof(sample) - 1);

    ASSERT_FALSE(cr.base.flags & RIO_FLAG_READ_ERROR) << "write-side streaming rio must not set read error";

    compressRioFree(&cr);
    sdsfree(inner.io.buffer.ptr);
}

/* --- Test: decompress_rio with large payload (>64KB) exercises partial
 * consume in the large-chunk read path. Before the fix, unconsumed
 * compressed bytes were dropped between iterations, causing false EOF
 * or data corruption. --- */
TEST_F(CompressionTest, decompressRioLargePayload) {
    /* Generate a large payload (256KB) with a repeating pattern so
     * it's compressible but large enough to require multiple
     * decompression iterations. */
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i % 251); /* prime modulus for variety */
    }

    /* Compress via stream_writer */
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_GE(streamWriterWrite(&t, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    /* Decompress via decompress_rio using the full VCS-wrapped stream. */
    sds comp_sds = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    decompressRio dr;
    ASSERT_EQ(initVcsRdbDecompressRio(&dr, &buffer_rio), 0);

    /* Read in small chunks (4KB) to force multiple iterations through
     * the decompression state machine. */
    uint8_t *result = (uint8_t *)zmalloc(payload_len);
    size_t total_read = 0;
    while (total_read < payload_len) {
        size_t chunk = 4096;
        if (chunk > payload_len - total_read) chunk = payload_len - total_read;
        size_t ret = rioRead((rio *)&dr, result + total_read, chunk);
        ASSERT_NE(ret, 0u) << "rioRead should succeed for large payload";
        total_read += chunk;
    }

    ASSERT_EQ(memcmp(result, payload, payload_len), 0) << "decompressed data should match original";

    decompressRioFree(&dr);
    sdsfree(comp_sds);
    zfree(result);
    zfree(payload);
    dynamicBufFree(&db);
}

/* --- Test: decompressRioRead handles a large single rioRead request.
 * Verifies correctness for a single 128KB read through the buffered path. --- */
TEST_F(CompressionTest, decompressRioDirectPath) {
    /* Generate a large payload (128KB). */
    const size_t payload_len = 128 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 7 + 13) % 256);
    }

    /* Compress */
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_GE(streamWriterWrite(&t, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    /* Decompress via decompress_rio with a single large read. */
    sds comp_sds = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    decompressRio dr;
    ASSERT_EQ(initVcsRdbDecompressRio(&dr, &buffer_rio), 0);

    uint8_t *result = (uint8_t *)zmalloc(payload_len);
    size_t ret = rioRead((rio *)&dr, result, payload_len);
    ASSERT_NE(ret, 0u) << "single large rioRead should succeed";
    ASSERT_EQ(memcmp(result, payload, payload_len), 0) << "decompressed data should match original";

    decompressRioFree(&dr);
    sdsfree(comp_sds);
    zfree(result);
    zfree(payload);
    dynamicBufFree(&db);
}

/* --- Test: stream_reader stops at the end of the compressed frame even if
 * the underlying source still has trailing bytes. This keeps caller-managed
 * framing viable on long-lived streams. --- */
TEST_F(CompressionTest, streamReaderStopsAtFrameEndBeforeTrailingBytes) {
    const char *payload = "stream-reader-frame-end";
    const size_t payload_len = strlen(payload);
    const char *trailer = "TRAILER-BYTES-AFTER-FRAME";
    const size_t trailer_len = strlen(trailer);

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);
    size_t frame_len = sdslen((const char *)db.data);

    sds input = sdsnewlen(db.data, sdslen((const char *)db.data));
    input = sdscatlen(input, trailer, trailer_len);

    MemReader mr = {};
    mr.data = (const uint8_t *)input;
    mr.len = sdslen(input);
    mr.max_chunk = 3;
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false, 8);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr), 0);

    char out[128];
    memset(out, 0, sizeof(out));
    ASSERT_EQ(streamReaderRead(&r, out, payload_len), (ssize_t)payload_len);
    ASSERT_EQ(memcmp(out, payload, payload_len), 0);

    /* The reader must stop cleanly at frame end instead of trying to decode
     * trailing bytes as part of the same compressed frame. */
    ASSERT_EQ(streamReaderRead(&r, out, sizeof(out)), 0);
    ASSERT_EQ(mr.pos, frame_len) << "streamReader must not consume bytes after the LZ4 frame";

    streamReaderFree(&r);
    sdsfree(input);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamReaderRejectsTruncatedFrameTrailer) {
    const size_t payload_len = 256;
    uint8_t payload[payload_len];
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i & 0xFF);
    }

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE + 1);
    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data) - 1;
    mr.max_chunk = 7;
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false, 8);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr), 0);

    uint8_t out[payload_len];
    ASSERT_EQ(streamReaderRead(&r, out, payload_len), (ssize_t)payload_len);
    ASSERT_EQ(memcmp(out, payload, payload_len), 0);
    ASSERT_LT(streamReaderRead(&r, out, 1), 0) << "EOF before frame end must still fail the read";
    ASSERT_EQ(r.error_kind, STREAM_READER_ERROR_TRUNCATED)
        << "EOF before frame end is a recoverable truncation (e.g. a dropped connection), not codec corruption";

    streamReaderFree(&r);
    dynamicBufFree(&db);
}

/* --- Test: a mutated byte inside the compressed frame body latches CORRUPT,
 * not TRUNCATED. This is the sibling of streamReaderRejectsTruncatedFrameTrailer:
 * truncation (clean EOF mid-frame) is recoverable and the replica should resync,
 * whereas genuine in-frame corruption is fatal and the replica aborts the load.
 * The block checksum is enabled so a single flipped body byte is reliably caught
 * by the codec instead of decompressing to wrong-but-valid data. --- */
TEST_F(CompressionTest, streamReaderRejectsCorruptedFrameBody) {
    const size_t payload_len = 256;
    uint8_t payload[payload_len];
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i & 0xFF);
    }

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB, /*codec_checksum_enabled=*/true);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_GE(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    size_t frame_len = sdslen((const char *)db.data);
    ASSERT_GT(frame_len, (size_t)VCS_ENVELOPE_SIZE + 16);
    ASSERT_TRUE(lz4FrameHasBlockChecksum(db.data + VCS_ENVELOPE_SIZE, frame_len - VCS_ENVELOPE_SIZE))
        << "test frame must carry a block checksum for deterministic corruption detection";

    /* Flip a byte in the middle of the buffer: past the VCS envelope and the LZ4
     * frame header (so it lands in checksum-covered compressed block data), and
     * before the trailing block checksum / end mark. */
    size_t flip_off = (VCS_ENVELOPE_SIZE + frame_len) / 2;
    ASSERT_GT(flip_off, (size_t)VCS_ENVELOPE_SIZE);
    ASSERT_LT(flip_off, frame_len);
    db.data[flip_off] ^= 0xFF;

    MemReader mr = {};
    mr.data = db.data;
    mr.len = frame_len;
    mr.max_chunk = 7;
    streamReaderConfig rcfg = makeReaderConfig(STREAM_KIND_RDB, false, 8);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr), 0);

    /* Drain until the codec latches the failure. The error may surface on the
     * first read or a follow-up read depending on how much the codec buffers
     * before validating the block checksum; either way the read must fail. */
    uint8_t out[payload_len];
    ssize_t n = streamReaderRead(&r, out, payload_len);
    while (n > 0) n = streamReaderRead(&r, out, payload_len);
    ASSERT_LT(n, 0) << "in-frame corruption must fail the read";
    ASSERT_EQ(r.error_kind, STREAM_READER_ERROR_CORRUPT)
        << "a mutated frame body is genuine codec corruption (replica aborts the load), "
           "distinct from a recoverable truncation that triggers a resync";

    streamReaderFree(&r);
    dynamicBufFree(&db);
}

/* --- Test: streamWriterWrite after finish is rejected.
 * Writes after finish must fail and must not emit bytes. --- */
TEST_F(CompressionTest, streamWriterWriteAfterFinish) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_GE(streamWriterWrite(&t, "hello", 5), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    size_t len_after_finish = sdslen((const char *)db.data);

    /* Write after finish must fail and emit no output. */
    ASSERT_LT(streamWriterWrite(&t, "world", 5), 0);
    ASSERT_EQ(sdslen((const char *)db.data), len_after_finish) << "write after finish should not produce output";

    /* Second finish — should also be a no-op */
    ASSERT_EQ(streamWriterFinish(&t), 0);
    ASSERT_EQ(sdslen((const char *)db.data), len_after_finish) << "second finish should not produce output";

    /* Verify the stream is still valid: one envelope + one frame */
    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE);
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0);

    uint8_t decompressed[64];
    size_t total = 0;
    uint8_t *cdata = db.data + VCS_ENVELOPE_SIZE;
    size_t comp_len = sdslen((const char *)db.data) - VCS_ENVELOPE_SIZE;
    size_t off = 0;
    while (off < comp_len) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressorFeed(
            &sd, decompressed + total, sizeof(decompressed) - total,
            cdata + off, comp_len - off, &consumed);
        ASSERT_GE(produced, 0);
        total += (size_t)produced;
        off += consumed;
        if (consumed == 0 && produced == 0) break;
    }

    ASSERT_EQ(total, 5u);
    ASSERT_EQ(memcmp(decompressed, "hello", 5), 0) << "should decompress to 'hello' only";

    streamDecompressorFree(&sd);
    streamWriterFree(&t);
    dynamicBufFree(&db);
}

/* Test that two independent compress/decompress streams can coexist
 * without interfering with each other. Verifies no shared mutable state. */
TEST_F(CompressionTest, independentStreamsCoexist) {
    /* Create two independent compress streams with different data */
    DynamicBuf db1, db2;
    dynamicBufInit(&db1);
    dynamicBufInit(&db2);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t1;
    ASSERT_EQ(streamWriterInit(&t1, &cfg, emitToDynamicBuf, &db1), 0);
    streamWriter t2;
    ASSERT_EQ(streamWriterInit(&t2, &cfg, emitToDynamicBuf, &db2), 0);

    const char *data1 = "Stream one data - unique content for first stream AAAA";
    const char *data2 = "Stream two data - different content for second stream BBBB";

    /* Interleave writes to both streams */
    ASSERT_GE(streamWriterWrite(&t1, data1, strlen(data1)), 0);
    ASSERT_GE(streamWriterWrite(&t2, data2, strlen(data2)), 0);
    ASSERT_GE(streamWriterWrite(&t1, data1, strlen(data1)), 0); /* write again to stream 1 */
    ASSERT_GE(streamWriterWrite(&t2, data2, strlen(data2)), 0); /* write again to stream 2 */

    ASSERT_EQ(streamWriterFinish(&t1), 0);
    ASSERT_EQ(streamWriterFinish(&t2), 0);

    /* Decompress both and verify independently */
    for (int i = 0; i < 2; i++) {
        DynamicBuf *db = (i == 0) ? &db1 : &db2;
        const char *expected = (i == 0) ? data1 : data2;
        size_t expected_len = strlen(expected) * 2; /* written twice */

        sds comp = sdsnewlen(db->data, sdslen((const char *)db->data));
        rio buf_rio;
        rioInitWithBuffer(&buf_rio, comp);

        decompressRio dr;
        ASSERT_EQ(initVcsRdbDecompressRio(&dr, &buf_rio), 0);

        char result[256];
        memset(result, 0, sizeof(result));
        ASSERT_NE(rioRead((rio *)&dr, result, expected_len), 0u) << "rioRead should succeed for coexisting stream";
        ASSERT_EQ(memcmp(result, expected, strlen(expected)), 0) << "first half should match";
        ASSERT_EQ(memcmp(result + strlen(expected), expected, strlen(expected)), 0) << "second half should match";

        decompressRioFree(&dr);
        sdsfree(comp);
    }

    streamWriterFree(&t1);
    streamWriterFree(&t2);
    dynamicBufFree(&db1);
    dynamicBufFree(&db2);
}

TEST_F(CompressionTest, streamWriterRepetitivePayloadRoundTrip) {
    /* Default block mode is linked.
     * Verify repetitive data still round-trips correctly. */
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, STREAM_KIND_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    /* Write repetitive data to a single stream */
    char pattern[4096];
    memset(pattern, 'X', sizeof(pattern));
    for (int i = 0; i < 32; i++) {
        ASSERT_GE(streamWriterWrite(&t, pattern, sizeof(pattern)), 0);
    }
    ASSERT_EQ(streamWriterFinish(&t), 0);
    ASSERT_TRUE(lz4FrameUsesLinkedBlocks(db.data + VCS_ENVELOPE_SIZE,
                                         sdslen((const char *)db.data) - VCS_ENVELOPE_SIZE))
        << "stream writer should use linked LZ4 blocks";

    sds comp = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buf_rio;
    rioInitWithBuffer(&buf_rio, comp);

    decompressRio dr;
    ASSERT_EQ(initVcsRdbDecompressRio(&dr, &buf_rio), 0);

    size_t total_len = sizeof(pattern) * 32;
    char *result = (char *)zmalloc(total_len);
    ASSERT_NE(rioRead((rio *)&dr, result, total_len), 0u) << "repetitive payload decompression should succeed";

    /* Verify all bytes match */
    for (size_t i = 0; i < total_len; i++) {
        ASSERT_EQ(result[i], 'X');
    }

    decompressRioFree(&dr);
    sdsfree(comp);
    zfree(result);
    streamWriterFree(&t);
    dynamicBufFree(&db);
}
