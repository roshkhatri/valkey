/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cstring>
#include <limits>
#include <string>

extern "C" {
#include "../../deps/lz4/lz4frame.h"
#include "compression.h"
#include "compression_stream.h"
#include "server.h"
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

typedef struct {
    int calls;
    int fail_on_call;
} FailingEmitter;

typedef struct {
    const uint8_t *data;
    size_t len;
    size_t pos;
    int calls;
    int overread_on_call;
} OverreadReader;

static streamReaderConfig makeReaderConfig(uint8_t expected_stream_kind,
                                           bool allow_passthrough,
                                           size_t buffer_size = STREAM_READER_BUFFER_SIZE_DEFAULT,
                                           bool skip_codec_checksum_validation = false) {
    streamReaderConfig cfg = {};
    cfg.expected_stream_kind = expected_stream_kind;
    cfg.allow_passthrough = allow_passthrough;
    cfg.skip_codec_checksum_validation = skip_codec_checksum_validation;
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

static ssize_t overreadReaderRead(void *ctx, void *buf, size_t len) {
    OverreadReader *r = (OverreadReader *)ctx;
    r->calls++;
    if (r->calls == r->overread_on_call) return (ssize_t)(len + 1);
    if (r->pos >= r->len) return 0;

    size_t avail = r->len - r->pos;
    size_t n = len < avail ? len : avail;
    memcpy(buf, r->data + r->pos, n);
    r->pos += n;
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

TEST_F(CompressionTest, streamCompressorInitFree) {
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0), 0) << "LZ4 init should succeed";
    ASSERT_EQ(sc.algo, ALGO_LZ4) << "algo should be LZ4";
    ASSERT_EQ(sc.stream_started, false) << "stream_started should be false";
    ASSERT_NE(sc.ctx, nullptr) << "ctx should be non-nullptr";
    streamCompressorFree(&sc);
    ASSERT_EQ(sc.ctx, nullptr) << "ctx should be nullptr after free";

    streamCompressor sc3;
    ASSERT_EQ(streamCompressorInit(&sc3, ALGO_NONE, 0), -1) << "NONE init should fail";
}

TEST_F(CompressionTest, streamDecompressorInitFree) {
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4), 0) << "LZ4 decomp init should succeed";
    ASSERT_EQ(sd.algo, ALGO_LZ4) << "algo should be LZ4";
    ASSERT_NE(sd.ctx, nullptr) << "ctx should be non-nullptr";
    streamDecompressorFree(&sd);
    ASSERT_EQ(sd.ctx, nullptr) << "ctx should be nullptr after free";

    streamDecompressor invalid;
    ASSERT_EQ(streamDecompressorInit(&invalid, ALGO_NONE), -1);
}

TEST_F(CompressionTest, streamCompressorDecompressorRoundTrip) {
    const char *input = "Hello, Valkey compression module! This is a test payload.";
    size_t input_len = strlen(input);

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

TEST_F(CompressionTest, streamCompressorOutputBound) {
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0), 0);

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

    size_t b_zero = streamCompressorOutputBound(&sc, 0);
    ASSERT_GT(b_zero, 0u) << "zero input bound should be > 0";

    zfree(seed_buf);
    streamCompressorFree(&sc);
}

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
    static const uint8_t invalid_vcs[VCS_ENVELOPE_SIZE] = {
        VCS_MAGIC_0, VCS_MAGIC_1, VCS_MAGIC_2, 0, VCS_CODEC_LZ4, 0, VCS_STREAM_RDB};
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
        streamReaderConfig cfg = makeReaderConfig(VCS_STREAM_RDB, cases[i].allow_passthrough);
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

TEST_F(CompressionTest, streamReaderRejectsEveryTruncatedVcsEnvelope) {
    const uint8_t envelope[VCS_ENVELOPE_SIZE] = {
        VCS_MAGIC_0,
        VCS_MAGIC_1,
        VCS_MAGIC_2,
        VCS_VERSION,
        VCS_CODEC_LZ4,
        0,
        VCS_STREAM_RDB,
    };

    for (size_t prefix_len = 1; prefix_len < VCS_ENVELOPE_SIZE; prefix_len++) {
        MemReader mr = {envelope, prefix_len, 0, 1};
        streamReaderConfig cfg = makeReaderConfig(VCS_STREAM_RDB, true);
        streamReader reader;
        ASSERT_EQ(streamReaderInit(&reader, &cfg, memReaderRead, &mr), 0);

        streamReaderInfo info;
        ASSERT_EQ(streamReaderGetInfo(&reader, &info), -1) << "accepted VCS prefix length " << prefix_len;
        ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_INCOMPATIBLE) << "VCS prefix length " << prefix_len;
        streamReaderFree(&reader);
    }

    MemReader empty = {};
    streamReaderConfig cfg = makeReaderConfig(VCS_STREAM_RDB, true);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &cfg, memReaderRead, &empty), 0);
    streamReaderInfo info;
    ASSERT_EQ(streamReaderGetInfo(&reader, &info), 0);
    ASSERT_FALSE(info.compressed);
    uint8_t out = 0;
    ASSERT_EQ(streamReaderRead(&reader, &out, 1), 0);
    streamReaderFree(&reader);
}

TEST_F(CompressionTest, streamReaderZeroLengthReadDoesNotProbe) {
    OverreadReader source = {};
    source.overread_on_call = 1;
    streamReaderConfig cfg = makeReaderConfig(VCS_STREAM_RDB, true, 1);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &cfg, overreadReaderRead, &source), 0);
    ASSERT_EQ(reader.buffer_size, (size_t)STREAM_READER_BUFFER_SIZE_MIN);

    uint8_t out = 0;
    ASSERT_EQ(streamReaderRead(&reader, &out, 0), 0);
    ASSERT_EQ(source.calls, 0) << "a zero-length read must not consume the source";

    streamReaderInfo info;
    ASSERT_EQ(streamReaderGetInfo(&reader, &info), -1);
    ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_IO);
    streamReaderFree(&reader);
}

TEST_F(CompressionTest, streamReaderRejectsOversizedReadRequest) {
    const uint8_t input[] = {'H', 'E', 'L', 'L', 'O'};
    MemReader mr = {};
    mr.data = input;
    mr.len = sizeof(input);
    mr.max_chunk = 2;
    streamReaderConfig cfg = makeReaderConfig(VCS_STREAM_RDB, true);

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

static int failSelectedEmit(void *ctx, const uint8_t *data, size_t len) {
    (void)data;
    (void)len;
    FailingEmitter *emitter = (FailingEmitter *)ctx;
    emitter->calls++;
    return emitter->calls == emitter->fail_on_call ? -1 : 0;
}

static int initVcsRdbStreamReader(streamReader *reader, rio *r) {
    return rdbInitStreamReader(r, reader, false, nullptr) == RDB_STREAM_READER_INIT_OK ? 0 : -1;
}

static void countRioUpdateCalls(rio *r, const void *buf, size_t len) {
    (void)buf;
    (void)len;
    r->cksum++;
}

static int emitToRioBackend(void *ctx, const uint8_t *data, size_t len) {
    return rioWriteRaw((rio *)ctx, data, len) ? 0 : -1;
}

static int attachCompressionWriter(rio *r, streamWriter *writer, bool codec_checksum) {
    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    cfg.codec_checksum_enabled = codec_checksum;
    if (streamWriterInit(writer, &cfg, emitToRioBackend, r) != 0) return -1;
    rioAttachStreamWriter(r, writer);
    return 0;
}

static int finishCompressionWriter(rio *r, streamWriter *writer) {
    if (streamWriterFinish(writer) != 0) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return -1;
    }
    if (rioFlushRaw(r)) return 0;
    r->flags |= RIO_FLAG_WRITE_ERROR;
    return -1;
}

static void freeCompressionWriter(rio *r, streamWriter *writer) {
    rioDetachStreamWriter(r);
    streamWriterFree(writer);
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
         VCS_STREAM_RDB,
         VCS_STREAM_RDB,
         3,
         STREAM_READER_BUFFER_SIZE_MIN,
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
         VCS_STREAM_RDB,
         0,
         STREAM_READER_BUFFER_SIZE_DEFAULT,
         false},
        {"RDB stream when custom expected",
         "stream-kind mismatch",
         VCS_STREAM_RDB,
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
        ASSERT_EQ(streamWriterWrite(&w, cases[i].payload, payload_len), 0) << cases[i].name;
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

TEST_F(CompressionTest, streamReaderClassifiesSourceCallbackFailuresAsIoErrors) {
    FlakyReader failed_source = {};
    failed_source.fail_after_success_reads = 0;
    streamReaderConfig failed_cfg = makeReaderConfig(VCS_STREAM_RDB, true);
    streamReader failed_reader;
    ASSERT_EQ(streamReaderInit(&failed_reader, &failed_cfg, flakyReaderRead, &failed_source), 0);
    streamReaderInfo info;
    ASSERT_EQ(streamReaderGetInfo(&failed_reader, &info), -1);
    ASSERT_EQ(failed_reader.error_kind, STREAM_READER_ERROR_IO);
    streamReaderFree(&failed_reader);

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&writer, "source callback contract", 24), 0);
    ASSERT_EQ(streamWriterFinish(&writer), 0);
    streamWriterFree(&writer);

    for (int overread_on_call : {1, 2, 3}) {
        OverreadReader source = {};
        source.data = db.data;
        source.len = sdslen((const char *)db.data);
        source.overread_on_call = overread_on_call;
        streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false);
        streamReader reader;
        ASSERT_EQ(streamReaderInit(&reader, &rcfg, overreadReaderRead, &source), 0);

        uint8_t out[24];
        ASSERT_EQ(streamReaderRead(&reader, out, sizeof(out)), -1) << "callback call " << overread_on_call;
        ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_IO) << "callback call " << overread_on_call;
        ASSERT_EQ(streamReaderRead(&reader, out, sizeof(out)), -1) << "I/O error must remain sticky";
        streamReaderFree(&reader);
    }

    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamReadEnvelopeInfoValidatesEveryField) {
    const uint8_t good[VCS_ENVELOPE_SIZE] = {
        VCS_MAGIC_0,
        VCS_MAGIC_1,
        VCS_MAGIC_2,
        VCS_VERSION,
        VCS_CODEC_LZ4,
        0,
        VCS_STREAM_RDB,
    };
    streamReaderInfo info;
    ASSERT_EQ(streamReadEnvelopeInfo(good, sizeof(good), VCS_STREAM_RDB, &info), 0);
    ASSERT_TRUE(info.compressed);
    ASSERT_EQ(info.algo, ALGO_LZ4);
    ASSERT_EQ(info.stream_kind, VCS_STREAM_RDB);

    for (size_t len = 0; len < VCS_ENVELOPE_SIZE; len++) {
        ASSERT_EQ(streamReadEnvelopeInfo(good, len, VCS_STREAM_RDB, &info), -1) << "length " << len;
    }

    struct {
        const char *name;
        size_t offset;
        uint8_t value;
    } cases[] = {
        {"magic", 0, 'X'},
        {"version", VCS_OFFSET_VERSION, VCS_VERSION + 1},
        {"unknown codec", VCS_OFFSET_CODEC, 0x7f},
        {"reserved byte", VCS_OFFSET_RESERVED, 1},
        {"stream kind", VCS_OFFSET_STREAM_KIND, 0x7f},
    };

    for (const auto &test : cases) {
        uint8_t mutated[VCS_ENVELOPE_SIZE];
        memcpy(mutated, good, sizeof(mutated));
        mutated[test.offset] = test.value;
        ASSERT_EQ(streamReadEnvelopeInfo(mutated, sizeof(mutated), VCS_STREAM_RDB, &info), -1) << test.name;
    }
}

TEST_F(CompressionTest, vcsCodecIdsAreMappedExplicitly) {
    vcsCodecId codec = (vcsCodecId)0;
    compressionAlgo algo = ALGO_NONE;

    ASSERT_TRUE(compressionAlgoToVcsCodec(ALGO_LZ4, &codec));
    ASSERT_EQ(codec, VCS_CODEC_LZ4);
    ASSERT_FALSE(compressionAlgoToVcsCodec(ALGO_LZF, &codec));

    ASSERT_TRUE(vcsCodecToCompressionAlgo(VCS_CODEC_LZ4, &algo));
    ASSERT_EQ(algo, ALGO_LZ4);
    ASSERT_FALSE(vcsCodecToCompressionAlgo(0x7f, &algo));
}

/* Regression for partial output followed by a source read error. The partial
 * bytes are returned, but the error must remain sticky for the next read. */
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
    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    const size_t first_chunk_len = 4 * 1024;
    ASSERT_EQ(streamWriterWrite(&w, payload, first_chunk_len), 0);
    ASSERT_EQ(streamWriterFlush(&w), 0);
    size_t fail_after_pos = sdslen((const char *)db.data);
    ASSERT_EQ(streamWriterWrite(&w, payload + first_chunk_len, payload_len - first_chunk_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    FlakyReader fr = {};
    fr.data = db.data;
    fr.len = sdslen((const char *)db.data);
    fr.max_chunk = 4096;
    fr.fail_after_pos = fail_after_pos;
    fr.fail_after_success_reads = (std::numeric_limits<int>::max)();
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false, STREAM_READER_BUFFER_SIZE_MIN);
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
    ASSERT_EQ(r.error_kind, STREAM_READER_ERROR_IO);

    streamReaderFree(&r);

    /* Passthrough mode should also preserve partial bytes when source read
     * fails after probe/prefix buffering, then latch sticky error state. */
    const uint8_t plain[] = "NOTVCS-passthrough-regression";
    FlakyReader fr_passthrough = {};
    fr_passthrough.data = plain;
    fr_passthrough.len = sizeof(plain) - 1;
    fr_passthrough.max_chunk = 0;
    fr_passthrough.fail_after_success_reads = 1; /* probe succeeds, next read fails */
    streamReaderConfig pass_cfg = makeReaderConfig(VCS_STREAM_RDB, true);
    streamReader rp;
    ASSERT_EQ(streamReaderInit(&rp, &pass_cfg, flakyReaderRead, &fr_passthrough), 0);

    uint8_t pass_out[64];
    ssize_t p1 = streamReaderRead(&rp, pass_out, sizeof(pass_out));
    ASSERT_GT(p1, 0) << "passthrough first read should return partial output";
    ASSERT_EQ(memcmp(pass_out, plain, (size_t)p1), 0) << "passthrough partial bytes should match input prefix";
    ASSERT_EQ(streamReaderRead(&rp, pass_out, sizeof(pass_out)), -1)
        << "passthrough second read should fail immediately";
    ASSERT_EQ(rp.error_kind, STREAM_READER_ERROR_IO);
    streamReaderFree(&rp);
    zfree(out);

    dynamicBufFree(&db);
    zfree(payload);
}

TEST_F(CompressionTest, streamWriterInitFree) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(t.errored, false) << "should not be errored";

    streamWriterFree(&t);
    dynamicBufFree(&db);

    streamWriterConfig bad_cfg = makeWriterConfig(ALGO_NONE, 0, VCS_STREAM_RDB);
    streamWriter bad_writer;
    ASSERT_EQ(streamWriterInit(&bad_writer, &bad_cfg, emitToDynamicBuf, &db), -1) << "ALGO_NONE should fail init";
    bad_cfg = makeWriterConfig(ALGO_LZF, 0, VCS_STREAM_RDB);
    ASSERT_EQ(streamWriterInit(&bad_writer, &bad_cfg, emitToDynamicBuf, &db), -1) << "ALGO_LZF should fail init";

    /* Concrete stream kinds outside the currently named ones are valid. */
    streamWriterConfig future_kind_cfg = makeWriterConfig(ALGO_LZ4, 0, 0x7f);
    streamWriter future_t;
    ASSERT_EQ(streamWriterInit(&future_t, &future_kind_cfg, emitToDynamicBuf, &db), 0);
    streamWriterFree(&future_t);
}

TEST_F(CompressionTest, streamWriterFinishProducesAValidEmptyStream) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterWrite(&writer, nullptr, 0), 0);
    ASSERT_EQ(streamWriterFlush(&writer), 0);
    ASSERT_EQ(sdslen((const char *)db.data), 0u) << "empty writes and flushes must stay lazy";
    ASSERT_EQ(streamWriterFinish(&writer), 0);
    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE);
    streamWriterFree(&writer);

    MemReader source = {db.data, sdslen((const char *)db.data), 0, 1};
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &source), 0);
    uint8_t out = 0;
    ASSERT_EQ(streamReaderRead(&reader, &out, 1), 0);
    ASSERT_EQ(streamReaderValidateEnd(&reader), 0);
    streamReaderFree(&reader);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamWriterSinkFailuresAreSticky) {
    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    for (int fail_on_call : {1, 2}) {
        FailingEmitter emitter = {0, fail_on_call};
        streamWriter writer;
        ASSERT_EQ(streamWriterInit(&writer, &cfg, failSelectedEmit, &emitter), 0);

        ASSERT_EQ(streamWriterWrite(&writer, "payload", 7), -1) << "sink call " << fail_on_call;
        ASSERT_TRUE(writer.errored);
        ASSERT_EQ(streamWriterWrite(&writer, "retry", 5), -1);
        ASSERT_EQ(streamWriterFlush(&writer), -1);
        ASSERT_EQ(streamWriterFinish(&writer), -1);
        ASSERT_EQ(emitter.calls, fail_on_call) << "an errored writer must not emit more bytes";
        streamWriterFree(&writer);
    }

    FailingEmitter emitter = {0, 0};
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, &cfg, failSelectedEmit, &emitter), 0);
    ASSERT_EQ(streamWriterWrite(&writer, "payload", 7), 0);
    emitter.fail_on_call = emitter.calls + 1;
    ASSERT_EQ(streamWriterFinish(&writer), -1);
    int calls_after_failure = emitter.calls;
    ASSERT_EQ(streamWriterFinish(&writer), -1);
    ASSERT_EQ(streamWriterWrite(&writer, "retry", 5), -1);
    ASSERT_EQ(emitter.calls, calls_after_failure) << "a failed finish must remain failed";
    streamWriterFree(&writer);
}

TEST_F(CompressionTest, streamWriterRoundTrip) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    const char *test_data = "Hello, compression world! This is a test of the stream writer API.";
    size_t data_len = strlen(test_data);
    ASSERT_EQ(streamWriterWrite(&t, test_data, data_len), 0);
    ASSERT_EQ(t.errored, false) << "should not be errored after write";
    ASSERT_GE(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE) << "write should emit envelope";

    ASSERT_EQ(streamWriterFinish(&t), 0);
    ASSERT_EQ(t.errored, false) << "should not be errored after finish";

    ASSERT_GE(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE)
        << "output should have at least envelope size";
    ASSERT_EQ(db.data[0], VCS_MAGIC_0) << "magic byte 0";
    ASSERT_EQ(db.data[1], VCS_MAGIC_1) << "magic byte 1";
    ASSERT_EQ(db.data[2], VCS_MAGIC_2) << "magic byte 2";
    ASSERT_EQ(db.data[3], VCS_VERSION) << "version";
    ASSERT_EQ(db.data[4], VCS_CODEC_LZ4) << "codec id";
    ASSERT_EQ(db.data[5], (uint8_t)0) << "reserved byte";
    ASSERT_EQ(db.data[6], VCS_STREAM_RDB) << "stream_kind RDB";

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

/* A single write larger than STREAM_WRITER_INPUT_CHUNK_SIZE exercises the
 * writer's bounded scratch-buffer path. */
TEST_F(CompressionTest, streamWriterLargeSingleWrite) {
    const size_t payload_len = (1024 * 1024) + 4096;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 17 + 11) % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&t, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data);
    mr.max_chunk = 0;
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false, STREAM_READER_BUFFER_SIZE_MIN);
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

/* Small caller reads exercise the buffered decompressed window. */
TEST_F(CompressionTest, streamReaderSmallReadsRoundTrip) {
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    ASSERT_NE(payload, nullptr);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 29 + 7) % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, -5, VCS_STREAM_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data);
    mr.max_chunk = 4096;
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false, STREAM_READER_BUFFER_SIZE_MIN);
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

TEST_F(CompressionTest, streamWriterFlushBehavior) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterFlush(&t), 0) << "flush before write should be no-op success";
    ASSERT_EQ(sdslen((const char *)db.data), 0u) << "flush before write should not emit bytes";

    ASSERT_EQ(streamWriterWrite(&t, "first chunk", 11), 0);
    ASSERT_EQ(streamWriterFlush(&t), 0);
    ASSERT_EQ(streamWriterWrite(&t, "second chunk", 12), 0);
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

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterWrite(&t, "payload", 7), 0);
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

        streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB,
                                                  codec_checksum);
        streamWriter t;
        ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);
        ASSERT_EQ(streamWriterWrite(&t, payload, strlen(payload)), 0);
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

TEST_F(CompressionTest, checksumBypassSkipsOnlyCodecVerification) {
    const char *payload = "checksum bypass payload";
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB, true);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&writer, payload, strlen(payload)), 0);
    ASSERT_EQ(streamWriterFinish(&writer), 0);
    streamWriterFree(&writer);

    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE + 4);
    db.data[sdslen((const char *)db.data) - 1] ^= 1;

    for (bool skip_codec_checksum_validation : {false, true}) {
        MemReader source = {db.data, sdslen((const char *)db.data), 0, 0};
        streamReaderConfig rcfg = makeReaderConfig(
            VCS_STREAM_RDB, false, STREAM_READER_BUFFER_SIZE_DEFAULT, skip_codec_checksum_validation);
        streamReader reader;
        ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &source), 0);

        char out[64] = {0};
        ASSERT_EQ(streamReaderRead(&reader, out, strlen(payload)), (ssize_t)strlen(payload));
        ASSERT_EQ(memcmp(out, payload, strlen(payload)), 0);
        ASSERT_EQ(streamReaderValidateEnd(&reader), skip_codec_checksum_validation ? 0 : -1);
        streamReaderFree(&reader);
    }

    MemReader truncated = {db.data, sdslen((const char *)db.data) - 5, 0, 0};
    streamReaderConfig bypass = makeReaderConfig(
        VCS_STREAM_RDB, false, STREAM_READER_BUFFER_SIZE_DEFAULT, true);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &bypass, memReaderRead, &truncated), 0);
    char out[64] = {0};
    ASSERT_EQ(streamReaderRead(&reader, out, strlen(payload)), (ssize_t)strlen(payload));
    ASSERT_EQ(streamReaderValidateEnd(&reader), -1)
        << "checksum bypass must not bypass exact frame-end validation";
    streamReaderFree(&reader);

    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamReaderValidateEndAcceptsClosedFrame) {
    const char *payload = "validate frame end payload";
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB, true);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, strlen(payload)), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);

    MemReader reader_ctx = {db.data, sdslen((const char *)db.data), 0, 7};
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false);
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

TEST_F(CompressionTest, streamReaderValidateEndRejectsSourceOverreadAsIoError) {
    const char payload[] = "validate source callback";
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&writer, payload, sizeof(payload) - 1), 0);
    ASSERT_EQ(streamWriterFinish(&writer), 0);
    streamWriterFree(&writer);

    OverreadReader source = {};
    source.data = db.data;
    source.len = sdslen((const char *)db.data);
    source.overread_on_call = (std::numeric_limits<int>::max)();
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &rcfg, overreadReaderRead, &source), 0);
    uint8_t out[sizeof(payload) - 1];
    ASSERT_EQ(streamReaderRead(&reader, out, sizeof(out)), (ssize_t)sizeof(out));
    ASSERT_EQ(memcmp(out, payload, sizeof(out)), 0);

    source.overread_on_call = source.calls + 1;
    ASSERT_EQ(streamReaderValidateEnd(&reader), -1);
    ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_IO);

    streamReaderFree(&reader);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamReaderValidateEndRejectsTrailingBytes) {
    const char *payload = "payload with trailing compressed bytes";
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB, true);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, strlen(payload)), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    db.data = (uint8_t *)sdscatlen((sds)db.data, "x", 1);

    MemReader reader_ctx = {db.data, sdslen((const char *)db.data), 0, 0};
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false);
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

    streamWriterConfig wcfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB, true);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &wcfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);

    MemReader reader_ctx = {db.data, sdslen((const char *)db.data), 0, 0};
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false);
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

TEST_F(CompressionTest, rioCompressionWriterRoundTrip) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&buffer_rio, &writer, false), 0);
    const char *test_data = "The quick brown fox jumps over the lazy dog. "
                            "Pack my box with five dozen liquor jugs.";
    size_t data_len = strlen(test_data);
    ASSERT_NE(rioWrite(&buffer_rio, test_data, data_len), 0u) << "rioWrite should succeed";

    ASSERT_EQ(finishCompressionWriter(&buffer_rio, &writer), 0);
    ASSERT_EQ(buffer_rio.processed_bytes, data_len);
    ASSERT_EQ((size_t)rioTell(&buffer_rio), data_len);

    sds compressed = buffer_rio.io.buffer.ptr;
    size_t compressed_len = sdslen(compressed);
    ASSERT_EQ(buffer_rio.backend_processed_bytes, compressed_len);
    ASSERT_GT(compressed_len, (size_t)VCS_ENVELOPE_SIZE) << "compressed output should exist";

    ASSERT_EQ(compressed[0], (char)VCS_MAGIC_0) << "magic V";
    ASSERT_EQ(compressed[1], (char)VCS_MAGIC_1) << "magic C";
    ASSERT_EQ(compressed[2], (char)VCS_MAGIC_2) << "magic S";
    ASSERT_EQ(compressed[3], (char)VCS_VERSION) << "version";

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
    freeCompressionWriter(&buffer_rio, &writer);
    sdsfree(compressed);
}

TEST_F(CompressionTest, rioCompressionWriterDoesNotOwnRdbChecksumPolicy) {
    for (bool inner_skips_checksum : {false, true}) {
        for (bool codec_checksum : {false, true}) {
            sds buf = sdsempty();
            rio inner;
            rioInitWithBuffer(&inner, buf);
            if (inner_skips_checksum) inner.flags |= RIO_FLAG_SKIP_RDB_CHECKSUM;

            streamWriter writer;
            ASSERT_EQ(attachCompressionWriter(&inner, &writer, codec_checksum), 0);
            ASSERT_EQ(inner.update_cksum, nullptr);
            ASSERT_EQ((inner.flags & RIO_FLAG_SKIP_RDB_CHECKSUM) != 0, inner_skips_checksum);
            ASSERT_NE(rioWrite(&inner, "checksum policy", 15), 0u);
            ASSERT_EQ(inner.cksum, 0u);
            ASSERT_EQ(finishCompressionWriter(&inner, &writer), 0);

            freeCompressionWriter(&inner, &writer);
            sdsfree(inner.io.buffer.ptr);
        }
    }
}

TEST_F(CompressionTest, rioCompressionWriterFlushFailureSetsWriteError) {
    rio inner;
    initFailingFlushRio(&inner);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&inner, &writer, false), 0);

    const char *payload = "flush failure should latch rio write error";
    ASSERT_NE(rioWrite(&inner, payload, strlen(payload)), 0u);
    ASSERT_EQ(rioFlush(&inner), 0);
    ASSERT_TRUE(inner.flags & RIO_FLAG_WRITE_ERROR);

    freeCompressionWriter(&inner, &writer);
}

TEST_F(CompressionTest, rioCompressionWriterFinishFailureSetsWriteError) {
    rio inner;
    initFailingFlushRio(&inner);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&inner, &writer, false), 0);

    const char *payload = "finish failure should latch rio write error";
    ASSERT_NE(rioWrite(&inner, payload, strlen(payload)), 0u);
    ASSERT_EQ(finishCompressionWriter(&inner, &writer), -1);
    ASSERT_TRUE(inner.flags & RIO_FLAG_WRITE_ERROR);

    freeCompressionWriter(&inner, &writer);
}

TEST_F(CompressionTest, rioStreamReaderRoundTrip) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    const char *test_data = "Decompression rio test data. "
                            "This should round-trip through compress then decompress.";
    size_t data_len = strlen(test_data);
    ASSERT_EQ(streamWriterWrite(&t, test_data, data_len), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    sds comp_sds = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    streamReader reader;
    ASSERT_EQ(initVcsRdbStreamReader(&reader, &buffer_rio), 0);

    char result[256];
    memset(result, 0, sizeof(result));
    ASSERT_NE(rioRead(&buffer_rio, result, data_len), 0u) << "rioRead should succeed";
    ASSERT_EQ(memcmp(result, test_data, data_len), 0) << "decompressed data should match original";

    rdbFreeStreamReader(&buffer_rio, &reader);
    sdsfree(comp_sds);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, rioStreamReaderTellTracksSourceProgress) {
    DynamicBuf db;
    dynamicBufInit(&db);

    std::string payload(4096, 'A');
    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&t, payload.data(), payload.size()), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    sds comp_sds = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    streamReader reader;
    ASSERT_EQ(initVcsRdbStreamReader(&reader, &buffer_rio), 0);

    char out[2048];
    ASSERT_NE(rioRead(&buffer_rio, out, sizeof(out)), 0u);
    ASSERT_EQ((size_t)rioTell(&buffer_rio), buffer_rio.backend_processed_bytes);
    ASSERT_LT((size_t)rioTell(&buffer_rio), sizeof(out))
        << "rio tell should track source bytes, not logical output bytes";

    rdbFreeStreamReader(&buffer_rio, &reader);
    sdsfree(comp_sds);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, rioStreamReaderHonorsMaxProcessingChunk) {
    const size_t payload_len = 1024;
    const size_t chunk_size = 128;
    uint8_t payload[payload_len];
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&writer, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&writer), 0);
    streamWriterFree(&writer);

    sds compressed = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, compressed);
    streamReader reader;
    ASSERT_EQ(initVcsRdbStreamReader(&reader, &buffer_rio), 0);

    buffer_rio.max_processing_chunk = chunk_size;
    buffer_rio.update_cksum = countRioUpdateCalls;
    uint8_t result[payload_len];
    ASSERT_NE(rioRead(&buffer_rio, result, payload_len), 0u);
    ASSERT_EQ(buffer_rio.cksum, payload_len / chunk_size);
    ASSERT_EQ(buffer_rio.processed_bytes, payload_len);
    ASSERT_EQ(memcmp(result, payload, payload_len), 0);

    rdbFreeStreamReader(&buffer_rio, &reader);
    sdsfree(compressed);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, rioStreamReaderClassifiesInput) {
    {
        const char *payload = "REDIS001remaining data after prefix";
        size_t payload_len = strlen(payload);
        sds buf = sdsnewlen(payload, payload_len);
        rio buffer_rio;
        rioInitWithBuffer(&buffer_rio, buf);

        streamReader reader;
        compressionAlgo algo = ALGO_NONE;
        ASSERT_EQ(rdbInitStreamReader(&buffer_rio, &reader, false, &algo), RDB_STREAM_READER_INIT_OK);
        ASSERT_EQ(algo, ALGO_NONE) << "passthrough stream should not be compressed";

        char result[64];
        memset(result, 0, sizeof(result));
        ASSERT_NE(rioRead(&buffer_rio, result, payload_len), 0u) << "rioRead should succeed";
        ASSERT_EQ(memcmp(result, payload, payload_len), 0) << "payload should be replayed exactly";

        rdbFreeStreamReader(&buffer_rio, &reader);
        sdsfree(buf);
    }

    {
        const uint8_t malformed[VCS_ENVELOPE_SIZE] = {
            VCS_MAGIC_0, VCS_MAGIC_1, VCS_MAGIC_2, 0, VCS_CODEC_LZ4, 0, VCS_STREAM_RDB};
        sds buf = sdsnewlen(malformed, sizeof(malformed));
        rio buffer_rio;
        rioInitWithBuffer(&buffer_rio, buf);

        streamReader reader;
        ASSERT_EQ(rdbInitStreamReader(&buffer_rio, &reader, false, nullptr),
                  RDB_STREAM_READER_INIT_INCOMPATIBLE);

        sdsfree(buf);
    }
}

TEST_F(CompressionTest, rioCompressionWriterFinishIdempotent) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&buffer_rio, &writer, false), 0);

    ASSERT_NE(rioWrite(&buffer_rio, "test", 4), 0u);
    ASSERT_EQ(finishCompressionWriter(&buffer_rio, &writer), 0);
    size_t len_after_first = sdslen(buffer_rio.io.buffer.ptr);

    ASSERT_EQ(finishCompressionWriter(&buffer_rio, &writer), 0);
    size_t len_after_second = sdslen(buffer_rio.io.buffer.ptr);
    ASSERT_EQ(len_after_first, len_after_second) << "second finish should not produce more output";

    freeCompressionWriter(&buffer_rio, &writer);
    sdsfree(buffer_rio.io.buffer.ptr);
}

TEST_F(CompressionTest, rioCompressionWriterFlushMidStream) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&buffer_rio, &writer, false), 0);

    ASSERT_NE(rioWrite(&buffer_rio, "first chunk", 11), 0u);

    ASSERT_NE(rioFlush(&buffer_rio), 0) << "flush should succeed";

    ASSERT_NE(rioWrite(&buffer_rio, "second chunk", 12), 0u) << "write after flush should succeed";

    ASSERT_EQ(finishCompressionWriter(&buffer_rio, &writer), 0);

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
    freeCompressionWriter(&buffer_rio, &writer);
    sdsfree(compressed);
}

/* The progress callback must leave a write-side stream rio alone. */
TEST_F(CompressionTest, rdbLoadProgressCallbackStreamingGuard) {
    sds buf = sdsempty();
    rio inner;
    rioInitWithBuffer(&inner, buf);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&inner, &writer, false), 0);

    const char sample[] = "progress-guard";
    rdbLoadProgressCallback(&inner, sample, sizeof(sample) - 1);

    ASSERT_FALSE(inner.flags & RIO_FLAG_READ_ERROR) << "write-side streaming rio must not set read error";

    freeCompressionWriter(&inner, &writer);
    sdsfree(inner.io.buffer.ptr);
}

/* A large read used to drop compressed bytes that were not consumed in one
 * decompression iteration, causing false EOF or data corruption. */
TEST_F(CompressionTest, rioStreamReaderLargePayload) {
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterWrite(&t, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    sds comp_sds = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, comp_sds);

    streamReader reader;
    ASSERT_EQ(initVcsRdbStreamReader(&reader, &buffer_rio), 0);

    uint8_t *result = (uint8_t *)zmalloc(payload_len);
    size_t total_read = 0;
    while (total_read < payload_len) {
        size_t chunk = 4096;
        if (chunk > payload_len - total_read) chunk = payload_len - total_read;
        size_t ret = rioRead(&buffer_rio, result + total_read, chunk);
        ASSERT_NE(ret, 0u) << "rioRead should succeed for large payload";
        total_read += chunk;
    }

    ASSERT_EQ(memcmp(result, payload, payload_len), 0) << "decompressed data should match original";

    rdbFreeStreamReader(&buffer_rio, &reader);
    sdsfree(comp_sds);
    zfree(result);
    zfree(payload);
    dynamicBufFree(&db);
}

/* The reader stops at the frame boundary so callers can manage subsequent
 * bytes on a long-lived stream. */
TEST_F(CompressionTest, streamReaderStopsAtFrameEndBeforeTrailingBytes) {
    const char *payload = "stream-reader-frame-end";
    const size_t payload_len = strlen(payload);
    const char *trailer = "TRAILER-BYTES-AFTER-FRAME";
    const size_t trailer_len = strlen(trailer);

    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);
    size_t frame_len = sdslen((const char *)db.data);

    sds input = sdsnewlen(db.data, sdslen((const char *)db.data));
    input = sdscatlen(input, trailer, trailer_len);

    MemReader mr = {};
    mr.data = (const uint8_t *)input;
    mr.len = sdslen(input);
    mr.max_chunk = 3;
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false, STREAM_READER_BUFFER_SIZE_MIN);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr), 0);

    char out[128];
    memset(out, 0, sizeof(out));
    ASSERT_EQ(streamReaderRead(&r, out, payload_len), (ssize_t)payload_len);
    ASSERT_EQ(memcmp(out, payload, payload_len), 0);

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

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, &cfg, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE + 1);
    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data) - 1;
    mr.max_chunk = 7;
    streamReaderConfig rcfg = makeReaderConfig(VCS_STREAM_RDB, false, STREAM_READER_BUFFER_SIZE_MIN);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr), 0);

    uint8_t out[payload_len];
    ASSERT_EQ(streamReaderRead(&r, out, payload_len), (ssize_t)payload_len);
    ASSERT_EQ(memcmp(out, payload, payload_len), 0);
    ASSERT_LT(streamReaderRead(&r, out, 1), 0) << "EOF before frame end should be treated as corruption";
    ASSERT_EQ(r.error_kind, STREAM_READER_ERROR_CORRUPT)
        << "truncated compressed frame should latch corruption, not I/O";

    streamReaderFree(&r);
    dynamicBufFree(&db);
}

TEST_F(CompressionTest, streamWriterWriteAfterFinish) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterWrite(&t, "hello", 5), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    size_t len_after_finish = sdslen((const char *)db.data);

    ASSERT_LT(streamWriterWrite(&t, "world", 5), 0);
    ASSERT_EQ(sdslen((const char *)db.data), len_after_finish) << "write after finish should not produce output";

    ASSERT_EQ(streamWriterFinish(&t), 0);
    ASSERT_EQ(sdslen((const char *)db.data), len_after_finish) << "second finish should not produce output";

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

TEST_F(CompressionTest, streamWriterRepetitivePayloadRoundTrip) {
    DynamicBuf db;
    dynamicBufInit(&db);

    streamWriterConfig cfg = makeWriterConfig(ALGO_LZ4, 0, VCS_STREAM_RDB);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, &cfg, emitToDynamicBuf, &db), 0);

    char pattern[4096];
    memset(pattern, 'X', sizeof(pattern));
    for (int i = 0; i < 32; i++) {
        ASSERT_EQ(streamWriterWrite(&t, pattern, sizeof(pattern)), 0);
    }
    ASSERT_EQ(streamWriterFinish(&t), 0);
    ASSERT_TRUE(lz4FrameUsesLinkedBlocks(db.data + VCS_ENVELOPE_SIZE,
                                         sdslen((const char *)db.data) - VCS_ENVELOPE_SIZE))
        << "stream writer should use linked LZ4 blocks";

    sds comp = sdsnewlen(db.data, sdslen((const char *)db.data));
    rio buf_rio;
    rioInitWithBuffer(&buf_rio, comp);

    streamReader reader;
    ASSERT_EQ(initVcsRdbStreamReader(&reader, &buf_rio), 0);

    size_t total_len = sizeof(pattern) * 32;
    char *result = (char *)zmalloc(total_len);
    ASSERT_NE(rioRead(&buf_rio, result, total_len), 0u) << "repetitive payload decompression should succeed";

    for (size_t i = 0; i < total_len; i++) {
        ASSERT_EQ(result[i], 'X');
    }

    rdbFreeStreamReader(&buf_rio, &reader);
    sdsfree(comp);
    zfree(result);
    streamWriterFree(&t);
    dynamicBufFree(&db);
}

TEST(AOFTest, syncRdbReuseRequiresSuccessfullyLoadedPlainFormat) {
    EXPECT_TRUE(aofCanReuseRdbAsBase(RDB_LOAD_FORMAT_PLAIN));
    EXPECT_FALSE(aofCanReuseRdbAsBase(RDB_LOAD_FORMAT_VCS));
    EXPECT_FALSE(aofCanReuseRdbAsBase(RDB_LOAD_FORMAT_UNKNOWN));
}
