/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <limits.h>
#include <string.h>

extern "C" {
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

static streamReaderConfig makeReaderConfig(bool allow_passthrough,
                                           size_t buffer_size,
                                           bool skip_codec_checksum_validation) {
    streamReaderConfig cfg = {};
    cfg.allow_passthrough = allow_passthrough;
    cfg.skip_codec_checksum_validation = skip_codec_checksum_validation;
    cfg.buffer_size = buffer_size;
    return cfg;
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

TEST(CompressionTest, streamCompressorInitFree) {
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0, false), 0);
    EXPECT_EQ(sc.stream_started, false) << "stream_started should be false";
    EXPECT_TRUE(sc.ctx != NULL) << "ctx should be non-NULL";
    streamCompressorFree(&sc);
    EXPECT_TRUE(sc.ctx == NULL) << "ctx should be NULL after free";
}

TEST(CompressionTest, streamDecompressorInitFree) {
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4, false), 0);
    EXPECT_TRUE(sd.ctx != NULL) << "ctx should be non-NULL";
    streamDecompressorFree(&sd);
    EXPECT_TRUE(sd.ctx == NULL) << "ctx should be NULL after free";
}

TEST(CompressionTest, streamCompressorDecompressorRoundTrip) {
    const char *input = "Hello, Valkey compression module! This is a test payload.";
    size_t input_len = strlen(input);

    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0, false), 0);

    size_t bound = streamCompressorOutputBound(&sc, input_len);
    ASSERT_GT(bound, 0u) << "bound should be > 0";

    uint8_t *compressed = (uint8_t *)zmalloc(bound);
    ASSERT_TRUE(compressed != NULL);
    ssize_t compressed_len = streamCompressorFeed(&sc, compressed, bound,
                                                  (const uint8_t *)input, input_len,
                                                  COMPRESS_FLUSH_END);
    ASSERT_GT(compressed_len, 0) << "compress should succeed";
    EXPECT_EQ(sc.stream_started, false) << "frame should be closed after end flush";
    streamCompressorFree(&sc);

    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4, false), 0);

    uint8_t decompressed[256];
    size_t input_consumed = 0;
    ssize_t decompressed_len = streamDecompressorFeed(&sd, decompressed, sizeof(decompressed),
                                                      compressed, (size_t)compressed_len,
                                                      &input_consumed);
    ASSERT_GT(decompressed_len, 0) << "decompress should succeed";
    EXPECT_EQ((size_t)decompressed_len, input_len) << "decompressed length should match input";
    EXPECT_EQ(memcmp(decompressed, input, input_len), 0) << "decompressed content should match input";
    EXPECT_EQ(input_consumed, (size_t)compressed_len) << "all compressed input should be consumed";

    streamDecompressorFree(&sd);
    zfree(compressed);
}

TEST(CompressionTest, streamCompressorOutputBound) {
    streamCompressor sc;
    ASSERT_EQ(streamCompressorInit(&sc, ALGO_LZ4, 0, false), 0);

    size_t b1 = streamCompressorOutputBound(&sc, 1024);
    ASSERT_GT(b1, 0u) << "bound for 1KB should be > 0";

    /* Bound is always conservative (includes frame header + flush overhead),
     * so it should be stable regardless of frame state. */
    size_t b_before = streamCompressorOutputBound(&sc, 1024);
    uint8_t *seed_buf = (uint8_t *)zmalloc(b_before);
    ASSERT_TRUE(seed_buf != NULL);
    ASSERT_GE(streamCompressorFeed(&sc, seed_buf, b_before,
                                   (const uint8_t *)"x", 1, COMPRESS_FLUSH_CONTINUE),
              0)
        << "seed write should start the frame";
    size_t b_after = streamCompressorOutputBound(&sc, 1024);
    EXPECT_EQ(b_before, b_after) << "bound should be the same before and after frame start";

    size_t b_zero = streamCompressorOutputBound(&sc, 0);
    EXPECT_GT(b_zero, 0u) << "zero input bound should be > 0";

    zfree(seed_buf);
    streamCompressorFree(&sc);
}

TEST(CompressionTest, streamReaderClassifiesProbeInputs) {
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
        streamReaderErrorKind expected_error;
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
        streamReaderConfig cfg = makeReaderConfig(cases[i].allow_passthrough,
                                                  STREAM_READER_BUFFER_SIZE_DEFAULT,
                                                  false);
        streamReader t;
        compressionAlgo algo = ALGO_NONE;
        if (cases[i].expect_probe_ok) {
            ASSERT_EQ(streamReaderInit(&t, &cfg, memReaderRead, &mr, &algo), 0) << cases[i].name;
            ASSERT_EQ(algo, ALGO_NONE) << cases[i].name;

            uint8_t out[16] = {0};
            ASSERT_EQ(streamReaderRead(&t, out, cases[i].expected_read_len), (ssize_t)cases[i].expected_read_len)
                << cases[i].name;
            EXPECT_EQ(memcmp(out, cases[i].input, cases[i].expected_read_len), 0) << cases[i].name;
            ASSERT_EQ(streamReaderRead(&t, out, sizeof(out)), 0) << cases[i].name;
        } else {
            ASSERT_EQ(streamReaderInit(&t, &cfg, memReaderRead, &mr, &algo), -1) << cases[i].name;
            ASSERT_EQ(t.error_kind, cases[i].expected_error) << cases[i].name;

            uint8_t out[8] = {0};
            ASSERT_EQ(streamReaderRead(&t, out, sizeof(out)), -1) << cases[i].name;
        }

        streamReaderFree(&t);
    }
}

TEST(CompressionTest, streamReaderRejectsEveryTruncatedVcsEnvelope) {
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
        streamReaderConfig cfg = makeReaderConfig(true, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
        streamReader reader;
        compressionAlgo algo = ALGO_NONE;
        ASSERT_EQ(streamReaderInit(&reader, &cfg, memReaderRead, &mr, &algo), -1)
            << "accepted VCS prefix length " << prefix_len;
        ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_INCOMPATIBLE)
            << "VCS prefix length " << prefix_len;
        streamReaderFree(&reader);
    }

    MemReader empty = {};
    streamReaderConfig cfg = makeReaderConfig(true, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
    streamReader reader;
    compressionAlgo algo = ALGO_LZ4;
    ASSERT_EQ(streamReaderInit(&reader, &cfg, memReaderRead, &empty, &algo), 0);
    ASSERT_EQ(algo, ALGO_NONE);
    uint8_t out = 0;
    ASSERT_EQ(streamReaderRead(&reader, &out, 1), 0);
    streamReaderFree(&reader);
}

TEST(CompressionTest, streamReaderInitProbesSource) {
    OverreadReader source = {};
    source.overread_on_call = 1;
    streamReaderConfig cfg = makeReaderConfig(true, 1, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &cfg, overreadReaderRead, &source, NULL), -1);
    ASSERT_EQ(reader.buffer_size, (size_t)STREAM_READER_BUFFER_SIZE_MIN);
    ASSERT_EQ(source.calls, 1);
    ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_IO);
    streamReaderFree(&reader);
}

TEST(CompressionTest, streamReaderRejectsOversizedReadRequest) {
    const uint8_t input[] = {'H', 'E', 'L', 'L', 'O'};
    MemReader mr = {};
    mr.data = input;
    mr.len = sizeof(input);
    mr.max_chunk = 2;
    streamReaderConfig cfg = makeReaderConfig(true, STREAM_READER_BUFFER_SIZE_DEFAULT, false);

    streamReader t;

    compressionAlgo algo = ALGO_LZ4;
    ASSERT_EQ(streamReaderInit(&t, &cfg, memReaderRead, &mr, &algo), 0);
    ASSERT_EQ(algo, ALGO_NONE);

    uint8_t out[8] = {0};
    size_t oversized = (size_t)SSIZE_MAX + 1;
    ASSERT_EQ(streamReaderRead(&t, out, oversized), -1)
        << "oversized reads should fail before touching stream state";

    ASSERT_EQ(streamReaderRead(&t, out, sizeof(input)), (ssize_t)sizeof(input));
    EXPECT_EQ(memcmp(out, input, sizeof(input)), 0) << "subsequent valid read should still succeed";

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
    db->data = NULL;
}

static int emitToDynamicBuf(void *ctx, const uint8_t *data, size_t len) {
    DynamicBuf *db = (DynamicBuf *)ctx;
    db->data = (uint8_t *)sdscatlen((sds)db->data, data, len);
    return db->data != NULL ? 0 : -1;
}

static int failSelectedEmit(void *ctx, const uint8_t *data, size_t len) {
    (void)data;
    (void)len;
    FailingEmitter *emitter = (FailingEmitter *)ctx;
    emitter->calls++;
    return emitter->calls == emitter->fail_on_call ? -1 : 0;
}

static int initVcsRdbStreamReader(streamReader *reader, rio *r) {
    return rdbInitStreamReader(r, reader, false, NULL) == RDB_STREAM_READER_INIT_OK ? 0 : -1;
}

static void countRioUpdateCalls(rio *r, const void *buf, size_t len) {
    (void)buf;
    (void)len;
    r->cksum++;
}

static int emitToRioBackend(void *ctx, const uint8_t *data, size_t len) {
    return rioWriteRaw((rio *)ctx, data, len) ? 0 : -1;
}

static int attachCompressionWriter(rio *r, streamWriter *writer) {
    if (streamWriterInit(writer, ALGO_LZ4, emitToRioBackend, r) != 0) return -1;
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

TEST(CompressionTest, streamReaderClassifiesSourceCallbackFailuresAsIoErrors) {
    FlakyReader failed_source = {};
    failed_source.fail_after_success_reads = 0;
    streamReaderConfig failed_cfg = makeReaderConfig(true, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
    streamReader failed_reader;
    ASSERT_EQ(streamReaderInit(&failed_reader, &failed_cfg, flakyReaderRead, &failed_source, NULL), -1);
    ASSERT_EQ(failed_reader.error_kind, STREAM_READER_ERROR_IO);
    streamReaderFree(&failed_reader);

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&writer, "source callback contract", 24), 0);
    ASSERT_EQ(streamWriterFinish(&writer), 0);
    streamWriterFree(&writer);

    const int overread_calls[] = {1, 2, 3};
    for (size_t i = 0; i < sizeof(overread_calls) / sizeof(overread_calls[0]); i++) {
        int overread_on_call = overread_calls[i];
        OverreadReader source = {};
        source.data = db.data;
        source.len = sdslen((const char *)db.data);
        source.overread_on_call = overread_on_call;
        streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
        streamReader reader;
        int init_result = streamReaderInit(&reader, &rcfg, overreadReaderRead, &source, NULL);
        if (overread_on_call <= 2) {
            ASSERT_EQ(init_result, -1) << "callback call " << overread_on_call;
            ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_IO)
                << "callback call " << overread_on_call;
            streamReaderFree(&reader);
            continue;
        }
        ASSERT_EQ(init_result, 0);

        uint8_t out[24];
        ASSERT_EQ(streamReaderRead(&reader, out, sizeof(out)), -1) << "callback call " << overread_on_call;
        ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_IO) << "callback call " << overread_on_call;
        ASSERT_EQ(streamReaderRead(&reader, out, sizeof(out)), -1) << "I/O error must remain sticky";
        streamReaderFree(&reader);
    }

    dynamicBufFree(&db);
}

TEST(CompressionTest, streamReaderRejectsInvalidEnvelopeFields) {
    const uint8_t good[VCS_ENVELOPE_SIZE] = {
        VCS_MAGIC_0,
        VCS_MAGIC_1,
        VCS_MAGIC_2,
        VCS_VERSION,
        VCS_CODEC_LZ4,
        0,
        VCS_STREAM_RDB,
    };
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

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        uint8_t mutated[VCS_ENVELOPE_SIZE];
        memcpy(mutated, good, sizeof(mutated));
        mutated[cases[i].offset] = cases[i].value;

        MemReader source = {mutated, sizeof(mutated), 0, 0};
        streamReaderConfig cfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
        streamReader reader;
        ASSERT_EQ(streamReaderInit(&reader, &cfg, memReaderRead, &source, NULL), -1) << cases[i].name;
        ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_INCOMPATIBLE) << cases[i].name;
        streamReaderFree(&reader);
    }
}

/* Regression for partial output followed by a source read error. The partial
 * bytes are returned, but the error must remain sticky for the next read. */
TEST(CompressionTest, streamReaderPartialThenErrorSetsErrored) {
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
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, ALGO_LZ4, emitToDynamicBuf, &db), 0);
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
    fr.fail_after_success_reads = INT_MAX;
    streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_MIN, false);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, flakyReaderRead, &fr, NULL), 0);

    const size_t out_len = 16 * 1024;
    uint8_t *out = (uint8_t *)zmalloc(out_len);
    ASSERT_TRUE(out != NULL);
    ssize_t n1 = streamReaderRead(&r, out, out_len);
    ASSERT_GT(n1, 0) << "first read should return partial output";
    ASSERT_LT(n1, (ssize_t)out_len) << "injected read error should stop the first read early";
    EXPECT_EQ(memcmp(out, payload, (size_t)n1), 0);
    ASSERT_EQ(r.error_kind, STREAM_READER_ERROR_IO) << "error must be latched before returning partial output";
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
    streamReaderConfig pass_cfg = makeReaderConfig(true, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
    streamReader rp;
    ASSERT_EQ(streamReaderInit(&rp, &pass_cfg, flakyReaderRead, &fr_passthrough, NULL), 0);

    uint8_t pass_out[64];
    ssize_t p1 = streamReaderRead(&rp, pass_out, sizeof(pass_out));
    ASSERT_GT(p1, 0) << "passthrough first read should return partial output";
    EXPECT_EQ(memcmp(pass_out, plain, (size_t)p1), 0) << "passthrough partial bytes should match input prefix";
    ASSERT_EQ(streamReaderRead(&rp, pass_out, sizeof(pass_out)), -1)
        << "passthrough second read should fail immediately";
    ASSERT_EQ(rp.error_kind, STREAM_READER_ERROR_IO);
    streamReaderFree(&rp);
    zfree(out);

    dynamicBufFree(&db);
    zfree(payload);
}

TEST(CompressionTest, streamWriterInitFree) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(t.state, STREAM_WRITER_STATE_INITIAL);

    streamWriterFree(&t);
    dynamicBufFree(&db);

    streamWriter bad_writer;
    ASSERT_EQ(streamWriterInit(&bad_writer, ALGO_NONE, emitToDynamicBuf, &db), -1) << "ALGO_NONE should fail init";
    ASSERT_EQ(streamWriterInit(&bad_writer, ALGO_LZF, emitToDynamicBuf, &db), -1) << "ALGO_LZF should fail init";
}

TEST(CompressionTest, streamWriterFinishProducesAValidEmptyStream) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, ALGO_LZ4, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterWrite(&writer, NULL, 0), 0);
    ASSERT_EQ(streamWriterFlush(&writer), 0);
    ASSERT_EQ(sdslen((const char *)db.data), 0u) << "empty writes and flushes must stay lazy";
    ASSERT_EQ(streamWriterFinish(&writer), 0);
    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE);
    streamWriterFree(&writer);

    MemReader source = {db.data, sdslen((const char *)db.data), 0, 1};
    streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &source, NULL), 0);
    uint8_t out = 0;
    ASSERT_EQ(streamReaderRead(&reader, &out, 1), 0);
    ASSERT_EQ(streamReaderFinish(&reader), 0);
    streamReaderFree(&reader);
    dynamicBufFree(&db);
}

TEST(CompressionTest, streamWriterSinkFailuresAreSticky) {
    const int failing_calls[] = {1, 2};
    for (size_t i = 0; i < sizeof(failing_calls) / sizeof(failing_calls[0]); i++) {
        int fail_on_call = failing_calls[i];
        FailingEmitter emitter = {0, fail_on_call};
        streamWriter writer;
        ASSERT_EQ(streamWriterInit(&writer, ALGO_LZ4, failSelectedEmit, &emitter), 0);

        ASSERT_EQ(streamWriterWrite(&writer, "payload", 7), -1) << "sink call " << fail_on_call;
        ASSERT_EQ(writer.state, STREAM_WRITER_STATE_ERROR);
        ASSERT_EQ(streamWriterWrite(&writer, "retry", 5), -1);
        ASSERT_EQ(streamWriterFlush(&writer), -1);
        ASSERT_EQ(streamWriterFinish(&writer), -1);
        ASSERT_EQ(emitter.calls, fail_on_call) << "an errored writer must not emit more bytes";
        streamWriterFree(&writer);
    }

    FailingEmitter emitter = {0, 0};
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, ALGO_LZ4, failSelectedEmit, &emitter), 0);
    ASSERT_EQ(streamWriterWrite(&writer, "payload", 7), 0);
    emitter.fail_on_call = emitter.calls + 1;
    ASSERT_EQ(streamWriterFinish(&writer), -1);
    int calls_after_failure = emitter.calls;
    ASSERT_EQ(streamWriterFinish(&writer), -1);
    ASSERT_EQ(streamWriterWrite(&writer, "retry", 5), -1);
    ASSERT_EQ(emitter.calls, calls_after_failure) << "a failed finish must remain failed";
    streamWriterFree(&writer);
}

TEST(CompressionTest, streamWriterRoundTrip) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);

    const char *test_data = "Hello, compression world! This is a test of the stream writer API.";
    size_t data_len = strlen(test_data);
    ASSERT_EQ(streamWriterWrite(&t, test_data, data_len), 0);
    EXPECT_EQ(t.state, STREAM_WRITER_STATE_ACTIVE);
    ASSERT_GE(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE) << "write should emit envelope";

    ASSERT_EQ(streamWriterFinish(&t), 0);
    EXPECT_EQ(t.state, STREAM_WRITER_STATE_FINISHED);

    ASSERT_GE(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE)
        << "output should have at least envelope size";
    EXPECT_EQ(db.data[0], VCS_MAGIC_0) << "magic byte 0";
    EXPECT_EQ(db.data[1], VCS_MAGIC_1) << "magic byte 1";
    EXPECT_EQ(db.data[2], VCS_MAGIC_2) << "magic byte 2";
    EXPECT_EQ(db.data[3], VCS_VERSION) << "version";
    EXPECT_EQ(db.data[4], VCS_CODEC_LZ4) << "codec id";
    EXPECT_EQ(db.data[5], (uint8_t)0) << "reserved byte";
    EXPECT_EQ(db.data[6], VCS_STREAM_RDB) << "stream_kind RDB";

    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4, false), 0);

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

    EXPECT_EQ(total_decompressed, data_len) << "decompressed length should match original";
    EXPECT_EQ(memcmp(decompressed, test_data, data_len), 0) << "decompressed data should match original";

    streamDecompressorFree(&sd);
    streamWriterFree(&t);
    dynamicBufFree(&db);
}

/* A single write larger than STREAM_WRITER_INPUT_CHUNK_SIZE exercises the
 * writer's bounded scratch-buffer path. */
TEST(CompressionTest, streamWriterLargeSingleWrite) {
    const size_t payload_len = (1024 * 1024) + 4096;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 17 + 11) % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&t, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    streamWriterFree(&t);

    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data);
    mr.max_chunk = 0;
    streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_MIN, false);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr, NULL), 0);

    uint8_t *out = (uint8_t *)zmalloc(payload_len);
    size_t total = 0;
    while (total < payload_len) {
        ssize_t nread = streamReaderRead(&r, out + total, payload_len - total);
        if (nread <= 0) {
            ADD_FAILURE() << "streamReaderRead should keep making progress";
            break;
        }
        total += (size_t)nread;
    }
    EXPECT_EQ(total, payload_len);
    if (total == payload_len) {
        EXPECT_EQ(memcmp(out, payload, payload_len), 0);
    }
    EXPECT_EQ(streamReaderRead(&r, out, 1), 0) << "reader should stop at frame end";

    zfree(out);
    zfree(payload);
    streamReaderFree(&r);
    dynamicBufFree(&db);
}

/* Small caller reads exercise the buffered decompressed window. */
TEST(CompressionTest, streamReaderSmallReadsRoundTrip) {
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    ASSERT_TRUE(payload != NULL);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)((i * 29 + 7) % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data);
    mr.max_chunk = 4096;
    streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_MIN, false);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr, NULL), 0);

    uint8_t *out = (uint8_t *)zmalloc(payload_len);
    ASSERT_TRUE(out != NULL);
    size_t total = 0;
    while (total < payload_len) {
        size_t step = payload_len - total;
        if (step > 17) step = 17;
        ssize_t nread = streamReaderRead(&r, out + total, step);
        if (nread <= 0) {
            ADD_FAILURE() << "streamReaderRead should keep making progress";
            break;
        }
        total += (size_t)nread;
    }

    EXPECT_EQ(total, payload_len);
    if (total == payload_len) {
        EXPECT_EQ(memcmp(out, payload, payload_len), 0);
    }
    EXPECT_EQ(streamReaderRead(&r, out, 1), 0) << "reader should stop at frame end";

    zfree(out);
    zfree(payload);
    streamReaderFree(&r);
    dynamicBufFree(&db);
}

TEST(CompressionTest, streamWriterFlushBehavior) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterFlush(&t), 0) << "flush before write should be no-op success";
    ASSERT_EQ(sdslen((const char *)db.data), 0u) << "flush before write should not emit bytes";

    ASSERT_EQ(streamWriterWrite(&t, "first chunk", 11), 0);
    ASSERT_EQ(streamWriterFlush(&t), 0);
    ASSERT_EQ(streamWriterWrite(&t, "second chunk", 12), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);

    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE);
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4, false), 0);

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

    EXPECT_EQ(total_decompressed, 23u);
    EXPECT_EQ(memcmp(decompressed, "first chunksecond chunk", 23), 0);

    streamDecompressorFree(&sd);
    streamWriterFree(&t);
    dynamicBufFree(&db);
}

TEST(CompressionTest, streamWriterFlushAfterFinishIsNoop) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterWrite(&t, "payload", 7), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    size_t len_after_finish = sdslen((const char *)db.data);

    ASSERT_EQ(streamWriterFlush(&t), 0) << "flush after finish should be a no-op success";
    ASSERT_EQ(sdslen((const char *)db.data), len_after_finish) << "flush after finish should not emit bytes";

    streamWriterFree(&t);
    dynamicBufFree(&db);
}

TEST(CompressionTest, checksumBypassSkipsOnlyCodecVerification) {
    const char *payload = "checksum bypass payload";
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&writer, payload, strlen(payload)), 0);
    ASSERT_EQ(streamWriterFinish(&writer), 0);
    streamWriterFree(&writer);

    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE + 4);
    db.data[sdslen((const char *)db.data) - 1] ^= 1;

    const bool skip_checksum_cases[] = {false, true};
    for (size_t i = 0; i < sizeof(skip_checksum_cases) / sizeof(skip_checksum_cases[0]); i++) {
        bool skip_codec_checksum_validation = skip_checksum_cases[i];
        MemReader source = {db.data, sdslen((const char *)db.data), 0, 0};
        streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_DEFAULT,
                                                   skip_codec_checksum_validation);
        streamReader reader;
        ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &source, NULL), 0);

        char out[64] = {0};
        ASSERT_EQ(streamReaderRead(&reader, out, strlen(payload)), (ssize_t)strlen(payload));
        EXPECT_EQ(memcmp(out, payload, strlen(payload)), 0);
        ASSERT_EQ(streamReaderFinish(&reader), skip_codec_checksum_validation ? 0 : -1);
        streamReaderFree(&reader);
    }

    MemReader truncated = {db.data, sdslen((const char *)db.data) - 5, 0, 0};
    streamReaderConfig bypass = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_DEFAULT, true);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &bypass, memReaderRead, &truncated, NULL), 0);
    char out[64] = {0};
    ASSERT_EQ(streamReaderRead(&reader, out, strlen(payload)), (ssize_t)strlen(payload));
    ASSERT_EQ(streamReaderFinish(&reader), -1)
        << "checksum bypass must not bypass exact frame-end validation";
    streamReaderFree(&reader);

    dynamicBufFree(&db);
}

TEST(CompressionTest, streamReaderFinishAcceptsClosedFrame) {
    const char *payload = "validate frame end payload";
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, strlen(payload)), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);

    MemReader reader_ctx = {db.data, sdslen((const char *)db.data), 0, 7};
    streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &reader_ctx, NULL), 0);
    ASSERT_EQ(reader.state, STREAM_READER_STATE_COMPRESSED);

    char out[64];
    ASSERT_EQ(streamReaderRead(&reader, out, strlen(payload)), (ssize_t)strlen(payload));
    ASSERT_EQ(reader.state, STREAM_READER_STATE_COMPRESSED);
    EXPECT_EQ(memcmp(out, payload, strlen(payload)), 0);
    ASSERT_EQ(streamReaderFinish(&reader), 0);
    ASSERT_EQ(reader.state, STREAM_READER_STATE_FINISHED);
    ASSERT_EQ(streamReaderFinish(&reader), 0);
    ASSERT_EQ(streamReaderRead(&reader, out, sizeof(out)), 0);

    streamReaderFree(&reader);
    streamWriterFree(&w);
    dynamicBufFree(&db);
}

TEST(CompressionTest, streamReaderFinishRejectsUnreadDecodedBytes) {
    const char *payload = "payload with unread decoded suffix";
    const size_t payload_len = strlen(payload);
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);

    MemReader reader_ctx = {db.data, sdslen((const char *)db.data), 0, 0};
    streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_DEFAULT, false);
    streamReader reader;
    ASSERT_EQ(streamReaderInit(&reader, &rcfg, memReaderRead, &reader_ctx, NULL), 0);

    char out[8];
    ASSERT_EQ(streamReaderRead(&reader, out, sizeof(out)), (ssize_t)sizeof(out));
    EXPECT_EQ(memcmp(out, payload, sizeof(out)), 0);
    ASSERT_EQ(streamReaderFinish(&reader), -1);
    ASSERT_EQ(reader.error_kind, STREAM_READER_ERROR_CORRUPT);

    streamReaderFree(&reader);
    streamWriterFree(&w);
    dynamicBufFree(&db);
}

TEST(CompressionTest, rioCompressionWriterRoundTrip) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&buffer_rio, &writer), 0);
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
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4, false), 0);

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

    EXPECT_EQ(total_decompressed, data_len) << "decompressed length should match";
    EXPECT_EQ(memcmp(decompressed, test_data, data_len), 0) << "decompressed data should match";

    streamDecompressorFree(&sd);
    freeCompressionWriter(&buffer_rio, &writer);
    sdsfree(compressed);
}

TEST(CompressionTest, rioCompressionWriterDoesNotOwnRdbChecksumPolicy) {
    const bool checksum_cases[] = {false, true};
    for (size_t i = 0; i < sizeof(checksum_cases) / sizeof(checksum_cases[0]); i++) {
        bool inner_skips_checksum = checksum_cases[i];
        sds buf = sdsempty();
        rio inner;
        rioInitWithBuffer(&inner, buf);
        if (inner_skips_checksum) inner.flags |= RIO_FLAG_SKIP_RDB_CHECKSUM;

        streamWriter writer;
        ASSERT_EQ(attachCompressionWriter(&inner, &writer), 0);
        ASSERT_TRUE(inner.update_cksum == NULL);
        ASSERT_EQ((inner.flags & RIO_FLAG_SKIP_RDB_CHECKSUM) != 0, inner_skips_checksum);
        ASSERT_NE(rioWrite(&inner, "checksum policy", 15), 0u);
        ASSERT_EQ(inner.cksum, 0u);
        ASSERT_EQ(finishCompressionWriter(&inner, &writer), 0);

        freeCompressionWriter(&inner, &writer);
        sdsfree(inner.io.buffer.ptr);
    }
}

TEST(CompressionTest, rioCompressionWriterFlushFailureSetsWriteError) {
    rio inner;
    initFailingFlushRio(&inner);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&inner, &writer), 0);

    const char *payload = "flush failure should latch rio write error";
    ASSERT_NE(rioWrite(&inner, payload, strlen(payload)), 0u);
    ASSERT_EQ(rioFlush(&inner), 0);
    ASSERT_TRUE(inner.flags & RIO_FLAG_WRITE_ERROR);

    freeCompressionWriter(&inner, &writer);
}

TEST(CompressionTest, rioCompressionWriterFinishFailureSetsWriteError) {
    rio inner;
    initFailingFlushRio(&inner);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&inner, &writer), 0);

    const char *payload = "finish failure should latch rio write error";
    ASSERT_NE(rioWrite(&inner, payload, strlen(payload)), 0u);
    ASSERT_EQ(finishCompressionWriter(&inner, &writer), -1);
    ASSERT_TRUE(inner.flags & RIO_FLAG_WRITE_ERROR);

    freeCompressionWriter(&inner, &writer);
}

TEST(CompressionTest, rioStreamReaderRoundTrip) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);

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
    EXPECT_EQ(memcmp(result, test_data, data_len), 0) << "decompressed data should match original";

    rdbFreeStreamReader(&buffer_rio, &reader);
    sdsfree(comp_sds);
    dynamicBufFree(&db);
}

TEST(CompressionTest, rioStreamReaderTellTracksSourceProgress) {
    DynamicBuf db;
    dynamicBufInit(&db);

    char payload[4096];
    memset(payload, 'A', sizeof(payload));
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&t, payload, sizeof(payload)), 0);
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

TEST(CompressionTest, rioStreamReaderHonorsMaxProcessingChunk) {
    const size_t payload_len = 1024;
    const size_t chunk_size = 128;
    uint8_t payload[payload_len];
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter writer;
    ASSERT_EQ(streamWriterInit(&writer, ALGO_LZ4, emitToDynamicBuf, &db), 0);
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
    EXPECT_EQ(memcmp(result, payload, payload_len), 0);

    rdbFreeStreamReader(&buffer_rio, &reader);
    sdsfree(compressed);
    dynamicBufFree(&db);
}

TEST(CompressionTest, rioStreamReaderClassifiesInput) {
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
        EXPECT_EQ(memcmp(result, payload, payload_len), 0) << "payload should be replayed exactly";

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
        ASSERT_EQ(rdbInitStreamReader(&buffer_rio, &reader, false, NULL),
                  RDB_STREAM_READER_INIT_INCOMPATIBLE);

        sdsfree(buf);
    }
}

TEST(CompressionTest, rioCompressionWriterFinishIdempotent) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&buffer_rio, &writer), 0);

    ASSERT_NE(rioWrite(&buffer_rio, "test", 4), 0u);
    ASSERT_EQ(finishCompressionWriter(&buffer_rio, &writer), 0);
    size_t len_after_first = sdslen(buffer_rio.io.buffer.ptr);

    ASSERT_EQ(finishCompressionWriter(&buffer_rio, &writer), 0);
    size_t len_after_second = sdslen(buffer_rio.io.buffer.ptr);
    ASSERT_EQ(len_after_first, len_after_second) << "second finish should not produce more output";

    freeCompressionWriter(&buffer_rio, &writer);
    sdsfree(buffer_rio.io.buffer.ptr);
}

TEST(CompressionTest, rioCompressionWriterFlushMidStream) {
    sds buf = sdsempty();
    rio buffer_rio;
    rioInitWithBuffer(&buffer_rio, buf);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&buffer_rio, &writer), 0);

    ASSERT_NE(rioWrite(&buffer_rio, "first chunk", 11), 0u);

    ASSERT_NE(rioFlush(&buffer_rio), 0) << "flush should succeed";

    ASSERT_NE(rioWrite(&buffer_rio, "second chunk", 12), 0u) << "write after flush should succeed";

    ASSERT_EQ(finishCompressionWriter(&buffer_rio, &writer), 0);

    sds compressed = buffer_rio.io.buffer.ptr;
    size_t compressed_len = sdslen(compressed);
    ASSERT_GT(compressed_len, (size_t)VCS_ENVELOPE_SIZE);

    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4, false), 0);

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

    EXPECT_EQ(total_decompressed, 23u) << "total decompressed should be 23 bytes";
    EXPECT_EQ(memcmp(decompressed, "first chunksecond chunk", 23), 0)
        << "decompressed should match concatenated input";

    streamDecompressorFree(&sd);
    freeCompressionWriter(&buffer_rio, &writer);
    sdsfree(compressed);
}

/* The progress callback must leave a write-side stream rio alone. */
TEST(CompressionTest, rdbLoadProgressCallbackStreamingGuard) {
    sds buf = sdsempty();
    rio inner;
    rioInitWithBuffer(&inner, buf);

    streamWriter writer;
    ASSERT_EQ(attachCompressionWriter(&inner, &writer), 0);

    const char sample[] = "progress-guard";
    rdbLoadProgressCallback(&inner, sample, sizeof(sample) - 1);

    ASSERT_FALSE(inner.flags & RIO_FLAG_READ_ERROR) << "write-side streaming rio must not set read error";

    freeCompressionWriter(&inner, &writer);
    sdsfree(inner.io.buffer.ptr);
}

/* A large read used to drop compressed bytes that were not consumed in one
 * decompression iteration, causing false EOF or data corruption. */
TEST(CompressionTest, rioStreamReaderLargePayload) {
    const size_t payload_len = 256 * 1024;
    uint8_t *payload = (uint8_t *)zmalloc(payload_len);
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i % 251);
    }

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);

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

    EXPECT_EQ(memcmp(result, payload, payload_len), 0) << "decompressed data should match original";

    rdbFreeStreamReader(&buffer_rio, &reader);
    sdsfree(comp_sds);
    zfree(result);
    zfree(payload);
    dynamicBufFree(&db);
}

/* The reader stops at the frame boundary so callers can manage subsequent
 * bytes on a long-lived stream. */
TEST(CompressionTest, streamReaderFinishStopsAtFrameEndBeforeTrailingBytes) {
    const char *payload = "stream-reader-frame-end";
    const size_t payload_len = strlen(payload);
    const char *trailer = "TRAILER-BYTES-AFTER-FRAME";
    const size_t trailer_len = strlen(trailer);

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, ALGO_LZ4, emitToDynamicBuf, &db), 0);
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
    streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_MIN, false);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr, NULL), 0);

    char out[128];
    memset(out, 0, sizeof(out));
    ASSERT_EQ(streamReaderRead(&r, out, payload_len), (ssize_t)payload_len);
    EXPECT_EQ(memcmp(out, payload, payload_len), 0);

    ASSERT_EQ(streamReaderFinish(&r), 0);
    ASSERT_EQ(r.error_kind, STREAM_READER_ERROR_NONE);
    ASSERT_EQ(mr.pos, frame_len) << "streamReader must not consume bytes after the LZ4 frame";

    streamReaderFree(&r);
    sdsfree(input);
    dynamicBufFree(&db);
}

TEST(CompressionTest, streamReaderRejectsTruncatedFrameTrailer) {
    const size_t payload_len = 256;
    uint8_t payload[payload_len];
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (uint8_t)(i & 0xFF);
    }

    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter w;
    ASSERT_EQ(streamWriterInit(&w, ALGO_LZ4, emitToDynamicBuf, &db), 0);
    ASSERT_EQ(streamWriterWrite(&w, payload, payload_len), 0);
    ASSERT_EQ(streamWriterFinish(&w), 0);
    streamWriterFree(&w);

    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE + 1);
    MemReader mr = {};
    mr.data = db.data;
    mr.len = sdslen((const char *)db.data) - 1;
    mr.max_chunk = 7;
    streamReaderConfig rcfg = makeReaderConfig(false, STREAM_READER_BUFFER_SIZE_MIN, false);
    streamReader r;
    ASSERT_EQ(streamReaderInit(&r, &rcfg, memReaderRead, &mr, NULL), 0);

    uint8_t out[payload_len];
    ASSERT_EQ(streamReaderRead(&r, out, payload_len), (ssize_t)payload_len);
    EXPECT_EQ(memcmp(out, payload, payload_len), 0);
    ASSERT_LT(streamReaderRead(&r, out, 1), 0) << "EOF before frame end should be treated as corruption";
    ASSERT_EQ(r.error_kind, STREAM_READER_ERROR_CORRUPT)
        << "truncated compressed frame should latch corruption, not I/O";

    streamReaderFree(&r);
    dynamicBufFree(&db);
}

TEST(CompressionTest, streamWriterWriteAfterFinish) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);

    ASSERT_EQ(streamWriterWrite(&t, "hello", 5), 0);
    ASSERT_EQ(streamWriterFinish(&t), 0);
    size_t len_after_finish = sdslen((const char *)db.data);

    ASSERT_LT(streamWriterWrite(&t, "world", 5), 0);
    ASSERT_EQ(sdslen((const char *)db.data), len_after_finish) << "write after finish should not produce output";

    ASSERT_EQ(streamWriterFinish(&t), 0);
    ASSERT_EQ(sdslen((const char *)db.data), len_after_finish) << "second finish should not produce output";

    ASSERT_GT(sdslen((const char *)db.data), (size_t)VCS_ENVELOPE_SIZE);
    streamDecompressor sd;
    ASSERT_EQ(streamDecompressorInit(&sd, ALGO_LZ4, false), 0);

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
    EXPECT_EQ(memcmp(decompressed, "hello", 5), 0) << "should decompress to 'hello' only";

    streamDecompressorFree(&sd);
    streamWriterFree(&t);
    dynamicBufFree(&db);
}

TEST(CompressionTest, streamWriterRepetitivePayloadRoundTrip) {
    DynamicBuf db;
    dynamicBufInit(&db);
    streamWriter t;
    ASSERT_EQ(streamWriterInit(&t, ALGO_LZ4, emitToDynamicBuf, &db), 0);

    char pattern[4096];
    memset(pattern, 'X', sizeof(pattern));
    for (int i = 0; i < 32; i++) {
        ASSERT_EQ(streamWriterWrite(&t, pattern, sizeof(pattern)), 0);
    }
    ASSERT_EQ(streamWriterFinish(&t), 0);

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
