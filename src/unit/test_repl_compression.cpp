/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Unit tests for replication compression configuration and capability constants. */

#include "generated_wrappers.hpp"

#include <cstring>

extern "C" {
#include "compression.h"
#include "compression_stream.h"
#include "repl_stream.h"
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

TEST(replCompression, capaCompressionStr) {
    EXPECT_STREQ(REPLICA_CAPA_COMPRESSION_STR, "compression");
}

TEST(replCompression, algoConstants) {
    /* ALGO_LZ4 must be non-zero so it's distinguishable from zero-init. */
    EXPECT_NE(ALGO_LZ4, 0);
}

/* ===================================================================
 * replStreamDecoder tests
 * =================================================================== */

static sds test_emit_buf = NULL;
static int testEmitCallback(void *ctx, const uint8_t *data, size_t len) {
    (void)ctx;
    test_emit_buf = sdscatlen(test_emit_buf, data, len);
    return 0;
}

TEST(replStreamDecoder, PassthroughMode) {
    const char *input = "*3\r\n$3\r\nSET\r\n";
    size_t input_len = strlen(input);

    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    ASSERT_NE(dec, nullptr);

    sds dst = sdsempty();
    ASSERT_EQ(replStreamDecoderFeed(dec, input, input_len, &dst), C_OK);
    EXPECT_EQ(sdslen(dst), input_len);
    EXPECT_EQ(memcmp(dst, input, input_len), 0);

    sdsfree(dst);
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, CompressedRoundTrip) {
    const char *original = "REPLCONF GETACK *\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    size_t original_len = strlen(original);

    /* Compress with stream_writer using STREAM_KIND_REPL */
    test_emit_buf = sdsempty();
    stream_writer_config_t cfg = {};
    cfg.algo = ALGO_LZ4;
    cfg.level = 0;
    cfg.stream_kind = STREAM_KIND_REPL;
    stream_writer_t *w = stream_writer_create(&cfg, testEmitCallback, NULL);
    ASSERT_NE(w, nullptr);
    ASSERT_GE(stream_writer_write(w, original, original_len), 0);
    ASSERT_EQ(stream_writer_finish(w), 0);
    stream_writer_destroy(w);

    /* Feed compressed output into decoder */
    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();
    ASSERT_EQ(replStreamDecoderFeed(dec, test_emit_buf, sdslen(test_emit_buf), &dst), C_OK);
    EXPECT_EQ(sdslen(dst), original_len);
    EXPECT_EQ(memcmp(dst, original, original_len), 0);

    sdsfree(dst);
    sdsfree(test_emit_buf);
    test_emit_buf = NULL;
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, IncrementalProbe) {
    /* Build a valid VKCS envelope for STREAM_KIND_REPL + LZ4, then compress some data */
    const char *payload = "hello";
    size_t payload_len = strlen(payload);

    test_emit_buf = sdsempty();
    stream_writer_config_t cfg = {};
    cfg.algo = ALGO_LZ4;
    cfg.stream_kind = STREAM_KIND_REPL;
    stream_writer_t *w = stream_writer_create(&cfg, testEmitCallback, NULL);
    ASSERT_NE(w, nullptr);
    ASSERT_GE(stream_writer_write(w, payload, payload_len), 0);
    ASSERT_EQ(stream_writer_finish(w), 0);
    stream_writer_destroy(w);

    /* Feed one byte at a time */
    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();
    size_t total = sdslen(test_emit_buf);
    for (size_t i = 0; i < total; i++) {
        ASSERT_EQ(replStreamDecoderFeed(dec, test_emit_buf + i, 1, &dst), C_OK);
    }
    EXPECT_EQ(sdslen(dst), payload_len);
    EXPECT_EQ(memcmp(dst, payload, payload_len), 0);

    sdsfree(dst);
    sdsfree(test_emit_buf);
    test_emit_buf = NULL;
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, WrongStreamKindRejected) {
    /* Build a valid VKCS envelope with STREAM_KIND_RDB instead of REPL */
    uint8_t envelope[VKCS_ENVELOPE_SIZE];
    envelope[0] = VKCS_MAGIC_0;
    envelope[1] = VKCS_MAGIC_1;
    envelope[2] = VKCS_MAGIC_2;
    envelope[3] = VKCS_MAGIC_3;
    envelope[4] = VKCS_VERSION;
    envelope[5] = VKCS_CODEC_LZ4;
    envelope[6] = 0;
    envelope[7] = STREAM_KIND_RDB; /* Wrong kind for repl decoder */

    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();
    EXPECT_EQ(replStreamDecoderFeed(dec, envelope, VKCS_ENVELOPE_SIZE, &dst), C_ERR);

    sdsfree(dst);
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, PartialCompressedData) {
    const char *original = "partial-feed-test-data-that-is-long-enough-to-produce-output";
    size_t original_len = strlen(original);

    test_emit_buf = sdsempty();
    stream_writer_config_t cfg = {};
    cfg.algo = ALGO_LZ4;
    cfg.stream_kind = STREAM_KIND_REPL;
    stream_writer_t *w = stream_writer_create(&cfg, testEmitCallback, NULL);
    ASSERT_NE(w, nullptr);
    ASSERT_GE(stream_writer_write(w, original, original_len), 0);
    ASSERT_EQ(stream_writer_finish(w), 0);
    stream_writer_destroy(w);

    size_t total = sdslen(test_emit_buf);
    size_t half = total / 2;

    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();

    /* Feed first half */
    ASSERT_EQ(replStreamDecoderFeed(dec, test_emit_buf, half, &dst), C_OK);

    /* Feed second half */
    ASSERT_EQ(replStreamDecoderFeed(dec, test_emit_buf + half, total - half, &dst), C_OK);
    EXPECT_EQ(sdslen(dst), original_len);
    EXPECT_EQ(memcmp(dst, original, original_len), 0);

    sdsfree(dst);
    sdsfree(test_emit_buf);
    test_emit_buf = NULL;
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, CorruptedCompressedData) {
    /* Valid VKCS envelope for REPL + LZ4, followed by garbage */
    uint8_t buf[VKCS_ENVELOPE_SIZE + 32];
    buf[0] = VKCS_MAGIC_0;
    buf[1] = VKCS_MAGIC_1;
    buf[2] = VKCS_MAGIC_2;
    buf[3] = VKCS_MAGIC_3;
    buf[4] = VKCS_VERSION;
    buf[5] = VKCS_CODEC_LZ4;
    buf[6] = 0;
    buf[7] = STREAM_KIND_REPL;
    /* Fill rest with garbage */
    for (int i = VKCS_ENVELOPE_SIZE; i < (int)sizeof(buf); i++) {
        buf[i] = (uint8_t)(i * 37 + 13);
    }

    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();
    EXPECT_EQ(replStreamDecoderFeed(dec, buf, sizeof(buf), &dst), C_ERR);

    sdsfree(dst);
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, EmptyFeed) {
    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();
    ASSERT_EQ(replStreamDecoderFeed(dec, "", 0, &dst), C_OK);
    EXPECT_EQ(sdslen(dst), 0u);

    sdsfree(dst);
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, NullArgs) {
    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();

    /* NULL decoder */
    EXPECT_EQ(replStreamDecoderFeed(NULL, "x", 1, &dst), C_ERR);
    /* NULL dst */
    EXPECT_EQ(replStreamDecoderFeed(dec, "x", 1, NULL), C_ERR);

    sdsfree(dst);
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, CreateDestroyLifecycle) {
    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    ASSERT_NE(dec, nullptr);
    replStreamDecoderDestroy(dec);
    /* Also verify NULL destroy is safe */
    replStreamDecoderDestroy(NULL);
}

TEST(replStreamDecoder, ZstdCompressedRoundTrip) {
    const char *original = "REPLCONF GETACK *\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    size_t original_len = strlen(original);

    test_emit_buf = sdsempty();
    stream_writer_config_t cfg = {};
    cfg.algo = ALGO_ZSTD;
    cfg.level = 0;
    cfg.stream_kind = STREAM_KIND_REPL;
    stream_writer_t *w = stream_writer_create(&cfg, testEmitCallback, NULL);
    ASSERT_NE(w, nullptr);
    ASSERT_GE(stream_writer_write(w, original, original_len), 0);
    ASSERT_EQ(stream_writer_finish(w), 0);
    stream_writer_destroy(w);

    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();
    ASSERT_EQ(replStreamDecoderFeed(dec, test_emit_buf, sdslen(test_emit_buf), &dst), C_OK);
    EXPECT_EQ(sdslen(dst), original_len);
    EXPECT_EQ(memcmp(dst, original, original_len), 0);

    sdsfree(dst);
    sdsfree(test_emit_buf);
    test_emit_buf = NULL;
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, ZstdIncrementalProbe) {
    const char *payload = "hello";
    size_t payload_len = strlen(payload);

    test_emit_buf = sdsempty();
    stream_writer_config_t cfg = {};
    cfg.algo = ALGO_ZSTD;
    cfg.stream_kind = STREAM_KIND_REPL;
    stream_writer_t *w = stream_writer_create(&cfg, testEmitCallback, NULL);
    ASSERT_NE(w, nullptr);
    ASSERT_GE(stream_writer_write(w, payload, payload_len), 0);
    ASSERT_EQ(stream_writer_finish(w), 0);
    stream_writer_destroy(w);

    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();
    size_t total = sdslen(test_emit_buf);
    for (size_t i = 0; i < total; i++) {
        ASSERT_EQ(replStreamDecoderFeed(dec, test_emit_buf + i, 1, &dst), C_OK);
    }
    EXPECT_EQ(sdslen(dst), payload_len);
    EXPECT_EQ(memcmp(dst, payload, payload_len), 0);

    sdsfree(dst);
    sdsfree(test_emit_buf);
    test_emit_buf = NULL;
    replStreamDecoderDestroy(dec);
}

TEST(replStreamDecoder, ZstdWrongStreamKindRejected) {
    uint8_t envelope[VKCS_ENVELOPE_SIZE];
    envelope[0] = VKCS_MAGIC_0;
    envelope[1] = VKCS_MAGIC_1;
    envelope[2] = VKCS_MAGIC_2;
    envelope[3] = VKCS_MAGIC_3;
    envelope[4] = VKCS_VERSION;
    envelope[5] = VKCS_CODEC_ZSTD;
    envelope[6] = 0;
    envelope[7] = STREAM_KIND_RDB; /* Wrong kind for repl decoder */

    repl_stream_decoder_t *dec = replStreamDecoderCreate();
    sds dst = sdsempty();
    EXPECT_EQ(replStreamDecoderFeed(dec, envelope, VKCS_ENVELOPE_SIZE, &dst), C_ERR);

    sdsfree(dst);
    replStreamDecoderDestroy(dec);
}