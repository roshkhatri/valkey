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
         * bits and still parse.  We just exercise the path here. */
        readVkcsEnvelope(eb.buf, eb.pos, &got_algo, &got_kind);
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
