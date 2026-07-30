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

TEST(replCompression, capaCompressionStr) {
    EXPECT_STREQ(REPLICA_CAPA_COMPRESSION_STR, "compression");
}

TEST(replCompression, algoConstants) {
    /* ALGO_LZ4 must be non-zero so it's distinguishable from zero-init. */
    EXPECT_NE(ALGO_LZ4, 0);
}

TEST(replCompression, resetBatchRetainsAllocationForIncompressibleBatch) {
    replCompressor *rc = replCompressorCreate(ALGO_LZ4, 0);
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
    ASSERT_EQ(replCompressorWrite(rc, buf, n), 0);
    ASSERT_EQ(replCompressorFlush(rc), 0);
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
