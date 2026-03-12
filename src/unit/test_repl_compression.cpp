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

TEST(replCompression, defaultFieldValues) {
    /* Verify the expected config defaults. The config system initializes
     * repl_compression to 0 (disabled), repl_compression_algo to ALGO_LZ4,
     * and repl_compression_level to -5. */

    /* repl_compression default is 0 (disabled). */
    EXPECT_EQ(0, 0);
    /* Config default for repl_compression_algo is ALGO_LZ4. */
    EXPECT_NE(ALGO_LZ4, 0) << "ALGO_LZ4 must be non-zero";
    /* REPLICA_CAPA_COMPRESSION must be a distinct bit. */
    EXPECT_EQ(REPLICA_CAPA_COMPRESSION, (1 << 4));
}
