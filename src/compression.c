/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Streaming compression/decompression using algorithm-native framing.
 * LZ4 via LZ4F frame API, ZSTD via streaming API.
 * See .kiro/specs/compression-module/design-compact.md for full design. */

#include "compression.h"

#include <string.h>

/* --- Envelope --- */

int write_vkcs_envelope(void (*emit_cb)(void *ctx, const uint8_t *data, size_t len),
                        void *ctx,
                        compression_algo_t algo,
                        uint8_t stream_kind) {
    /* TODO: Task 1.3 */
    (void)emit_cb;
    (void)ctx;
    (void)algo;
    (void)stream_kind;
    return -1;
}

int envelope_read(const uint8_t *buf, size_t len, compression_algo_t *algo, uint8_t *stream_kind) {
    /* TODO: Task 1.3 */
    (void)buf;
    (void)len;
    (void)algo;
    (void)stream_kind;
    return -1;
}

/* --- Streaming compressor --- */

int stream_compressor_init(stream_compressor_t *sc, compression_algo_t algo, int level) {
    /* TODO: Task 2.1 (LZ4), Task 22.1 (ZSTD) */
    (void)sc;
    (void)algo;
    (void)level;
    return -1;
}

void stream_compressor_destroy(stream_compressor_t *sc) {
    /* TODO: Task 2.1 (LZ4), Task 22.1 (ZSTD) */
    (void)sc;
}

int stream_decompressor_init(stream_decompressor_t *sd, compression_algo_t algo) {
    /* TODO: Task 2.2 (LZ4), Task 22.2 (ZSTD) */
    (void)sd;
    (void)algo;
    return -1;
}

void stream_decompressor_destroy(stream_decompressor_t *sd) {
    /* TODO: Task 2.2 (LZ4), Task 22.2 (ZSTD) */
    (void)sd;
}

size_t streamCompressOutputBound(compression_algo_t algo, size_t input_len, int frame_started, int flush_mode) {
    /* TODO: Task 2.3 */
    (void)algo;
    (void)input_len;
    (void)frame_started;
    (void)flush_mode;
    return 0;
}

ssize_t streamCompressFeed(stream_compressor_t *sc,
                           uint8_t **output_ptr,
                           size_t output_capacity,
                           const uint8_t *input,
                           size_t input_len,
                           int flush_mode) {
    /* TODO: Task 2.3 */
    (void)sc;
    (void)output_ptr;
    (void)output_capacity;
    (void)input;
    (void)input_len;
    (void)flush_mode;
    return -1;
}

ssize_t streamDecompressFeed(stream_decompressor_t *sd,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             size_t *input_consumed) {
    /* TODO: Task 2.2 */
    (void)sd;
    (void)output;
    (void)output_capacity;
    (void)input;
    (void)input_len;
    (void)input_consumed;
    return -1;
}
