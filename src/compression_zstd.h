/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_ZSTD_H
#define COMPRESSION_ZSTD_H

#include "compression.h"

int compressionZstdCompressorInit(stream_compressor_t *sc);
void compressionZstdCompressorDestroy(stream_compressor_t *sc);
int compressionZstdDecompressorInit(stream_decompressor_t *sd);
void compressionZstdDecompressorDestroy(stream_decompressor_t *sd);
size_t compressionZstdOutputBound(size_t input_len, bool frame_started, compress_flush_mode_t flush_mode);
ssize_t compressionZstdCompressFeed(stream_compressor_t *sc,
                                    uint8_t *output,
                                    size_t output_capacity,
                                    const uint8_t *input,
                                    size_t input_len,
                                    compress_flush_mode_t flush_mode);
ssize_t compressionZstdDecompressFeed(stream_decompressor_t *sd,
                                      uint8_t *output,
                                      size_t output_capacity,
                                      const uint8_t *input,
                                      size_t input_len,
                                      size_t *input_consumed);

#endif /* COMPRESSION_ZSTD_H */
