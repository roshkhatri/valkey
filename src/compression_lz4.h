/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_LZ4_H
#define COMPRESSION_LZ4_H

#include "compression.h"

/* LZ4 codec hooks used by the generic streaming compression layer. Callers
 * allocate the streamCompressor/streamDecompressor and free initialized codec
 * state with the matching Free function. */

/* Initializes LZ4 compressor state. Returns 0 on success. */
int compressionLz4CompressorInit(streamCompressor *compressor);

/* Releases LZ4 compressor resources. */
void compressionLz4CompressorFree(streamCompressor *compressor);

/* Initializes LZ4 decompressor state. Returns 0 on success. */
int compressionLz4DecompressorInit(streamDecompressor *decompressor);

/* Releases LZ4 decompressor resources. */
void compressionLz4DecompressorFree(streamDecompressor *decompressor);

/* Returns a conservative upper bound for any flush mode. */
size_t compressionLz4OutputBound(size_t input_len);

/* Compresses input into output. input may be NULL when input_len is zero. */
ssize_t compressionLz4CompressFeed(streamCompressor *compressor,
                                   uint8_t *output,
                                   size_t output_capacity,
                                   const uint8_t *input,
                                   size_t input_len,
                                   compressFlushMode flush_mode);

/* Decompresses input into output and reports consumed input bytes. */
ssize_t compressionLz4DecompressFeed(streamDecompressor *decompressor,
                                     uint8_t *output,
                                     size_t output_capacity,
                                     const uint8_t *input,
                                     size_t input_len,
                                     size_t *input_consumed);

#endif /* COMPRESSION_LZ4_H */
