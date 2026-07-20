/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_H
#define COMPRESSION_H

#include "fmacros.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

typedef enum {
    ALGO_NONE = 0,
    ALGO_LZF = 1, /* Per-string LZF inside the RDB payload (legacy). */
    ALGO_LZ4 = 2,
} compressionAlgo;

typedef enum {
    COMPRESS_FLUSH_CONTINUE = 0, /* Buffer internally. */
    COMPRESS_FLUSH_SYNC = 1,     /* Drain buffered bytes, keep frame open. */
    COMPRESS_FLUSH_END = 2,      /* Finalize frame. */
} compressFlushMode;

typedef struct {
    compressionAlgo algo;
    int level; /* 0 selects the codec default. */
    void *ctx;
    bool stream_started;
    bool codec_checksum;
} streamCompressor;

typedef struct {
    compressionAlgo algo;
    bool frame_done;
    bool skip_codec_checksum_validation;
    void *ctx;
    size_t input_hint; /* Preferred compressed bytes for next feed, 0 if unknown. */
} streamDecompressor;

/* Returns a static algorithm name for logs and config output. */
const char *compressionAlgoName(compressionAlgo algo);

/* Codec dispatch used by streamReader and streamWriter. The stream objects
 * own lifecycle and sticky error state; these functions only manage codec
 * state and route operations to the selected implementation. */
int streamCompressorInit(streamCompressor *compressor, compressionAlgo algo, int level, bool codec_checksum);
void streamCompressorFree(streamCompressor *compressor);
size_t streamCompressorOutputBound(const streamCompressor *compressor, size_t input_len);
ssize_t streamCompressorFeed(streamCompressor *compressor,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             compressFlushMode flush_mode);

int streamDecompressorInit(streamDecompressor *decompressor,
                           compressionAlgo algo,
                           bool skip_codec_checksum_validation);
void streamDecompressorFree(streamDecompressor *decompressor);
ssize_t streamDecompressorFeed(streamDecompressor *decompressor,
                               uint8_t *output,
                               size_t output_capacity,
                               const uint8_t *input,
                               size_t input_len,
                               size_t *input_consumed);

#endif /* COMPRESSION_H */
