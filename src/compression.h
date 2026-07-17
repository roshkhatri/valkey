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
    FLUSH_CONTINUE = 0, /* Buffer internally. */
    FLUSH_SYNC = 1,     /* Drain buffered bytes, keep frame open. */
    FLUSH_END = 2,      /* Finalize frame. */
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
    bool errored;
    bool frame_done;
    bool skip_codec_checksum_validation;
    void *ctx;
    size_t input_hint; /* Preferred compressed bytes for next feed, 0 if unknown. */
} streamDecompressor;

/* Compression APIs expect caller-owned streamCompressor/streamDecompressor
 * storage and valid pointer arguments. Instances are not thread-safe. */

/* Returns true when algo has a streaming codec implementation. */
bool compressionAlgoSupportsStreaming(compressionAlgo algo);

/* Returns a static algorithm name for logs and config output. */
const char *compressionAlgoName(compressionAlgo algo);

/* Initializes compressor state for algo and level. Returns 0 on success. */
int streamCompressorInit(streamCompressor *compressor, compressionAlgo algo, int level);

/* Releases resources owned by an initialized compressor. */
void streamCompressorFree(streamCompressor *compressor);

/* Initializes decompressor state for algo. Returns 0 on success. */
int streamDecompressorInit(streamDecompressor *decompressor, compressionAlgo algo);

/* Releases resources owned by an initialized decompressor. */
void streamDecompressorFree(streamDecompressor *decompressor);

/* Conservative bound covering header + data + flush/end overhead, so the
 * caller can size one scratch buffer for all flush modes. */
size_t streamCompressorOutputBound(streamCompressor *compressor, size_t input_len);

/* Feeds input into the compressor and writes compressed bytes to output.
 * Called repeatedly to build one frame: FLUSH_CONTINUE keeps buffering,
 * FLUSH_SYNC drains buffered bytes but leaves the frame open, FLUSH_END
 * closes it. output must be at least streamCompressorOutputBound(input_len)
 * bytes. Returns bytes written, or -1 on error. */
ssize_t streamCompressorFeed(streamCompressor *compressor,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             compressFlushMode flush_mode);

/* Decompresses input into output. When output fills before input is drained,
 * only part of input is used: *input_consumed reports how many input bytes
 * were read, and the caller feeds the rest on the next call with more output
 * space. Returns bytes written, or -1 on error. */
ssize_t streamDecompressorFeed(streamDecompressor *decompressor,
                               uint8_t *output,
                               size_t output_capacity,
                               const uint8_t *input,
                               size_t input_len,
                               size_t *input_consumed);

#endif /* COMPRESSION_H */
