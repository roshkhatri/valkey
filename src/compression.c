/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression.h"
#include "compression_lz4.h"
#include "serverassert.h"
#include <string.h>

typedef struct {
    int (*compressor_init)(streamCompressor *compressor);
    void (*compressor_free)(streamCompressor *compressor);
    int (*decompressor_init)(streamDecompressor *decompressor);
    void (*decompressor_free)(streamDecompressor *decompressor);
    size_t (*compress_output_bound)(size_t input_len);
    ssize_t (*compress_feed)(streamCompressor *compressor,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             compressFlushMode flush_mode);
    ssize_t (*decompress_feed)(streamDecompressor *decompressor,
                               uint8_t *output,
                               size_t output_capacity,
                               const uint8_t *input,
                               size_t input_len,
                               size_t *input_consumed);
} compressionCodec;

static const compressionCodec compressionLz4Codec = {
    .compressor_init = compressionLz4CompressorInit,
    .compressor_free = compressionLz4CompressorFree,
    .decompressor_init = compressionLz4DecompressorInit,
    .decompressor_free = compressionLz4DecompressorFree,
    .compress_output_bound = compressionLz4OutputBound,
    .compress_feed = compressionLz4CompressFeed,
    .decompress_feed = compressionLz4DecompressFeed,
};

static const compressionCodec *compressionCodecForAlgo(compressionAlgo algo) {
    switch (algo) {
    case ALGO_LZ4:
        return &compressionLz4Codec;
    default:
        return NULL;
    }
}

bool compressionAlgoSupportsStreaming(compressionAlgo algo) {
    return compressionCodecForAlgo(algo) != NULL;
}

const char *compressionAlgoName(compressionAlgo algo) {
    switch (algo) {
    case ALGO_NONE:
        return "none";
    case ALGO_LZF:
        return "lzf";
    case ALGO_LZ4:
        return "lz4";
    default:
        return "unknown";
    }
}

int streamCompressorInit(streamCompressor *compressor, compressionAlgo algo, int level) {
    memset(compressor, 0, sizeof(*compressor));

    const compressionCodec *codec = compressionCodecForAlgo(algo);
    if (!codec) return -1;

    compressor->algo = algo;
    compressor->level = level;

    if (codec->compressor_init(compressor) != 0) {
        codec->compressor_free(compressor);
        return -1;
    }
    return 0;
}

void streamCompressorFree(streamCompressor *compressor) {
    const compressionCodec *codec = compressionCodecForAlgo(compressor->algo);
    assert(codec != NULL);
    codec->compressor_free(compressor);
}

int streamDecompressorInit(streamDecompressor *decompressor, compressionAlgo algo) {
    memset(decompressor, 0, sizeof(*decompressor));

    const compressionCodec *codec = compressionCodecForAlgo(algo);
    if (!codec) return -1;

    decompressor->algo = algo;

    if (codec->decompressor_init(decompressor) != 0) {
        codec->decompressor_free(decompressor);
        return -1;
    }
    return 0;
}

void streamDecompressorFree(streamDecompressor *decompressor) {
    const compressionCodec *codec = compressionCodecForAlgo(decompressor->algo);
    assert(codec != NULL);
    codec->decompressor_free(decompressor);
}

size_t streamCompressorOutputBound(streamCompressor *compressor, size_t input_len) {
    const compressionCodec *codec = compressionCodecForAlgo(compressor->algo);
    assert(codec != NULL);
    return codec->compress_output_bound(input_len);
}

ssize_t streamCompressorFeed(streamCompressor *compressor,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             compressFlushMode flush_mode) {
    const compressionCodec *codec = compressionCodecForAlgo(compressor->algo);
    assert(codec != NULL);
    return codec->compress_feed(compressor, output, output_capacity, input, input_len, flush_mode);
}

ssize_t streamDecompressorFeed(streamDecompressor *decompressor,
                               uint8_t *output,
                               size_t output_capacity,
                               const uint8_t *input,
                               size_t input_len,
                               size_t *input_consumed) {
    *input_consumed = 0;
    if (decompressor->errored) return -1;
    if (decompressor->frame_done) return 0;

    const compressionCodec *codec = compressionCodecForAlgo(decompressor->algo);
    assert(codec != NULL);
    return codec->decompress_feed(decompressor, output, output_capacity, input, input_len, input_consumed);
}
