/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression.h"
#include "compression_lz4.h"
#include "serverassert.h"
#include <string.h>

bool compressionAlgoSupportsStreaming(compressionAlgo algo) {
    switch (algo) {
    case ALGO_LZ4:
        return true;
    default:
        return false;
    }
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
    compressor->algo = algo;
    compressor->level = level;

    switch (algo) {
    case ALGO_LZ4:
        if (compressionLz4CompressorInit(compressor) != 0) {
            compressionLz4CompressorFree(compressor);
            return -1;
        }
        return 0;
    default:
        return -1;
    }
}

void streamCompressorFree(streamCompressor *compressor) {
    switch (compressor->algo) {
    case ALGO_LZ4:
        compressionLz4CompressorFree(compressor);
        return;
    default:
        assert(0);
        return;
    }
}

int streamDecompressorInit(streamDecompressor *decompressor, compressionAlgo algo) {
    memset(decompressor, 0, sizeof(*decompressor));
    decompressor->algo = algo;

    switch (algo) {
    case ALGO_LZ4:
        if (compressionLz4DecompressorInit(decompressor) != 0) {
            compressionLz4DecompressorFree(decompressor);
            return -1;
        }
        return 0;
    default:
        return -1;
    }
}

void streamDecompressorFree(streamDecompressor *decompressor) {
    switch (decompressor->algo) {
    case ALGO_LZ4:
        compressionLz4DecompressorFree(decompressor);
        return;
    default:
        assert(0);
        return;
    }
}

size_t streamCompressorOutputBound(streamCompressor *compressor, size_t input_len) {
    switch (compressor->algo) {
    case ALGO_LZ4:
        return compressionLz4OutputBound(input_len);
    default:
        assert(0);
        return 0;
    }
}

ssize_t streamCompressorFeed(streamCompressor *compressor,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             compressFlushMode flush_mode) {
    switch (compressor->algo) {
    case ALGO_LZ4:
        return compressionLz4CompressFeed(compressor, output, output_capacity, input, input_len, flush_mode);
    default:
        assert(0);
        return -1;
    }
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

    switch (decompressor->algo) {
    case ALGO_LZ4:
        return compressionLz4DecompressFeed(decompressor, output, output_capacity, input, input_len, input_consumed);
    default:
        assert(0);
        return -1;
    }
}
