/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression.h"
#include "compression_lz4.h"
#include "serverassert.h"
#include <limits.h>
#include <string.h>

typedef struct {
    int (*compressor_init)(streamCompressor *sc);
    void (*compressor_free)(streamCompressor *sc);
    int (*decompressor_init)(streamDecompressor *sd);
    void (*decompressor_free)(streamDecompressor *sd);
    size_t (*compress_output_bound)(size_t input_len);
    ssize_t (*compress_feed)(streamCompressor *sc,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             compressFlushMode flush_mode);
    ssize_t (*decompress_feed)(streamDecompressor *sd,
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

typedef struct {
    const char *name;
    const compressionCodec *impl;
} compressionAlgoEntry;

static const compressionAlgoEntry compressionAlgoTable[] = {
    [ALGO_NONE] = {"none", NULL},
    [ALGO_LZF] = {"lzf", NULL},
    [ALGO_LZ4] = {"lz4", &compressionLz4Codec},
};

static const compressionAlgoEntry *compressionAlgoEntryForAlgo(compressionAlgo algo) {
    unsigned int i = (unsigned int)algo;
    if (i >= sizeof(compressionAlgoTable) / sizeof(compressionAlgoTable[0])) return NULL;
    return &compressionAlgoTable[i];
}

bool compressionAlgoSupportsStreaming(compressionAlgo algo) {
    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(algo);
    return entry && entry->impl != NULL;
}

const char *compressionAlgoName(compressionAlgo algo) {
    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(algo);
    return (entry && entry->name) ? entry->name : "unknown";
}

int streamCompressorInit(streamCompressor *sc, compressionAlgo algo, int level) {
    assert(sc != NULL);
    memset(sc, 0, sizeof(*sc));

    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(algo);
    const compressionCodec *impl = entry ? entry->impl : NULL;
    if (!impl) return -1;

    sc->algo = algo;
    sc->level = level;

    if (impl->compressor_init(sc) != 0) {
        streamCompressorFree(sc);
        return -1;
    }
    return 0;
}

void streamCompressorFree(streamCompressor *sc) {
    if (!sc) return;
    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(sc->algo);
    const compressionCodec *impl = entry ? entry->impl : NULL;
    if (impl) impl->compressor_free(sc);
    memset(sc, 0, sizeof(*sc));
}

int streamDecompressorInit(streamDecompressor *sd, compressionAlgo algo) {
    assert(sd != NULL);
    memset(sd, 0, sizeof(*sd));

    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(algo);
    const compressionCodec *impl = entry ? entry->impl : NULL;
    if (!impl) return -1;

    sd->algo = algo;

    if (impl->decompressor_init(sd) != 0) {
        streamDecompressorFree(sd);
        return -1;
    }
    return 0;
}

void streamDecompressorFree(streamDecompressor *sd) {
    if (!sd) return;
    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(sd->algo);
    const compressionCodec *impl = entry ? entry->impl : NULL;
    if (impl) impl->decompressor_free(sd);
    memset(sd, 0, sizeof(*sd));
}

size_t streamCompressOutputBound(const streamCompressor *sc, size_t input_len) {
    assert(sc != NULL);
    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(sc->algo);
    const compressionCodec *impl = entry ? entry->impl : NULL;
    assert(impl != NULL);
    return impl->compress_output_bound(input_len);
}

ssize_t streamCompressFeed(streamCompressor *sc,
                           uint8_t *output,
                           size_t output_capacity,
                           const uint8_t *input,
                           size_t input_len,
                           compressFlushMode flush_mode) {
    assert(sc != NULL);
    assert(output != NULL);
    assert(input_len == 0 || input != NULL);
    if (sc->errored) return -1;

    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(sc->algo);
    const compressionCodec *impl = entry ? entry->impl : NULL;
    assert(impl != NULL);

    return impl->compress_feed(sc, output, output_capacity, input, input_len, flush_mode);
}

ssize_t streamDecompressFeed(streamDecompressor *sd,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             size_t *input_consumed) {
    assert(sd != NULL);
    assert(output != NULL);
    assert(input_consumed != NULL);
    assert(input_len == 0 || input != NULL);
    assert(output_capacity > 0);
    assert(output_capacity <= (size_t)SSIZE_MAX);

    *input_consumed = 0;
    if (sd->errored) return -1;
    if (sd->frame_done) return 0;

    const compressionAlgoEntry *entry = compressionAlgoEntryForAlgo(sd->algo);
    const compressionCodec *impl = entry ? entry->impl : NULL;
    assert(impl != NULL);

    return impl->decompress_feed(sd, output, output_capacity, input, input_len, input_consumed);
}
