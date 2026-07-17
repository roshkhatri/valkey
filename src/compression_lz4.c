/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_lz4.h"
#include "serverassert.h"
#include "zmalloc.h"
#include <limits.h>

#define LZ4F_STATIC_LINKING_ONLY
#include <lz4frame.h>

static void *lz4Zmalloc(void *opaque, size_t size) {
    (void)opaque;
    return zmalloc(size);
}

static void *lz4Zcalloc(void *opaque, size_t size) {
    (void)opaque;
    return zcalloc(size);
}

static void lz4Zfree(void *opaque, void *address) {
    (void)opaque;
    zfree(address);
}

static const LZ4F_CustomMem lz4f_mem = {
    .customAlloc = lz4Zmalloc,
    .customCalloc = lz4Zcalloc,
    .customFree = lz4Zfree,
    .opaqueState = NULL,
};

/* Shared bound-calc preferences. The actual compress level and checksum mode
 * are overridden per stream before LZ4F_compressBegin. */
static const LZ4F_preferences_t lz4f_prefs = {
    .frameInfo = {
        .blockChecksumFlag = LZ4F_blockChecksumEnabled,
        .contentChecksumFlag = LZ4F_contentChecksumEnabled,
        .blockSizeID = LZ4F_max64KB,
        .blockMode = LZ4F_blockLinked,
    },
    .compressionLevel = 0,
};

int compressionLz4CompressorInit(streamCompressor *compressor) {
    compressor->ctx = LZ4F_createCompressionContext_advanced(lz4f_mem, LZ4F_VERSION);
    assert(compressor->ctx != NULL);
    return 0;
}

void compressionLz4CompressorFree(streamCompressor *compressor) {
    if (compressor->ctx) {
        LZ4F_freeCompressionContext((LZ4F_cctx *)compressor->ctx);
        compressor->ctx = NULL;
    }
}

int compressionLz4DecompressorInit(streamDecompressor *decompressor) {
    decompressor->ctx = LZ4F_createDecompressionContext_advanced(lz4f_mem, LZ4F_VERSION);
    assert(decompressor->ctx != NULL);
    decompressor->input_hint = LZ4F_HEADER_SIZE_MIN;
    return 0;
}

void compressionLz4DecompressorFree(streamDecompressor *decompressor) {
    if (decompressor->ctx) {
        LZ4F_freeDecompressionContext((LZ4F_dctx *)decompressor->ctx);
        decompressor->ctx = NULL;
    }
}

size_t compressionLz4OutputBound(size_t input_len) {
    return LZ4F_compressBound(input_len, &lz4f_prefs) + LZ4F_HEADER_SIZE_MAX + LZ4F_compressBound(0, &lz4f_prefs);
}

ssize_t compressionLz4CompressFeed(streamCompressor *compressor,
                                   uint8_t *output,
                                   size_t output_capacity,
                                   const uint8_t *input,
                                   size_t input_len,
                                   compressFlushMode flush_mode) {
    assert(compressor->ctx != NULL);

    LZ4F_cctx *cctx = (LZ4F_cctx *)compressor->ctx;
    size_t offset = 0;

    /* Capacity-shortage returns are retriable: they happen before LZ4F mutates
     * its own state, so the caller can grow the buffer and retry without
     * breaking the frame. An LZ4F error after that point is permanent: the
     * frame is partially emitted and cannot be retried. */

    if (!compressor->stream_started) {
        LZ4F_preferences_t prefs = lz4f_prefs;
        prefs.compressionLevel = compressor->level;
        prefs.frameInfo.blockChecksumFlag = compressor->codec_checksum
                                                ? LZ4F_blockChecksumEnabled
                                                : LZ4F_noBlockChecksum;
        prefs.frameInfo.contentChecksumFlag = compressor->codec_checksum
                                                  ? LZ4F_contentChecksumEnabled
                                                  : LZ4F_noContentChecksum;
        size_t r = LZ4F_compressBegin(cctx, output, output_capacity, &prefs);
        if (LZ4F_isError(r)) return -1; /* No frame bytes emitted yet: retriable, don't latch errored. */
        offset = r;
        compressor->stream_started = true;
    }

    if (input_len > 0) {
        if (offset >= output_capacity) return -1; /* Retriable: buffer too small, frame not yet advanced. */
        size_t r = LZ4F_compressUpdate(cctx, output + offset, output_capacity - offset, input, input_len, NULL);
        if (LZ4F_isError(r)) return -1;
        offset += r;
    }

    switch (flush_mode) {
    case FLUSH_CONTINUE:
        break;
    case FLUSH_SYNC: {
        if (offset >= output_capacity) return -1; /* Retriable: buffer too small, frame not yet advanced. */
        size_t r = LZ4F_flush(cctx, output + offset, output_capacity - offset, NULL);
        if (LZ4F_isError(r)) return -1;
        offset += r;
        break;
    }
    case FLUSH_END: {
        if (offset >= output_capacity) return -1; /* Retriable: buffer too small, frame not yet advanced. */
        size_t r = LZ4F_compressEnd(cctx, output + offset, output_capacity - offset, NULL);
        if (LZ4F_isError(r)) return -1;
        offset += r;
        compressor->stream_started = false;
        break;
    }
    default:
        assert(0 && "invalid compressFlushMode");
    }

    if (offset > (size_t)SSIZE_MAX) return -1;
    return (ssize_t)offset;
}

ssize_t compressionLz4DecompressFeed(streamDecompressor *decompressor,
                                     uint8_t *output,
                                     size_t output_capacity,
                                     const uint8_t *input,
                                     size_t input_len,
                                     size_t *input_consumed) {
    assert(decompressor->ctx != NULL);

    LZ4F_dctx *dctx = (LZ4F_dctx *)decompressor->ctx;
    size_t dst_size = output_capacity;
    size_t src_size = input_len;
    LZ4F_decompressOptions_t options = {
        .skipChecksums = decompressor->skip_codec_checksum_validation,
    };
    size_t ret = LZ4F_decompress(dctx, output, &dst_size, input, &src_size, &options);
    if (LZ4F_isError(ret)) {
        decompressor->errored = true;
        return -1;
    }
    *input_consumed = src_size;
    decompressor->input_hint = ret;
    if (ret == 0) decompressor->frame_done = true;
    return (ssize_t)dst_size;
}
