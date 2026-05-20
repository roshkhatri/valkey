/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_zstd.h"
#include <limits.h>
#include <zstd.h>

#define ZSTD_FRAME_OVERHEAD_MAX 22 /* 18-byte frame header + 4-byte content checksum. */

int compressionZstdCompressorInit(streamCompressor *sc) {
    ZSTD_CCtx *cctx = NULL;
    if (!sc) return -1;

    cctx = ZSTD_createCCtx();
    if (!cctx) return -1;

    sc->ctx = cctx;
    return 0;
}

void compressionZstdCompressorDestroy(streamCompressor *sc) {
    if (!sc || !sc->ctx) return;
    ZSTD_freeCCtx((ZSTD_CCtx *)sc->ctx);
    sc->ctx = NULL;
}

int compressionZstdDecompressorInit(streamDecompressor *sd) {
    ZSTD_DCtx *dctx = NULL;
    if (!sd) return -1;

    dctx = ZSTD_createDCtx();
    if (!dctx) return -1;

    sd->ctx = dctx;
    sd->input_hint = ZSTD_DStreamInSize();
    return 0;
}

void compressionZstdDecompressorDestroy(streamDecompressor *sd) {
    if (!sd || !sd->ctx) return;
    ZSTD_freeDCtx((ZSTD_DCtx *)sd->ctx);
    sd->ctx = NULL;
}

size_t compressionZstdOutputBound(size_t input_len) {
    size_t data_bound = ZSTD_compressBound(input_len);
    size_t stream_bound = ZSTD_CStreamOutSize();
    if (ZSTD_isError(data_bound) || data_bound > SIZE_MAX - stream_bound) return 0;
    size_t bound = data_bound + stream_bound;
    if (bound > SIZE_MAX - ZSTD_FRAME_OVERHEAD_MAX) return 0;
    return bound + ZSTD_FRAME_OVERHEAD_MAX;
}

ssize_t compressionZstdCompressFeed(streamCompressor *sc,
                                    uint8_t *output,
                                    size_t output_capacity,
                                    const uint8_t *input,
                                    size_t input_len,
                                    compressFlushMode flush_mode) {
    if (!sc || !sc->ctx) return -1;
    size_t bound = compressionZstdOutputBound(input_len);
    if (bound == 0 || output_capacity < bound) {
        if (sc->stream_started) sc->errored = true;
        return -1;
    }

    ZSTD_CCtx *cctx = (ZSTD_CCtx *)sc->ctx;
    ZSTD_EndDirective directive;
    switch (flush_mode) {
    case FLUSH_CONTINUE: directive = ZSTD_e_continue; break;
    case FLUSH_SYNC: directive = ZSTD_e_flush; break;
    case FLUSH_END: directive = ZSTD_e_end; break;
    default: sc->errored = true; return -1;
    }

    if (!sc->stream_started) {
        size_t ret = ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);
        if (ZSTD_isError(ret)) goto zstd_error;
        ret = ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, sc->level);
        if (ZSTD_isError(ret)) goto zstd_error;
        ret = ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, sc->codec_checksum ? 1 : 0);
        if (ZSTD_isError(ret)) goto zstd_error;
        sc->stream_started = true;
    }

    uint8_t empty_sentinel = 0;
    ZSTD_inBuffer in_buf = {
        .src = input ? input : &empty_sentinel,
        .size = input_len,
        .pos = 0,
    };
    ZSTD_outBuffer out_buf = {
        .dst = output,
        .size = output_capacity,
        .pos = 0,
    };

    size_t ret = 0;
    do {
        ret = ZSTD_compressStream2(cctx, &out_buf, &in_buf, directive);
        if (ZSTD_isError(ret)) goto zstd_error;
        if (out_buf.pos == output_capacity && (in_buf.pos < in_buf.size || ret != 0)) goto zstd_error;
    } while (in_buf.pos < in_buf.size || (directive != ZSTD_e_continue && ret != 0));

    if (out_buf.pos > (size_t)SSIZE_MAX) goto zstd_error;
    if (flush_mode == FLUSH_END) sc->stream_started = false;
    return (ssize_t)out_buf.pos;

zstd_error:
    sc->errored = true;
    return -1;
}

ssize_t compressionZstdDecompressFeed(streamDecompressor *sd,
                                      uint8_t *output,
                                      size_t output_capacity,
                                      const uint8_t *input,
                                      size_t input_len,
                                      size_t *input_consumed) {
    if (!sd || !sd->ctx || !input_consumed) return -1;

    ZSTD_DCtx *dctx = (ZSTD_DCtx *)sd->ctx;
    uint8_t empty_sentinel = 0;
    ZSTD_inBuffer in_buf = {
        .src = input ? input : &empty_sentinel,
        .size = input_len,
        .pos = 0,
    };
    ZSTD_outBuffer out_buf = {
        .dst = output,
        .size = output_capacity,
        .pos = 0,
    };

    size_t ret = ZSTD_decompressStream(dctx, &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
        sd->errored = true;
        return -1;
    }

    *input_consumed = in_buf.pos;
    sd->input_hint = ret;
    if (ret == 0) sd->frame_done = true;
    if (out_buf.pos > (size_t)SSIZE_MAX) {
        sd->errored = true;
        return -1;
    }
    return (ssize_t)out_buf.pos;
}
