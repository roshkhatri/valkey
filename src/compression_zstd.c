/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_zstd.h"
#include <limits.h>
#include <zstd.h>

int compressionZstdCompressorInit(stream_compressor_t *sc) {
    ZSTD_CCtx *cctx = NULL;

    if (!sc) return -1;

    cctx = ZSTD_createCCtx();
    if (!cctx) return -1;

    sc->ctx = cctx;
    return 0;
}

void compressionZstdCompressorDestroy(stream_compressor_t *sc) {
    if (!sc || !sc->ctx) return;

    ZSTD_freeCCtx((ZSTD_CCtx *)sc->ctx);
    sc->ctx = NULL;
}

int compressionZstdDecompressorInit(stream_decompressor_t *sd) {
    ZSTD_DCtx *dctx = NULL;

    if (!sd) return -1;

    dctx = ZSTD_createDCtx();
    if (!dctx) return -1;

    sd->ctx = dctx;
    return 0;
}

void compressionZstdDecompressorDestroy(stream_decompressor_t *sd) {
    if (!sd || !sd->ctx) return;

    ZSTD_freeDCtx((ZSTD_DCtx *)sd->ctx);
    sd->ctx = NULL;
}

size_t compressionZstdOutputBound(size_t input_len) {
    /* ZSTD_compressBound gives worst-case compressed size for a single
     * contiguous input. ZSTD_CStreamOutSize() is the recommended output
     * buffer size for streaming and accounts for internal buffering.
     * Take the maximum so that both data writes and flush/end calls
     * (which may need to drain internally buffered data) have enough space. */
    size_t data_bound = ZSTD_compressBound(input_len);
    size_t stream_bound = ZSTD_CStreamOutSize();
    return (data_bound > stream_bound ? data_bound : stream_bound) + 22;
}

ssize_t compressionZstdCompressFeed(stream_compressor_t *sc,
                                    uint8_t *output,
                                    size_t output_capacity,
                                    const uint8_t *input,
                                    size_t input_len,
                                    compress_flush_mode_t flush_mode) {
    ZSTD_CCtx *cctx;
    ZSTD_inBuffer in_buf;
    ZSTD_outBuffer out_buf;
    ZSTD_EndDirective zstd_directive;
    size_t ret;

    if (!sc || !sc->ctx) return -1;

    cctx = (ZSTD_CCtx *)sc->ctx;

    /* Apply parameters on first call (before any data is fed). */
    if (!sc->frame_started) {
        ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, sc->level);
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, sc->codec_checksum ? 1 : 0);
        sc->frame_started = true;
    }

    /* ZSTD_compressStream2 requires a valid input pointer even when
     * input_len is 0 (e.g. for flush/end calls). Use a stack sentinel. */
    uint8_t empty_sentinel;
    in_buf.src = input ? input : &empty_sentinel;
    in_buf.size = input_len;
    in_buf.pos = 0;

    out_buf.dst = output;
    out_buf.size = output_capacity;
    out_buf.pos = 0;

    switch (flush_mode) {
    case FLUSH_CONTINUE: zstd_directive = ZSTD_e_continue; break;
    case FLUSH_SYNC: zstd_directive = ZSTD_e_flush; break;
    case FLUSH_END: zstd_directive = ZSTD_e_end; break;
    default: goto zstd_error;
    }

    ret = ZSTD_compressStream2(cctx, &out_buf, &in_buf, zstd_directive);
    if (ZSTD_isError(ret)) goto zstd_error;

    /* For FLUSH_END, ret==0 means frame is complete. */
    if (flush_mode == FLUSH_END && ret == 0) {
        sc->frame_started = false;
    }

    if (out_buf.pos > (size_t)SSIZE_MAX) goto zstd_error;
    return (ssize_t)out_buf.pos;

zstd_error:
    sc->errored = true;
    return -1;
}

ssize_t compressionZstdDecompressFeed(stream_decompressor_t *sd,
                                      uint8_t *output,
                                      size_t output_capacity,
                                      const uint8_t *input,
                                      size_t input_len,
                                      size_t *input_consumed) {
    ZSTD_DCtx *dctx;
    ZSTD_inBuffer in_buf;
    ZSTD_outBuffer out_buf;
    size_t ret;

    if (!sd || !sd->ctx || !input_consumed) return -1;

    dctx = (ZSTD_DCtx *)sd->ctx;

    in_buf.src = input;
    in_buf.size = input_len;
    in_buf.pos = 0;

    out_buf.dst = output;
    out_buf.size = output_capacity;
    out_buf.pos = 0;

    ret = ZSTD_decompressStream(dctx, &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
        sd->errored = true;
        return -1;
    }

    *input_consumed = in_buf.pos;
    /* ret == 0 means the frame is fully decoded. */
    if (ret == 0) sd->frame_done = true;
    if (out_buf.pos > (size_t)SSIZE_MAX) {
        sd->errored = true;
        return -1;
    }
    return (ssize_t)out_buf.pos;
}
