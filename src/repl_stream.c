/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "server.h"
#include "compression_stream.h"
#include "repl_stream.h"
#include "zmalloc.h"
#include <string.h>

#define REPL_STREAM_DECODER_INPUT_MAX (64 * 1024 * 1024)

typedef enum {
    REPL_STREAM_MODE_PROBING = 0,
    REPL_STREAM_MODE_PASSTHROUGH,
    REPL_STREAM_MODE_COMPRESSED,
} repl_stream_mode_t;

struct repl_stream_decoder {
    repl_stream_mode_t mode;
    uint8_t header[VKCS_ENVELOPE_SIZE];
    size_t header_len;

    stream_decompressor_t decompressor;
    bool decompressor_initialized;

    sds input_buf;
    size_t input_pos;
};

static void replStreamDecoderCompactInput(repl_stream_decoder_t *decoder) {
    size_t input_len;

    if (!decoder || decoder->input_pos == 0 || !decoder->input_buf) return;

    input_len = sdslen(decoder->input_buf);
    if (decoder->input_pos >= input_len) {
        sdsclear(decoder->input_buf);
        decoder->input_pos = 0;
        return;
    }

    /* Preserve the remaining compressed suffix while avoiding per-feed memmove
     * churn when only a tiny prefix has been consumed. */
    if (decoder->input_pos < PROTO_IOBUF_LEN &&
        decoder->input_pos * 2 < input_len) {
        return;
    }

    sdsrange(decoder->input_buf, decoder->input_pos, -1);
    decoder->input_pos = 0;
}

static int replStreamDecoderSetCompressed(repl_stream_decoder_t *decoder,
                                          compression_algo_t algo) {
    if (!decoder) return C_ERR;
    if (!compressionAlgoSupportsStreaming(algo)) return C_ERR;
    if (streamDecompressorInit(&decoder->decompressor, algo) != 0) return C_ERR;

    decoder->decompressor_initialized = true;
    decoder->mode = REPL_STREAM_MODE_COMPRESSED;
    decoder->header_len = 0;
    return C_OK;
}

static int replStreamDecoderProbe(repl_stream_decoder_t *decoder,
                                  const uint8_t **src,
                                  size_t *remaining,
                                  sds *dst) {
    while (decoder->mode == REPL_STREAM_MODE_PROBING && *remaining > 0) {
        size_t target = decoder->header_len < 4 ? 4 : VKCS_ENVELOPE_SIZE;
        size_t needed = target - decoder->header_len;
        size_t take = *remaining < needed ? *remaining : needed;

        memcpy(decoder->header + decoder->header_len, *src, take);
        decoder->header_len += take;
        *src += take;
        *remaining -= take;

        if (decoder->header_len >= 4 &&
            memcmp(decoder->header, "VKCS", 4) != 0) {
            decoder->mode = REPL_STREAM_MODE_PASSTHROUGH;
            *dst = sdscatlen(*dst, decoder->header, decoder->header_len);
            decoder->header_len = 0;
            break;
        }

        if (decoder->header_len == VKCS_ENVELOPE_SIZE) {
            vkcs_codec_t codec = 0;
            uint8_t stream_kind = 0;
            bool codec_checksum_enabled = false;

            if (read_vkcs_envelope(decoder->header, VKCS_ENVELOPE_SIZE,
                                   &codec, &stream_kind,
                                   &codec_checksum_enabled) != 0 ||
                stream_kind != STREAM_KIND_REPL) {
                return C_ERR;
            }
            compression_algo_t algo;
            switch (codec) {
            case VKCS_CODEC_LZ4: algo = ALGO_LZ4; break;
            case VKCS_CODEC_ZSTD: algo = ALGO_ZSTD; break;
            default: return C_ERR;
            }
            if (replStreamDecoderSetCompressed(decoder, algo) != C_OK) {
                return C_ERR;
            }
            (void)codec_checksum_enabled;
        }
    }

    return C_OK;
}

static int replStreamDecoderDrainCompressed(repl_stream_decoder_t *decoder, sds *dst) {
    char outbuf[PROTO_IOBUF_LEN];

    while (decoder->input_pos < sdslen(decoder->input_buf)) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(&decoder->decompressor,
                                                (uint8_t *)outbuf, sizeof(outbuf),
                                                (const uint8_t *)decoder->input_buf + decoder->input_pos,
                                                sdslen(decoder->input_buf) - decoder->input_pos,
                                                &consumed);
        if (produced < 0) return C_ERR;

        decoder->input_pos += consumed;
        if (produced > 0) *dst = sdscatlen(*dst, outbuf, (size_t)produced);

        if (decoder->decompressor.frame_done &&
            decoder->input_pos < sdslen(decoder->input_buf)) {
            return C_ERR;
        }
        if (consumed == 0 && produced == 0) break;
    }

    replStreamDecoderCompactInput(decoder);
    return C_OK;
}

repl_stream_decoder_t *replStreamDecoderCreate(void) {
    repl_stream_decoder_t *decoder = zmalloc(sizeof(*decoder));

    memset(decoder, 0, sizeof(*decoder));
    decoder->mode = REPL_STREAM_MODE_PROBING;
    decoder->input_buf = sdsempty();
    return decoder;
}

int replStreamDecoderFeed(repl_stream_decoder_t *decoder, const void *src, size_t len, sds *dst) {
    const uint8_t *input = src;
    size_t remaining = len;

    if (!decoder || !dst) return C_ERR;
    if (len > 0 && !src) return C_ERR;
    if (decoder->mode == REPL_STREAM_MODE_COMPRESSED &&
        decoder->decompressor.frame_done && len > 0) {
        return C_ERR;
    }

    if (decoder->mode == REPL_STREAM_MODE_PROBING &&
        replStreamDecoderProbe(decoder, &input, &remaining, dst) != C_OK) {
        return C_ERR;
    }

    if (decoder->mode == REPL_STREAM_MODE_PASSTHROUGH) {
        if (remaining > 0) *dst = sdscatlen(*dst, input, remaining);
        return C_OK;
    }

    if (decoder->mode != REPL_STREAM_MODE_COMPRESSED) {
        return C_OK;
    }

    if (remaining > 0) decoder->input_buf = sdscatlen(decoder->input_buf, input, remaining);
    if (sdslen(decoder->input_buf) > REPL_STREAM_DECODER_INPUT_MAX) return C_ERR;
    return replStreamDecoderDrainCompressed(decoder, dst);
}

void replStreamDecoderDestroy(repl_stream_decoder_t *decoder) {
    if (!decoder) return;

    if (decoder->decompressor_initialized) {
        streamDecompressorDestroy(&decoder->decompressor);
    }
    sdsfree(decoder->input_buf);
    zfree(decoder);
}
