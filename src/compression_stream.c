/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_stream.h"
#include "zmalloc.h"
#include <string.h>

struct stream_writer {
    stream_compressor_t compressor;
    uint8_t *out_buf;    /* Reusable output buffer, sized via streamCompressOutputBound */
    size_t out_buf_size; /* Current allocation size of out_buf */
    vkcsEmitFn emit_cb;  /* Returns 0 on success, -1 on error */
    void *emit_ctx;
    uint8_t stream_kind; /* STREAM_KIND_RDB or STREAM_KIND_REPL */
    int raw_frame;       /* 1 => skip VKCS envelope and emit raw codec frame */
    int envelope_written;
    int finished; /* Set by stream_writer_finish — blocks further writes.
                   * Prevents accidental multi-frame output under one envelope. */
    int errored;  /* Sticky error flag — once set, all writes fail */
};

static int streamWriterValidateConfig(const stream_writer_config_t *cfg) {
    if (!cfg) return -1;
    if (!compressionAlgoSupportsStreaming(cfg->algo)) return -1;
    if (!cfg->raw_frame &&
        cfg->stream_kind != STREAM_KIND_RDB &&
        cfg->stream_kind != STREAM_KIND_REPL) {
        return -1;
    }
    return 0;
}

static int streamWriterInitContext(stream_writer_t *t,
                                   const stream_writer_config_t *cfg,
                                   vkcsEmitFn emit_cb,
                                   void *emit_ctx) {
    if (!t || !emit_cb) return -1;
    if (streamWriterValidateConfig(cfg) != 0) return -1;

    memset(t, 0, sizeof(*t));
    t->emit_cb = emit_cb;
    t->emit_ctx = emit_ctx;
    t->stream_kind = cfg->stream_kind;
    t->raw_frame = cfg->raw_frame != 0;

    if (streamCompressorInit(&t->compressor, cfg->algo, cfg->level) != 0) {
        return -1;
    }
    t->compressor.block_checksum = cfg->block_checksum != 0;
    return 0;
}

/* Emit envelope lazily on first write/flush/finish.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int streamWriterEnsureEnvelope(stream_writer_t *t) {
    if (t->envelope_written) return 0;
    if (t->raw_frame) {
        t->envelope_written = 1;
        return 0;
    }
    if (writeVkcsEnvelope(t->emit_cb, t->emit_ctx, t->compressor.algo, t->stream_kind) != 0) {
        t->errored = 1;
        return -1;
    }
    t->envelope_written = 1;
    return 0;
}

/* Emit compressed bytes to the output sink.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int streamWriterEmit(stream_writer_t *t, const uint8_t *buf, size_t len) {
    if (len == 0) return 0;
    if (t->emit_cb(t->emit_ctx, buf, len) != 0) {
        t->errored = 1;
        return -1;
    }
    return 0;
}

/* Ensure the output buffer is large enough for the given input.
 * Reuses the existing buffer when possible to avoid per-write allocation.
 * zmalloc aborts on OOM, so this cannot fail. */
static void streamWriterEnsureOutBuf(stream_writer_t *t, size_t input_len, compress_flush_mode_t flush_mode) {
    size_t needed = streamCompressOutputBound(t->compressor.algo, input_len,
                                              t->compressor.frame_started, flush_mode);
    if (needed == 0) {
        /* Ensure a minimal valid buffer so streamCompressFeed never gets NULL. */
        if (t->out_buf == NULL) {
            t->out_buf = zmalloc(64);
            t->out_buf_size = 64;
        }
        return;
    }
    if (needed > t->out_buf_size) {
        t->out_buf = zrealloc(t->out_buf, needed);
        t->out_buf_size = needed;
    }
}

/* Compress one chunk with the requested flush mode and emit produced bytes.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int streamWriterFeedAndEmit(stream_writer_t *t,
                                   const uint8_t *input,
                                   size_t input_len,
                                   compress_flush_mode_t flush_mode) {
    streamWriterEnsureOutBuf(t, input_len, flush_mode);

    uint8_t *out_ptr = t->out_buf;
    ssize_t compressed = streamCompressFeed(&t->compressor, &out_ptr,
                                            t->out_buf_size,
                                            input, input_len, flush_mode);
    if (compressed < 0) {
        t->errored = 1;
        return -1;
    }
    return streamWriterEmit(t, out_ptr, (size_t)compressed);
}

/* Release stream compressor state owned by stream_writer_t.
 * Does not free the context object itself. */
static void streamWriterReleaseContext(stream_writer_t *t) {
    if (!t) return;
    streamCompressorDestroy(&t->compressor);
    if (t->out_buf) {
        zfree(t->out_buf);
        t->out_buf = NULL;
    }
    t->out_buf_size = 0;
}

stream_writer_t *stream_writer_create(const stream_writer_config_t *cfg,
                                      vkcsEmitFn emit_cb,
                                      void *emit_ctx) {
    stream_writer_t *t = zmalloc(sizeof(*t));
    if (streamWriterInitContext(t, cfg, emit_cb, emit_ctx) != 0) {
        zfree(t);
        return NULL;
    }
    return t;
}

int stream_writer_write(stream_writer_t *t, const void *buf, size_t len) {
    if (!t) return -1;
    if (t->errored) return -1;
    /* Writes after finish are always a caller bug. Returning an error
     * prevents silent data drops in shared API users (rio/replication). */
    if (t->finished) return -1;
    if (len == 0) return 0;

    if (streamWriterEnsureEnvelope(t) != 0) return -1;
    if (streamWriterFeedAndEmit(t, (const uint8_t *)buf, len, FLUSH_CONTINUE) != 0) return -1;
    return 0;
}

int stream_writer_flush(stream_writer_t *t) {
    if (!t) return -1;
    if (t->errored) return -1;
    /* Flush-after-finish is a harmless no-op: frame is already closed. */
    if (t->finished) return 0;

    if (!t->envelope_written || !t->compressor.frame_started) return 0;
    if (streamWriterFeedAndEmit(t, NULL, 0, FLUSH_SYNC) != 0) return -1;
    return 0;
}

int stream_writer_finish(stream_writer_t *t) {
    if (!t) return -1;
    if (t->errored) return -1;
    if (t->finished) return 0;
    t->finished = 1;

    /* If nothing was ever written, emit envelope + empty frame end. */
    if (streamWriterEnsureEnvelope(t) != 0) return -1;
    if (streamWriterFeedAndEmit(t, NULL, 0, FLUSH_END) != 0) return -1;
    return 0;
}

void stream_writer_destroy(stream_writer_t *t) {
    if (!t) return;
    streamWriterReleaseContext(t);
    zfree(t);
}

int stream_writer_is_errored(const stream_writer_t *t) {
    return t && t->errored;
}

int stream_writer_is_finished(const stream_writer_t *t) {
    return t && t->finished;
}

void stream_writer_set_error(stream_writer_t *t) {
    if (!t) return;
    t->errored = 1;
}
