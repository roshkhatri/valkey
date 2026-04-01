/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_stream.h"
#include "zmalloc.h"
#include <limits.h>
#include <string.h>

/* --- VKCS envelope --- */

/* Write 8-byte VKCS envelope via callback.
 * Shared utility used by both sync (rio decorator) and async (replication)
 * paths. Prevents "sync emits envelope but async forgets it" class of bugs.
 *
 * Layout:
 *   [0..3] magic  "VKCS" (0x56 0x4B 0x43 0x53)
 *   [4]    version (VKCS_VERSION, currently 1)
 *   [5]    algo_id (compression_algo_t value)
 *   [6]    flags   (bit 0 = codec checksum enabled, remaining bits reserved)
 *   [7]    stream_kind (full 8-bit kind)
 *
 * Returns 0 on success, -1 on error (invalid algo or emit_cb failure). */
int writeVkcsEnvelope(vkcsEmitFn emit_cb,
                      void *ctx,
                      compression_algo_t algo,
                      uint8_t stream_kind,
                      bool codec_checksum_enabled) {
    if (!emit_cb) return -1;
    if (!compressionAlgoSupportsStreaming(algo)) return -1;

    uint8_t envelope[VKCS_ENVELOPE_SIZE];
    envelope[0] = VKCS_MAGIC_0;
    envelope[1] = VKCS_MAGIC_1;
    envelope[2] = VKCS_MAGIC_2;
    envelope[3] = VKCS_MAGIC_3;
    envelope[4] = VKCS_VERSION;
    envelope[5] = (uint8_t)algo;
    envelope[6] = codec_checksum_enabled ? VKCS_FLAG_CODEC_CHECKSUM : 0;
    envelope[7] = stream_kind;

    return emit_cb(ctx, envelope, VKCS_ENVELOPE_SIZE) == 0 ? 0 : -1;
}

/* Parse 8-byte VKCS envelope from buffer.
 * Validates magic bytes, version, algorithm, and reserved fields.
 * Rejects envelopes with unknown flag bits so future versions are detected
 * early rather than causing silent data corruption.
 * On success populates *algo and *stream_kind and returns 0.
 * Returns -1 on error (bad magic, unsupported version, unknown algo,
 * reserved bits set). */
int readVkcsEnvelope(const uint8_t *buf,
                     size_t len,
                     compression_algo_t *algo,
                     uint8_t *stream_kind,
                     bool *codec_checksum_enabled) {
    if (!buf || len < VKCS_ENVELOPE_SIZE) return -1;

    if (buf[0] != VKCS_MAGIC_0 || buf[1] != VKCS_MAGIC_1 ||
        buf[2] != VKCS_MAGIC_2 || buf[3] != VKCS_MAGIC_3) {
        return -1;
    }
    if (buf[4] != VKCS_VERSION) return -1;

    uint8_t algo_id = buf[5];
    if (!compressionAlgoSupportsStreaming((compression_algo_t)algo_id)) return -1;

    uint8_t flags = buf[6];
    if (flags & ~VKCS_FLAG_CODEC_CHECKSUM) return -1;

    if (algo) *algo = (compression_algo_t)algo_id;
    if (stream_kind) *stream_kind = buf[7];
    if (codec_checksum_enabled) *codec_checksum_enabled = (flags & VKCS_FLAG_CODEC_CHECKSUM) != 0;
    return 0;
}

/* Generic streaming writer implementation. */

struct stream_writer {
    stream_compressor_t compressor;
    uint8_t *out_buf;    /* Reusable output buffer, sized via streamCompressOutputBound */
    size_t out_buf_size; /* Current allocation size of out_buf */
    vkcsEmitFn emit_cb;  /* Returns 0 on success, -1 on error */
    void *emit_ctx;
    uint8_t stream_kind; /* Concrete on-wire stream kind */
    bool raw_frame;      /* true => skip VKCS envelope and emit raw codec frame */
    bool envelope_written;
    bool finished;          /* Set by stream_writer_finish — blocks further writes.
                             * Prevents accidental multi-frame output under one envelope. */
    bool errored;           /* Sticky error flag — once set, all writes fail */
    uint64_t bytes_emitted; /* Running total of bytes successfully emitted */
};

static int streamWriterValidateConfig(const stream_writer_config_t *cfg) {
    if (!cfg) return -1;
    if (!compressionAlgoSupportsStreaming(cfg->algo)) return -1;
    if (cfg->block_mode != COMPRESS_BLOCK_INDEPENDENT &&
        cfg->block_mode != COMPRESS_BLOCK_LINKED) {
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
    t->raw_frame = cfg->raw_frame;

    if (streamCompressorInit(&t->compressor, cfg->algo, cfg->level) != 0) {
        return -1;
    }
    t->compressor.block_mode = cfg->block_mode;
    t->compressor.block_checksum = cfg->block_checksum;
    t->compressor.stable_src = cfg->stable_src;
    return 0;
}

/* Emit envelope lazily on first write/flush/finish.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int streamWriterEnsureEnvelope(stream_writer_t *t) {
    if (t->envelope_written) return 0;
    if (t->raw_frame) {
        t->envelope_written = true;
        return 0;
    }
    if (writeVkcsEnvelope(t->emit_cb, t->emit_ctx, t->compressor.algo,
                          t->stream_kind, t->compressor.block_checksum) != 0) {
        t->errored = true;
        return -1;
    }
    t->bytes_emitted += VKCS_ENVELOPE_SIZE;
    t->envelope_written = true;
    return 0;
}

/* Emit compressed bytes to the output sink.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int streamWriterEmit(stream_writer_t *t, const uint8_t *buf, size_t len) {
    if (len == 0) return 0;
    if (t->emit_cb(t->emit_ctx, buf, len) != 0) {
        t->errored = true;
        return -1;
    }
    t->bytes_emitted += len;
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

    ssize_t compressed = streamCompressFeed(&t->compressor, t->out_buf,
                                            t->out_buf_size,
                                            input, input_len, flush_mode);
    if (compressed < 0) {
        t->errored = true;
        return -1;
    }
    return streamWriterEmit(t, t->out_buf, (size_t)compressed);
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

ssize_t stream_writer_write(stream_writer_t *t, const void *buf, size_t len) {
    if (!t) return -1;
    if (t->errored) return -1;
    /* Writes after finish are always a caller bug. Returning an error
     * prevents silent data drops in shared API users (rio/replication). */
    if (t->finished) return -1;
    if (len == 0) return 0;

    uint64_t emitted_before = t->bytes_emitted;
    if (streamWriterEnsureEnvelope(t) != 0) return -1;
    if (streamWriterFeedAndEmit(t, (const uint8_t *)buf, len, FLUSH_CONTINUE) != 0) return -1;
    uint64_t emitted_delta = t->bytes_emitted - emitted_before;
    if (emitted_delta > (uint64_t)SSIZE_MAX) {
        t->errored = true;
        return -1;
    }
    return (ssize_t)emitted_delta;
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
    t->finished = true;

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

uint64_t stream_writer_bytes_emitted(const stream_writer_t *t) {
    if (!t) return 0;
    return t->bytes_emitted;
}

int stream_writer_is_errored(const stream_writer_t *t) {
    return t && t->errored;
}

void stream_writer_set_error(stream_writer_t *t) {
    if (!t) return;
    t->errored = true;
}

struct stream_reader {
    stream_reader_read_fn read_cb; /* Returns >0 bytes, 0 EOF, -1 error */
    void *read_ctx;

    bool raw_frame;         /* Input is raw codec frame, no VKCS envelope */
    bool allow_passthrough; /* Non-VKCS input is copied through unchanged */
    uint8_t expected_stream_kind;
    size_t batch_size;

    bool probed;
    bool compressed; /* 1 => codec decompression path, 0 => passthrough */
    bool errored;

    compression_algo_t algo;
    bool codec_checksum_enabled;
    uint8_t stream_kind;

    stream_decompressor_t decompressor;
    bool decompressor_initialized;

    uint8_t prefix[VKCS_ENVELOPE_SIZE]; /* Buffered bytes for passthrough mode */
    size_t prefix_len;
    size_t prefix_pos;

    uint8_t *read_buf; /* Buffered compressed input */
    size_t read_buf_size;
    size_t read_buf_pos;
    size_t read_buf_fill;

    uint8_t *window_buf; /* Decode window served by memcpy */
    size_t window_size;
    size_t window_pos;
    size_t window_len;
};

static int streamReaderValidateConfig(const stream_reader_config_t *cfg) {
    if (!cfg) return -1;

    if (cfg->raw_frame) {
        if (!compressionAlgoSupportsStreaming(cfg->algo)) return -1;
    }
    return 0;
}

static void streamReaderSetError(stream_reader_t *t) {
    if (!t) return;
    t->errored = true;
}

/* Preserve partial output on read errors while latching sticky error state. */
static ssize_t streamReaderFail(stream_reader_t *t, size_t partial_bytes) {
    streamReaderSetError(t);
    return partial_bytes > 0 ? (ssize_t)partial_bytes : -1;
}

static int streamReaderInitCompressedState(stream_reader_t *t,
                                           compression_algo_t algo,
                                           bool codec_checksum_enabled,
                                           uint8_t stream_kind,
                                           size_t batch_size) {
    if (!t) return -1;
    if (!compressionAlgoSupportsStreaming(algo)) return -1;

    if (streamDecompressorInit(&t->decompressor, algo) != 0) {
        return -1;
    }
    t->decompressor_initialized = true;

    t->read_buf = zmalloc(batch_size);
    t->window_buf = zmalloc(batch_size);
    t->read_buf_size = batch_size;
    t->window_size = batch_size;
    t->read_buf_pos = 0;
    t->read_buf_fill = 0;
    t->window_pos = 0;
    t->window_len = 0;

    t->algo = algo;
    t->codec_checksum_enabled = codec_checksum_enabled;
    t->stream_kind = stream_kind;
    t->compressed = true;
    return 0;
}

static void streamReaderResetCompressedState(stream_reader_t *t) {
    if (!t) return;

    if (t->decompressor_initialized) {
        streamDecompressorDestroy(&t->decompressor);
        t->decompressor_initialized = false;
    }
    if (t->read_buf) {
        zfree(t->read_buf);
        t->read_buf = NULL;
    }
    if (t->window_buf) {
        zfree(t->window_buf);
        t->window_buf = NULL;
    }
    t->read_buf_size = 0;
    t->read_buf_pos = 0;
    t->read_buf_fill = 0;
    t->window_size = 0;
    t->window_pos = 0;
    t->window_len = 0;
}

stream_reader_t *stream_reader_create(const stream_reader_config_t *cfg,
                                      stream_reader_read_fn read_cb,
                                      void *read_ctx) {
    if (!read_cb) return NULL;
    if (streamReaderValidateConfig(cfg) != 0) return NULL;

    stream_reader_t *t = zmalloc(sizeof(*t));
    memset(t, 0, sizeof(*t));
    t->read_cb = read_cb;
    t->read_ctx = read_ctx;
    t->raw_frame = cfg->raw_frame;
    t->allow_passthrough = cfg->allow_passthrough;
    t->expected_stream_kind = cfg->expected_stream_kind;
    if (t->raw_frame) t->allow_passthrough = false;
    t->batch_size = cfg->batch_size ? cfg->batch_size : STREAM_READER_BATCH_SIZE_DEFAULT;

    if (t->raw_frame) {
        if (streamReaderInitCompressedState(t, cfg->algo, false,
                                            t->expected_stream_kind, t->batch_size) != 0) {
            streamReaderResetCompressedState(t);
            zfree(t);
            return NULL;
        }
        t->probed = true;
    }

    return t;
}

static int streamReaderReadProbeHeader(stream_reader_t *t, uint8_t *header, size_t *header_len) {
    size_t total = 0;
    while (total < VKCS_ENVELOPE_SIZE) {
        ssize_t got = t->read_cb(t->read_ctx, header + total, VKCS_ENVELOPE_SIZE - total);
        if (got < 0) return -1;
        if (got == 0) break;
        if ((size_t)got > VKCS_ENVELOPE_SIZE - total) return -1;
        total += (size_t)got;
    }
    *header_len = total;
    return 0;
}

static void streamReaderInitPassthroughState(stream_reader_t *t,
                                             const uint8_t *prefix,
                                             size_t prefix_len) {
    if (prefix_len > 0) memcpy(t->prefix, prefix, prefix_len);
    t->prefix_len = prefix_len;
    t->prefix_pos = 0;
    t->compressed = false;
    t->algo = ALGO_NONE;
    t->codec_checksum_enabled = false;
    t->stream_kind = 0;
    t->probed = true;
}

int stream_reader_probe(stream_reader_t *t) {
    if (!t) return -1;
    if (t->errored) return -1;
    if (t->probed) return 0;

    uint8_t header[VKCS_ENVELOPE_SIZE];
    size_t header_len = 0;
    if (streamReaderReadProbeHeader(t, header, &header_len) != 0) {
        streamReaderSetError(t);
        return -1;
    }

    if (header_len < VKCS_ENVELOPE_SIZE) {
        /* Short stream: preserve what was read so caller can consume it. */
        if (!t->allow_passthrough) {
            streamReaderSetError(t);
            return -1;
        }
        streamReaderInitPassthroughState(t, header, header_len);
        return 0;
    }

    int has_vkcs_magic = header[0] == VKCS_MAGIC_0 &&
                         header[1] == VKCS_MAGIC_1 &&
                         header[2] == VKCS_MAGIC_2 &&
                         header[3] == VKCS_MAGIC_3;
    if (!has_vkcs_magic) {
        if (!t->allow_passthrough) {
            streamReaderSetError(t);
            return -1;
        }
        streamReaderInitPassthroughState(t, header, VKCS_ENVELOPE_SIZE);
        return 0;
    }

    compression_algo_t algo = ALGO_NONE;
    bool codec_checksum_enabled = false;
    uint8_t stream_kind = 0;
    if (readVkcsEnvelope(header, VKCS_ENVELOPE_SIZE, &algo, &stream_kind,
                         &codec_checksum_enabled) != 0) {
        streamReaderSetError(t);
        return -1;
    }
    if (stream_kind != t->expected_stream_kind) {
        streamReaderSetError(t);
        return -1;
    }

    if (streamReaderInitCompressedState(t, algo, codec_checksum_enabled,
                                        stream_kind, t->batch_size) != 0) {
        streamReaderSetError(t);
        return -1;
    }
    t->probed = true;
    return 0;
}

static size_t streamReaderPrefixAvail(const stream_reader_t *t) {
    if (!t || t->prefix_len <= t->prefix_pos) return 0;
    return t->prefix_len - t->prefix_pos;
}

/* Returns produced bytes (>=0). If *read_error is set, the caller should
 * latch sticky error state after returning any partial bytes. */
static ssize_t streamReaderReadPassthrough(stream_reader_t *t,
                                           uint8_t *dst,
                                           size_t len,
                                           bool *read_error) {
    size_t total = 0;
    if (read_error) *read_error = false;

    size_t prefix_avail = streamReaderPrefixAvail(t);
    if (prefix_avail > 0) {
        size_t from_prefix = prefix_avail < len ? prefix_avail : len;
        memcpy(dst, t->prefix + t->prefix_pos, from_prefix);
        t->prefix_pos += from_prefix;
        dst += from_prefix;
        len -= from_prefix;
        total += from_prefix;
    }

    if (len == 0) {
        if (total > (size_t)SSIZE_MAX) return -1;
        return (ssize_t)total;
    }

    ssize_t got = t->read_cb(t->read_ctx, dst, len);
    if (got < 0) {
        if (read_error) *read_error = true;
        if (total > (size_t)SSIZE_MAX) return -1;
        return (ssize_t)total;
    }
    if (got == 0) {
        if (total > (size_t)SSIZE_MAX) return -1;
        return (ssize_t)total;
    }
    if ((size_t)got > len) return -1;
    if (total + (size_t)got > (size_t)SSIZE_MAX) return -1;
    return (ssize_t)(total + (size_t)got);
}

static int streamReaderDrainReadBuf(stream_reader_t *t,
                                    uint8_t *out,
                                    size_t out_size,
                                    size_t *out_written) {
    *out_written = 0;
    while (t->read_buf_fill > 0 && *out_written < out_size) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &t->decompressor,
            out + *out_written, out_size - *out_written,
            t->read_buf + t->read_buf_pos,
            t->read_buf_fill, &consumed);
        if (produced < 0) return -1;
        *out_written += (size_t)produced;
        t->read_buf_pos += consumed;
        t->read_buf_fill -= consumed;
        if (consumed == 0 && produced == 0) break;
    }
    if (t->read_buf_fill == 0) t->read_buf_pos = 0;
    return 0;
}

static size_t streamReaderReadBufTailSpace(stream_reader_t *t) {
    size_t tail_space = t->read_buf_size - t->read_buf_pos - t->read_buf_fill;
    if (tail_space > 0) return tail_space;

    if (t->read_buf_fill == 0) {
        t->read_buf_pos = 0;
        return t->read_buf_size;
    }

    if (t->read_buf_pos > 0) {
        memmove(t->read_buf, t->read_buf + t->read_buf_pos, t->read_buf_fill);
        t->read_buf_pos = 0;
        return t->read_buf_size - t->read_buf_fill;
    }

    return 0;
}

static int streamReaderPump(stream_reader_t *t,
                            uint8_t *out,
                            size_t out_size,
                            size_t *out_written) {
    *out_written = 0;

    while (*out_written < out_size) {
        if (t->read_buf_fill > 0) {
            size_t written = 0;
            if (streamReaderDrainReadBuf(t, out + *out_written,
                                         out_size - *out_written, &written) != 0)
                return -1;
            *out_written += written;
            if (*out_written >= out_size) return 0;
        }

        /* Stop reading from source when the codec frame is fully decoded.
         * Remaining bytes in read_buf (if any) belong to data after the
         * compressed stream and must not be fed to the decompressor. */
        if (t->decompressor.frame_done) break;

        size_t read_size = streamReaderReadBufTailSpace(t);
        if (read_size == 0) return -1;

        ssize_t got = t->read_cb(
            t->read_ctx,
            t->read_buf + t->read_buf_pos + t->read_buf_fill,
            read_size);
        if (got < 0) return -1;
        if (got == 0) {
            /* Reaching EOF before the codec reports frame completion means
             * the compressed stream was truncated. */
            return t->decompressor.frame_done ? 0 : -1;
        }
        if ((size_t)got > read_size) return -1;
        t->read_buf_fill += (size_t)got;
    }

    return 0;
}

static ssize_t streamReaderFillWindow(stream_reader_t *t) {
    t->window_pos = 0;
    t->window_len = 0;

    size_t written = 0;
    if (streamReaderPump(t, t->window_buf, t->window_size, &written) != 0)
        return -1;
    t->window_len = written;
    return (ssize_t)written;
}

static inline size_t streamReaderWindowAvail(const stream_reader_t *t) {
    if (!t || t->window_len <= t->window_pos) return 0;
    return t->window_len - t->window_pos;
}

static size_t streamReaderCopyFromWindow(stream_reader_t *t,
                                         uint8_t **dst,
                                         size_t *remaining) {
    size_t avail = streamReaderWindowAvail(t);
    if (avail == 0 || *remaining == 0) return 0;

    size_t to_copy = avail < *remaining ? avail : *remaining;
    memcpy(*dst, t->window_buf + t->window_pos, to_copy);
    t->window_pos += to_copy;
    *dst += to_copy;
    *remaining -= to_copy;
    return to_copy;
}

static ssize_t streamReaderReadCompressed(stream_reader_t *t, uint8_t *dst, size_t len) {
    size_t remaining = len;
    size_t total = 0;

    total += streamReaderCopyFromWindow(t, &dst, &remaining);
    while (remaining > 0) {
        if (streamReaderWindowAvail(t) == 0 &&
            t->window_size > 0 &&
            remaining >= t->window_size) {
            size_t direct_written = 0;
            if (streamReaderPump(t, dst, remaining, &direct_written) != 0) {
                total += direct_written;
                return streamReaderFail(t, total);
            }
            if (direct_written == 0) break; /* EOF */
            dst += direct_written;
            remaining -= direct_written;
            total += direct_written;
            continue;
        }

        if (streamReaderWindowAvail(t) == 0) {
            ssize_t filled = streamReaderFillWindow(t);
            if (filled < 0) {
                return streamReaderFail(t, total);
            }
            if (filled == 0) break; /* EOF */
        }

        total += streamReaderCopyFromWindow(t, &dst, &remaining);
    }

    return (ssize_t)total;
}

ssize_t stream_reader_read(stream_reader_t *t, void *buf, size_t len) {
    if (!t || !buf) return -1;
    if (t->errored) return -1;
    if (len == 0) return 0;

    if (stream_reader_probe(t) != 0) return -1;

    ssize_t nread;
    if (!t->compressed) {
        bool read_error = false;
        nread = streamReaderReadPassthrough(t, (uint8_t *)buf, len, &read_error);
        if (nread < 0) {
            return streamReaderFail(t, 0);
        }
        if (read_error) {
            return streamReaderFail(t, (size_t)nread);
        }
    } else {
        nread = streamReaderReadCompressed(t, (uint8_t *)buf, len);
        if (nread < 0) return streamReaderFail(t, 0);
    }

    return nread;
}

int stream_reader_get_info(stream_reader_t *t, stream_reader_info_t *info) {
    if (!t || !info) return -1;
    if (stream_reader_probe(t) != 0) return -1;

    info->compressed = t->compressed;
    info->algo = t->compressed ? t->algo : ALGO_NONE;
    info->codec_checksum_enabled = t->compressed ? t->codec_checksum_enabled : false;
    info->stream_kind = t->stream_kind;
    return 0;
}

int stream_reader_finish(stream_reader_t *t) {
    uint8_t discard[256];

    if (!t) return -1;
    if (stream_reader_probe(t) != 0) return -1;
    if (!t->compressed) return 0;

    /* Callers invoke this only when they are done consuming the logical
     * payload. Drop any decoded bytes still buffered so we can continue
     * draining the compressed frame to its true boundary. */
    t->window_pos = t->window_len;

    while (!t->decompressor.frame_done) {
        size_t discarded = 0;
        if (streamReaderPump(t, discard, sizeof(discard), &discarded) != 0) return -1;
        if (discarded == 0 && !t->decompressor.frame_done) return -1;
    }
    return 0;
}

int stream_reader_get_pending_input(stream_reader_t *t, const uint8_t **buf, size_t *len) {
    if (!t || !buf || !len) return -1;
    if (stream_reader_probe(t) != 0) return -1;

    *buf = NULL;
    *len = 0;

    if (!t->compressed) {
        size_t prefix_avail = streamReaderPrefixAvail(t);
        if (prefix_avail > 0) {
            *buf = t->prefix + t->prefix_pos;
            *len = prefix_avail;
        }
        return 0;
    }

    if (t->read_buf_fill > 0) {
        *buf = t->read_buf + t->read_buf_pos;
        *len = t->read_buf_fill;
    }
    return 0;
}

void stream_reader_destroy(stream_reader_t *t) {
    if (!t) return;
    streamReaderResetCompressedState(t);
    zfree(t);
}
