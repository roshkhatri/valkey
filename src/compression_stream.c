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

typedef enum {
    VKCS_PROBE_NEED_INPUT = 0,
    VKCS_PROBE_PASSTHROUGH = 1,
    VKCS_PROBE_COMPRESSED = 2,
    VKCS_PROBE_ERROR = 3,
} vkcs_probe_result_t;

/* Probe options. */
typedef struct {
    bool allow_passthrough;
    uint8_t expected_stream_kind;
} vkcs_probe_config_t;

/* Probe state. */
typedef struct {
    uint8_t header[VKCS_ENVELOPE_SIZE];
    size_t header_len;
    bool ready;
    bool compressed;
    bool codec_checksum_enabled;
    compression_algo_t algo;
    uint8_t stream_kind;
} vkcs_probe_t;

/* Write 8-byte VKCS envelope via callback.
 * Layout:
 *   [0..3] magic  "VKCS" (0x56 0x4B 0x43 0x53)
 *   [4]    version (VKCS_VERSION, currently 1)
 *   [5]    codec_id (VKCS codec registry)
 *   [6]    flags   (bit 0 = codec checksum enabled, remaining bits reserved)
 *   [7]    stream_kind (full 8-bit kind)
 *
 * Returns 0 on success, -1 on error (invalid codec or emit_cb failure). */
static bool vkcsCodecIsSupported(vkcs_codec_t codec) {
    return codec == VKCS_CODEC_LZ4;
}

static bool vkcsProbeHasMagicPrefix(const vkcs_probe_t *probe) {
    if (probe->header_len == 0) return false;
    size_t magic_prefix_len = probe->header_len < 4 ? probe->header_len : 4;
    return memcmp(probe->header, "VKCS", magic_prefix_len) == 0;
}

static void vkcsProbeSetPassthrough(vkcs_probe_t *probe) {
    probe->ready = true;
    probe->compressed = false;
    probe->codec_checksum_enabled = false;
    probe->algo = ALGO_NONE;
    probe->stream_kind = 0;
}

static void vkcsProbeSetCompressed(vkcs_probe_t *probe,
                                   compression_algo_t algo,
                                   uint8_t stream_kind,
                                   bool codec_checksum_enabled) {
    probe->ready = true;
    probe->compressed = true;
    probe->codec_checksum_enabled = codec_checksum_enabled;
    probe->algo = algo;
    probe->stream_kind = stream_kind;
}

static int compressionAlgoToVkcsCodec(compression_algo_t algo, vkcs_codec_t *codec) {
    switch (algo) {
    case ALGO_LZ4:
        *codec = VKCS_CODEC_LZ4;
        return 0;
    default:
        return -1;
    }
}

static int vkcsCodecToCompressionAlgo(vkcs_codec_t codec, compression_algo_t *algo) {
    switch (codec) {
    case VKCS_CODEC_LZ4:
        *algo = ALGO_LZ4;
        return 0;
    default:
        return -1;
    }
}

int write_vkcs_envelope(vkcs_emit_fn emit_cb,
                        void *ctx,
                        vkcs_codec_t codec,
                        uint8_t stream_kind,
                        bool codec_checksum_enabled) {
    if (!emit_cb) return -1;
    if (!vkcsCodecIsSupported(codec)) return -1;

    uint8_t envelope[VKCS_ENVELOPE_SIZE];
    envelope[0] = VKCS_MAGIC_0;
    envelope[1] = VKCS_MAGIC_1;
    envelope[2] = VKCS_MAGIC_2;
    envelope[3] = VKCS_MAGIC_3;
    envelope[4] = VKCS_VERSION;
    envelope[5] = (uint8_t)codec;
    envelope[6] = codec_checksum_enabled ? VKCS_FLAG_CODEC_CHECKSUM : 0;
    envelope[7] = stream_kind;

    return emit_cb(ctx, envelope, VKCS_ENVELOPE_SIZE) == 0 ? 0 : -1;
}

/* Parse 8-byte VKCS envelope from buffer.
 * Validates magic bytes, version, codec, and reserved fields.
 * Rejects envelopes with unknown flag bits so future versions are detected
 * early rather than causing silent data corruption.
 * On success populates *codec and *stream_kind and returns 0.
 * Returns -1 on error (bad magic, unsupported version, unknown codec,
 * reserved bits set). */
int read_vkcs_envelope(const uint8_t *buf,
                       size_t len,
                       vkcs_codec_t *codec,
                       uint8_t *stream_kind,
                       bool *codec_checksum_enabled) {
    if (!buf || len < VKCS_ENVELOPE_SIZE) return -1;

    if (buf[0] != VKCS_MAGIC_0 || buf[1] != VKCS_MAGIC_1 ||
        buf[2] != VKCS_MAGIC_2 || buf[3] != VKCS_MAGIC_3) {
        return -1;
    }
    if (buf[4] != VKCS_VERSION) return -1;

    vkcs_codec_t parsed_codec = (vkcs_codec_t)buf[5];
    if (!vkcsCodecIsSupported(parsed_codec)) return -1;

    uint8_t flags = buf[6];
    if (flags & ~VKCS_FLAG_CODEC_CHECKSUM) return -1;

    if (codec) *codec = parsed_codec;
    if (stream_kind) *stream_kind = buf[7];
    if (codec_checksum_enabled) *codec_checksum_enabled = (flags & VKCS_FLAG_CODEC_CHECKSUM) != 0;
    return 0;
}

static int readVkcsEnvelopeInfo(const uint8_t *buf,
                                uint8_t expected_stream_kind,
                                stream_reader_info_t *info) {
    vkcs_codec_t codec;
    uint8_t stream_kind = 0;
    bool codec_checksum_enabled = false;
    compression_algo_t algo = ALGO_NONE;

    if (read_vkcs_envelope(buf, VKCS_ENVELOPE_SIZE,
                           &codec, &stream_kind, &codec_checksum_enabled) != 0 ||
        stream_kind != expected_stream_kind ||
        vkcsCodecToCompressionAlgo(codec, &algo) != 0) {
        return -1;
    }

    info->compressed = true;
    info->codec_checksum_enabled = codec_checksum_enabled;
    info->algo = algo;
    info->stream_kind = stream_kind;
    return 0;
}

static void vkcsProbeInit(vkcs_probe_t *probe) {
    memset(probe, 0, sizeof(*probe));
    probe->algo = ALGO_NONE;
    probe->codec_checksum_enabled = false;
}

/* Probe incrementally because wrapped rios may legally return fewer than
 * VKCS_ENVELOPE_SIZE bytes per read. The probe also retains any consumed
 * prefix so passthrough streams can replay those bytes exactly. */
static vkcs_probe_result_t vkcsProbeFeed(vkcs_probe_t *probe,
                                         const vkcs_probe_config_t *cfg,
                                         const uint8_t *src,
                                         size_t src_len,
                                         bool input_eof,
                                         size_t *src_consumed) {
    size_t consumed = 0;

    *src_consumed = 0;
    if (probe->ready) {
        return probe->compressed ? VKCS_PROBE_COMPRESSED : VKCS_PROBE_PASSTHROUGH;
    }

    while (consumed < src_len) {
        size_t target = probe->header_len < 4 ? 4 : VKCS_ENVELOPE_SIZE;
        size_t need = target - probe->header_len;
        size_t take = src_len - consumed < need ? src_len - consumed : need;

        memcpy(probe->header + probe->header_len, src + consumed, take);
        probe->header_len += take;
        consumed += take;

        if (probe->header_len >= 4 &&
            memcmp(probe->header, "VKCS", 4) != 0) {
            *src_consumed = consumed;
            if (!cfg->allow_passthrough) return VKCS_PROBE_ERROR;
            vkcsProbeSetPassthrough(probe);
            return VKCS_PROBE_PASSTHROUGH;
        }

        if (probe->header_len == VKCS_ENVELOPE_SIZE) {
            stream_reader_info_t info = {0};

            if (readVkcsEnvelopeInfo(probe->header, cfg->expected_stream_kind, &info) != 0) {
                *src_consumed = consumed;
                return VKCS_PROBE_ERROR;
            }

            vkcsProbeSetCompressed(probe, info.algo, info.stream_kind,
                                   info.codec_checksum_enabled);
            *src_consumed = consumed;
            return VKCS_PROBE_COMPRESSED;
        }
    }

    if (input_eof) {
        *src_consumed = consumed;
        /* If EOF lands in the middle of a potential VKCS header, treat it as
         * a malformed compressed stream rather than silently downgrading it to
         * passthrough mode. */
        if (vkcsProbeHasMagicPrefix(probe)) return VKCS_PROBE_ERROR;
        if (!cfg->allow_passthrough) return VKCS_PROBE_ERROR;
        vkcsProbeSetPassthrough(probe);
        return VKCS_PROBE_PASSTHROUGH;
    }

    *src_consumed = consumed;
    return VKCS_PROBE_NEED_INPUT;
}

/* Generic streaming writer implementation. */

#define STREAM_WRITER_INPUT_CHUNK_SIZE (1024 * 1024)

/* Streaming writer context. */
struct stream_writer {
    stream_compressor_t compressor;
    uint8_t *out_buf;     /* Reusable output buffer, sized via streamCompressOutputBound */
    size_t out_buf_size;  /* Current allocation size of out_buf */
    vkcs_emit_fn emit_cb; /* Returns 0 on success, -1 on error */
    void *emit_ctx;
    uint8_t stream_kind; /* Concrete on-wire stream kind */
    bool envelope_written;
    bool finished;          /* Set by stream_writer_finish — blocks further writes.
                             * Prevents accidental multi-frame output under one envelope. */
    bool errored;           /* Sticky error flag — once set, all writes fail */
    uint64_t bytes_emitted; /* Running total of bytes successfully emitted */
};

static int streamWriterInitContext(stream_writer_t *t,
                                   const stream_writer_config_t *cfg,
                                   vkcs_emit_fn emit_cb,
                                   void *emit_ctx) {
    memset(t, 0, sizeof(*t));
    t->emit_cb = emit_cb;
    t->emit_ctx = emit_ctx;
    t->stream_kind = cfg->stream_kind;

    if (streamCompressorInit(&t->compressor, cfg->algo, cfg->level) != 0) {
        return -1;
    }
    t->compressor.codec_checksum = cfg->codec_checksum;
    return 0;
}

/* Emit envelope lazily on first write/flush/finish.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int streamWriterEnsureEnvelope(stream_writer_t *t) {
    if (t->envelope_written) return 0;
    vkcs_codec_t codec;
    if (compressionAlgoToVkcsCodec(t->compressor.algo, &codec) != 0 ||
        write_vkcs_envelope(t->emit_cb, t->emit_ctx, codec,
                            t->stream_kind, t->compressor.codec_checksum) != 0) {
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
    size_t needed = streamCompressOutputBound(&t->compressor, input_len, flush_mode);
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
    streamCompressorDestroy(&t->compressor);
    if (t->out_buf) {
        zfree(t->out_buf);
        t->out_buf = NULL;
    }
    t->out_buf_size = 0;
}

stream_writer_t *stream_writer_create(const stream_writer_config_t *cfg,
                                      vkcs_emit_fn emit_cb,
                                      void *emit_ctx) {
    if (!cfg || !emit_cb || !compressionAlgoSupportsStreaming(cfg->algo)) {
        return NULL;
    }

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

    const uint8_t *src = (const uint8_t *)buf;
    size_t remaining = len;
    uint64_t emitted_before = t->bytes_emitted;
    if (streamWriterEnsureEnvelope(t) != 0) return -1;
    while (remaining > 0) {
        size_t chunk_len = remaining < STREAM_WRITER_INPUT_CHUNK_SIZE
                               ? remaining
                               : STREAM_WRITER_INPUT_CHUNK_SIZE;
        if (streamWriterFeedAndEmit(t, src, chunk_len, FLUSH_CONTINUE) != 0) return -1;
        src += chunk_len;
        remaining -= chunk_len;
    }
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

int stream_writer_is_errored(const stream_writer_t *t) {
    return t && t->errored;
}

void stream_writer_set_error(stream_writer_t *t) {
    if (!t) return;
    t->errored = true;
}

/* Streaming reader context. */
struct stream_reader {
    stream_reader_read_fn read_cb; /* Returns >0 bytes, 0 EOF, -1 error */
    void *read_ctx;

    vkcs_probe_config_t probe_cfg;
    vkcs_probe_t probe;
    size_t probe_pos; /* Unread passthrough bytes buffered by vkcsProbeFeed */
    size_t batch_size;
    bool errored;
    stream_reader_error_t error_kind;

    stream_decompressor_t decompressor;
    bool decompressor_initialized;

    uint8_t *buf_slab; /* Combined allocation for compressed input + output window */
    uint8_t *read_buf; /* Buffered compressed input */
    size_t read_buf_size;
    size_t read_buf_pos;
    size_t read_buf_fill;

    uint8_t *window_buf; /* Buffered decompressed output for small caller reads */
    size_t window_size;
    size_t window_pos;
    size_t window_len;
};

static void streamReaderSetError(stream_reader_t *t, stream_reader_error_t error_kind) {
    t->errored = true;
    if (t->error_kind == STREAM_READER_ERROR_NONE) {
        t->error_kind = error_kind;
    }
}

/* Preserve partial output on read errors while latching sticky error state. */
static ssize_t streamReaderFail(stream_reader_t *t, size_t partial_bytes) {
    streamReaderSetError(t, STREAM_READER_ERROR_IO);
    return partial_bytes > 0 ? (ssize_t)partial_bytes : -1;
}

static ssize_t streamReaderFailWithError(stream_reader_t *t,
                                         size_t partial_bytes,
                                         stream_reader_error_t error_kind) {
    streamReaderSetError(t, error_kind);
    return partial_bytes > 0 ? (ssize_t)partial_bytes : -1;
}

static int streamReaderInitCompressedState(stream_reader_t *t, size_t batch_size) {
    if (batch_size > SIZE_MAX / 2) return -1;

    if (streamDecompressorInit(&t->decompressor, t->probe.algo) != 0) {
        return -1;
    }
    t->decompressor_initialized = true;

    t->buf_slab = zmalloc(batch_size * 2);
    t->read_buf = t->buf_slab;
    t->window_buf = t->buf_slab + batch_size;
    t->read_buf_size = batch_size;
    t->read_buf_pos = 0;
    t->read_buf_fill = 0;
    t->window_size = batch_size;
    t->window_pos = 0;
    t->window_len = 0;
    return 0;
}

static void streamReaderResetCompressedState(stream_reader_t *t) {
    if (t->decompressor_initialized) {
        streamDecompressorDestroy(&t->decompressor);
        t->decompressor_initialized = false;
    }
    if (t->buf_slab) {
        zfree(t->buf_slab);
        t->buf_slab = NULL;
    }
    t->read_buf = NULL;
    t->window_buf = NULL;
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
    if (!cfg) return NULL;

    stream_reader_t *t = zmalloc(sizeof(*t));
    memset(t, 0, sizeof(*t));
    t->read_cb = read_cb;
    t->read_ctx = read_ctx;
    t->probe_cfg.allow_passthrough = cfg->allow_passthrough;
    t->probe_cfg.expected_stream_kind = cfg->expected_stream_kind;
    vkcsProbeInit(&t->probe);
    t->batch_size = cfg->batch_size ? cfg->batch_size : STREAM_READER_BATCH_SIZE_DEFAULT;
    if (t->batch_size < STREAM_READER_BATCH_SIZE_MIN) {
        t->batch_size = STREAM_READER_BATCH_SIZE_MIN;
    }
    return t;
}

static size_t streamReaderProbeBytesNeeded(const stream_reader_t *t) {
    if (t->probe.header_len < 4) return 4 - t->probe.header_len;
    return VKCS_ENVELOPE_SIZE - t->probe.header_len;
}

int stream_reader_probe(stream_reader_t *t) {
    if (!t) return -1;
    if (t->errored) return -1;
    if (t->probe.ready) return 0;

    while (!t->probe.ready) {
        uint8_t buf[VKCS_ENVELOPE_SIZE];
        size_t need = streamReaderProbeBytesNeeded(t);
        ssize_t got = t->read_cb(t->read_ctx, buf, need);
        size_t consumed = 0;

        if (got < 0 || (size_t)got > need) {
            streamReaderSetError(t, STREAM_READER_ERROR_IO);
            return -1;
        }

        vkcs_probe_result_t status = vkcsProbeFeed(&t->probe, &t->probe_cfg, buf,
                                                   got > 0 ? (size_t)got : 0,
                                                   got == 0, &consumed);
        if (status == VKCS_PROBE_ERROR) {
            streamReaderSetError(t, STREAM_READER_ERROR_INCOMPATIBLE);
            return -1;
        }
        if (consumed != (size_t)(got > 0 ? got : 0)) {
            streamReaderSetError(t, STREAM_READER_ERROR_IO);
            return -1;
        }
        if (status == VKCS_PROBE_NEED_INPUT) continue;
        if (status == VKCS_PROBE_COMPRESSED &&
            !t->decompressor_initialized &&
            streamReaderInitCompressedState(t, t->batch_size) != 0) {
            streamReaderSetError(t, STREAM_READER_ERROR_IO);
            return -1;
        }
    }

    return 0;
}

static size_t streamReaderProbeAvail(const stream_reader_t *t) {
    if (t->probe.header_len <= t->probe_pos) return 0;
    return t->probe.header_len - t->probe_pos;
}

/* Passthrough reads may need to replay probe bytes before reading directly
 * from the wrapped source. On a transport read error after replaying some
 * bytes, return the partial payload and latch a sticky error for the next call. */
static ssize_t streamReaderReadPassthrough(stream_reader_t *t,
                                           uint8_t *dst,
                                           size_t len) {
    size_t total = 0;

    size_t prefix_avail = streamReaderProbeAvail(t);
    if (prefix_avail > 0) {
        size_t from_prefix = prefix_avail < len ? prefix_avail : len;
        memcpy(dst, t->probe.header + t->probe_pos, from_prefix);
        t->probe_pos += from_prefix;
        dst += from_prefix;
        len -= from_prefix;
        total += from_prefix;
    }

    if (len == 0) return (ssize_t)total;

    ssize_t got = t->read_cb(t->read_ctx, dst, len);
    if (got < 0 || (size_t)got > len) return streamReaderFail(t, total);
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
        if (produced < 0) {
            streamReaderSetError(t, STREAM_READER_ERROR_CORRUPT);
            return -1;
        }
        if (consumed > t->read_buf_fill ||
            (size_t)produced > out_size - *out_written) {
            streamReaderSetError(t, STREAM_READER_ERROR_CORRUPT);
            return -1;
        }
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

    /* The generic stream reader intentionally keeps a fixed-size compressed
     * input buffer and a fixed-size decompressed output window. If the codec
     * cannot make progress while the compressed buffer is full, treat the
     * stream as corrupt instead of growing more state. */
    streamReaderSetError(t, STREAM_READER_ERROR_CORRUPT);
    return 0;
}

static int streamReaderReadMoreCompressed(stream_reader_t *t) {
    size_t read_size = streamReaderReadBufTailSpace(t);
    if (read_size > (size_t)SSIZE_MAX) read_size = (size_t)SSIZE_MAX;
    if (read_size == 0) return -1;

    ssize_t got = t->read_cb(
        t->read_ctx,
        t->read_buf + t->read_buf_pos + t->read_buf_fill,
        read_size);
    if (got < 0) return -1;
    if (got == 0) return 0;
    if ((size_t)got > read_size) return -1;
    t->read_buf_fill += (size_t)got;
    return 1;
}

static ssize_t streamReaderFillWindow(stream_reader_t *t) {
    size_t written = 0;

    t->window_pos = 0;
    t->window_len = 0;

    while (written < t->window_size) {
        if (t->read_buf_fill > 0) {
            size_t chunk_written = 0;
            if (streamReaderDrainReadBuf(t, t->window_buf + written,
                                         t->window_size - written, &chunk_written) != 0) {
                written += chunk_written;
                break;
            }
            written += chunk_written;
            if (written >= t->window_size) break;
        }

        int read_rc = streamReaderReadMoreCompressed(t);
        if (read_rc < 0) {
            streamReaderSetError(t, STREAM_READER_ERROR_IO);
            if (written == 0) return -1;
            break;
        }
        if (read_rc == 0) break;
    }

    t->window_len = written;
    return (ssize_t)written;
}

static inline size_t streamReaderWindowAvail(const stream_reader_t *t) {
    if (t->window_len <= t->window_pos) return 0;
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
        if (streamReaderWindowAvail(t) == 0) {
            ssize_t filled = streamReaderFillWindow(t);
            if (filled < 0) {
                return streamReaderFailWithError(
                    t, total,
                    t->error_kind == STREAM_READER_ERROR_NONE ? STREAM_READER_ERROR_IO
                                                              : t->error_kind);
            }
            if (filled == 0 && !t->decompressor.frame_done) {
                return streamReaderFailWithError(t, total, STREAM_READER_ERROR_CORRUPT);
            }
            if (filled == 0) break;
        }

        total += streamReaderCopyFromWindow(t, &dst, &remaining);
        if (t->errored) break;
    }

    return (ssize_t)total;
}

ssize_t stream_reader_read(stream_reader_t *t, void *buf, size_t len) {
    if (!t || !buf) return -1;
    if (t->errored) return -1;
    if (len == 0) return 0;
    if (len > (size_t)SSIZE_MAX) return -1;

    if (stream_reader_probe(t) != 0) return -1;

    if (!t->probe.compressed) {
        return streamReaderReadPassthrough(t, (uint8_t *)buf, len);
    }

    return streamReaderReadCompressed(t, (uint8_t *)buf, len);
}

int stream_reader_get_info(stream_reader_t *t, stream_reader_info_t *info) {
    if (!t || !info) return -1;
    if (stream_reader_probe(t) != 0) return -1;

    info->compressed = t->probe.compressed;
    info->codec_checksum_enabled = t->probe.compressed ? t->probe.codec_checksum_enabled : false;
    info->algo = t->probe.compressed ? t->probe.algo : ALGO_NONE;
    info->stream_kind = t->probe.stream_kind;
    return 0;
}

stream_reader_error_t stream_reader_get_error(const stream_reader_t *t) {
    return t ? t->error_kind : STREAM_READER_ERROR_IO;
}

int stream_reader_finish(stream_reader_t *t) {
    if (!t) return -1;
    if (stream_reader_probe(t) != 0) return -1;
    if (!t->probe.compressed) return 0;

    /* Any already-buffered decompressed bytes belong to the current frame, so
     * finishing can discard them in place without copying them anywhere. */
    t->window_pos = t->window_len;
    while (!t->decompressor.frame_done) {
        ssize_t filled = streamReaderFillWindow(t);
        if (filled < 0) return -1;
        if (filled == 0) {
            if (t->decompressor.frame_done) break;
            streamReaderSetError(t, STREAM_READER_ERROR_CORRUPT);
            return -1;
        }
        t->window_pos = t->window_len;
    }
    return 0;
}

int stream_reader_detach(stream_reader_t *t, const uint8_t **buf, size_t *len) {
    if (!t || !buf || !len) return -1;
    /* Detach is the handoff point for callers that must keep using the wrapped
     * transport after this stream ends. Finish drains the current frame so the
     * pending raw bytes belong entirely to the next segment. */
    if (stream_reader_finish(t) != 0) return -1;
    return stream_reader_get_pending_input(t, buf, len);
}

int stream_reader_get_pending_input(stream_reader_t *t, const uint8_t **buf, size_t *len) {
    if (!t || !buf || !len) return -1;
    if (stream_reader_probe(t) != 0) return -1;

    *buf = NULL;
    *len = 0;

    if (!t->probe.compressed) {
        size_t prefix_avail = streamReaderProbeAvail(t);
        if (prefix_avail > 0) {
            *buf = t->probe.header + t->probe_pos;
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
