/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_stream.h"
#include "serverassert.h"
#include "zmalloc.h"
#include <limits.h>
#include <string.h>

/* ===== VCS envelope ===== */

static const uint8_t VCS_MAGIC[VCS_MAGIC_SIZE] = {
    VCS_MAGIC_0,
    VCS_MAGIC_1,
    VCS_MAGIC_2,
};

/* True when the first len bytes of buf match the VCS magic. When len is below
 * VCS_MAGIC_SIZE this only compares that prefix. */
static bool vcsHasMagicPrefix(const uint8_t *buf, size_t len) {
    size_t n = len < VCS_MAGIC_SIZE ? len : VCS_MAGIC_SIZE;
    return memcmp(buf, VCS_MAGIC, n) == 0;
}

typedef enum {
    VCS_PROBE_NEED_INPUT = 0,
    VCS_PROBE_PASSTHROUGH = 1,
    VCS_PROBE_COMPRESSED = 2,
    VCS_PROBE_ERROR = 3,
} vcsProbeResult;

static bool streamReaderProbeHasMagicPrefix(streamReader *reader) {
    if (reader->probe.header_len == 0) return false;
    return vcsHasMagicPrefix(reader->probe.header, reader->probe.header_len);
}

static void streamReaderProbeSetPassthrough(streamReader *reader) {
    reader->probe.ready = true;
    reader->probe.compressed = false;
    reader->probe.codec_checksum_enabled = false;
    reader->probe.algo = ALGO_NONE;
    reader->probe.stream_kind = 0;
}

static void streamReaderProbeSetCompressed(streamReader *reader,
                                           compressionAlgo algo,
                                           uint8_t stream_kind,
                                           bool codec_checksum_enabled) {
    reader->probe.ready = true;
    reader->probe.compressed = true;
    reader->probe.codec_checksum_enabled = codec_checksum_enabled;
    reader->probe.algo = algo;
    reader->probe.stream_kind = stream_kind;
}

static int writeVcsEnvelope(streamWriterEmitFn emit_fn,
                            void *ctx,
                            compressionAlgo algo,
                            uint8_t stream_kind,
                            bool codec_checksum_enabled) {
    if (!compressionAlgoSupportsStreaming(algo)) return -1;

    uint8_t envelope[VCS_ENVELOPE_SIZE] = {
        VCS_MAGIC_0,
        VCS_MAGIC_1,
        VCS_MAGIC_2,
        [VCS_OFFSET_VERSION] = VCS_VERSION,
        [VCS_OFFSET_ALGO] = (uint8_t)algo,
        [VCS_OFFSET_FLAGS] = codec_checksum_enabled ? VCS_FLAG_CODEC_CHECKSUM : 0,
        [VCS_OFFSET_STREAM_KIND] = stream_kind,
    };
    return emit_fn(ctx, envelope, VCS_ENVELOPE_SIZE) == 0 ? 0 : -1;
}

/* Rejects unknown flag bits so a future format extension fails loud rather
 * than silently corrupting load. */
static int readVcsEnvelope(const uint8_t *buf,
                           size_t len,
                           compressionAlgo *algo,
                           uint8_t *stream_kind,
                           bool *codec_checksum_enabled) {
    if (len < VCS_ENVELOPE_SIZE) return -1;

    if (!vcsHasMagicPrefix(buf, VCS_MAGIC_SIZE)) return -1;
    if (buf[VCS_OFFSET_VERSION] != VCS_VERSION) return -1;

    compressionAlgo parsed_algo = (compressionAlgo)buf[VCS_OFFSET_ALGO];
    if (!compressionAlgoSupportsStreaming(parsed_algo)) return -1;

    uint8_t flags = buf[VCS_OFFSET_FLAGS];
    if (flags & ~VCS_FLAG_CODEC_CHECKSUM) return -1;

    if (algo) *algo = parsed_algo;
    if (stream_kind) *stream_kind = buf[VCS_OFFSET_STREAM_KIND];
    if (codec_checksum_enabled) *codec_checksum_enabled = (flags & VCS_FLAG_CODEC_CHECKSUM) != 0;
    return 0;
}

int streamReadEnvelopeInfo(const uint8_t *buf,
                           size_t len,
                           uint8_t expected_stream_kind,
                           streamReaderInfo *info) {
    uint8_t stream_kind = 0;
    bool codec_checksum_enabled = false;
    compressionAlgo algo = ALGO_NONE;

    if (len < VCS_ENVELOPE_SIZE ||
        readVcsEnvelope(buf, len, &algo, &stream_kind, &codec_checksum_enabled) != 0 ||
        stream_kind != expected_stream_kind) {
        return -1;
    }

    info->compressed = true;
    info->codec_checksum_enabled = codec_checksum_enabled;
    info->algo = algo;
    info->stream_kind = stream_kind;
    return 0;
}

/* Incremental: wrapped rios may legally return fewer than VCS_ENVELOPE_SIZE
 * bytes per read. Consumed bytes are retained in probe->header so passthrough
 * can replay them exactly. */
static vcsProbeResult streamReaderProbeFeed(streamReader *reader,
                                            const uint8_t *src,
                                            size_t src_len,
                                            bool input_eof,
                                            size_t *src_consumed) {
    size_t consumed = 0;
    *src_consumed = 0;
    if (reader->probe.ready) {
        return reader->probe.compressed ? VCS_PROBE_COMPRESSED : VCS_PROBE_PASSTHROUGH;
    }

    while (consumed < src_len) {
        size_t target = reader->probe.header_len < VCS_MAGIC_SIZE ? VCS_MAGIC_SIZE : VCS_ENVELOPE_SIZE;
        size_t need = target - reader->probe.header_len;
        size_t take = src_len - consumed < need ? src_len - consumed : need;

        memcpy(reader->probe.header + reader->probe.header_len, src + consumed, take);
        reader->probe.header_len += take;
        consumed += take;

        if (reader->probe.header_len >= VCS_MAGIC_SIZE && !vcsHasMagicPrefix(reader->probe.header, VCS_MAGIC_SIZE)) {
            *src_consumed = consumed;
            if (!reader->probe_cfg.allow_passthrough) return VCS_PROBE_ERROR;
            streamReaderProbeSetPassthrough(reader);
            return VCS_PROBE_PASSTHROUGH;
        }

        if (reader->probe.header_len == VCS_ENVELOPE_SIZE) {
            streamReaderInfo info = {0};
            if (streamReadEnvelopeInfo(reader->probe.header, VCS_ENVELOPE_SIZE,
                                       reader->probe_cfg.expected_stream_kind, &info) != 0) {
                *src_consumed = consumed;
                return VCS_PROBE_ERROR;
            }
            streamReaderProbeSetCompressed(reader, info.algo, info.stream_kind,
                                           info.codec_checksum_enabled);
            *src_consumed = consumed;
            return VCS_PROBE_COMPRESSED;
        }
    }

    if (input_eof) {
        *src_consumed = consumed;
        /* EOF mid-magic looks like a truncated VCS, not a valid passthrough. */
        if (streamReaderProbeHasMagicPrefix(reader)) return VCS_PROBE_ERROR;
        if (!reader->probe_cfg.allow_passthrough) return VCS_PROBE_ERROR;
        streamReaderProbeSetPassthrough(reader);
        return VCS_PROBE_PASSTHROUGH;
    }

    *src_consumed = consumed;
    return VCS_PROBE_NEED_INPUT;
}

/* ===== Streaming writer ===== */

#define STREAM_WRITER_INPUT_CHUNK_SIZE (1024 * 1024)

int streamWriterInit(streamWriter *writer, streamWriterConfig *cfg, streamWriterEmitFn emit_fn, void *emit_ctx) {
    if (!compressionAlgoSupportsStreaming(cfg->algo)) return -1;

    memset(writer, 0, sizeof(*writer));
    writer->emit_fn = emit_fn;
    writer->emit_ctx = emit_ctx;
    writer->stream_kind = cfg->stream_kind;

    if (streamCompressorInit(&writer->compressor, cfg->algo, cfg->level) != 0) return -1;
    writer->compressor.codec_checksum = cfg->codec_checksum_enabled;
    return 0;
}

/* Envelope is emitted lazily so a writer that's created but never written
 * doesn't leave a stub envelope on the sink. */
static int streamWriterEnsureEnvelope(streamWriter *writer) {
    if (writer->envelope_written) return 0;
    if (writeVcsEnvelope(writer->emit_fn, writer->emit_ctx, writer->compressor.algo,
                         writer->stream_kind, writer->compressor.codec_checksum) != 0) {
        writer->errored = true;
        return -1;
    }
    writer->bytes_emitted += VCS_ENVELOPE_SIZE;
    writer->envelope_written = true;
    return 0;
}

static int streamWriterEmit(streamWriter *writer, const uint8_t *buf, size_t len) {
    if (len == 0) return 0;
    if (writer->emit_fn(writer->emit_ctx, buf, len) != 0) {
        writer->errored = true;
        return -1;
    }
    writer->bytes_emitted += len;
    return 0;
}

static int streamWriterEnsureOutBuf(streamWriter *writer, size_t input_len) {
    size_t needed = streamCompressorOutputBound(&writer->compressor, input_len);
    if (needed == 0) {
        writer->errored = true;
        return -1;
    }
    if (needed > writer->out_buf_size) {
        writer->out_buf = zrealloc(writer->out_buf, needed);
        writer->out_buf_size = needed;
    }
    return 0;
}

static int streamWriterFeedAndEmit(streamWriter *writer,
                                   const uint8_t *input,
                                   size_t input_len,
                                   compressFlushMode flush_mode) {
    if (streamWriterEnsureOutBuf(writer, input_len) != 0) return -1;

    ssize_t compressed = streamCompressorFeed(&writer->compressor, writer->out_buf,
                                              writer->out_buf_size,
                                              input, input_len, flush_mode);
    if (compressed < 0) {
        writer->errored = true;
        return -1;
    }
    return streamWriterEmit(writer, writer->out_buf, (size_t)compressed);
}

void streamWriterFree(streamWriter *writer) {
    streamCompressorFree(&writer->compressor);
    if (writer->out_buf) {
        zfree(writer->out_buf);
        writer->out_buf = NULL;
    }
    writer->out_buf_size = 0;
}

ssize_t streamWriterWrite(streamWriter *writer, const void *buf, size_t len) {
    /* Writes after finish are a caller bug; silently dropping them would
     * corrupt the consumer's view of the stream. */
    if (writer->finished) return -1;
    if (len == 0) return 0;
    if (len > (size_t)SSIZE_MAX) return -1;

    const uint8_t *src = (const uint8_t *)buf;
    size_t remaining = len;
    uint64_t emitted_before = writer->bytes_emitted;
    if (streamWriterEnsureEnvelope(writer) != 0) return -1;
    while (remaining > 0) {
        size_t chunk_len = remaining < STREAM_WRITER_INPUT_CHUNK_SIZE
                               ? remaining
                               : STREAM_WRITER_INPUT_CHUNK_SIZE;
        if (streamWriterFeedAndEmit(writer, src, chunk_len, FLUSH_CONTINUE) != 0) return -1;
        src += chunk_len;
        remaining -= chunk_len;
    }
    uint64_t emitted_delta = writer->bytes_emitted - emitted_before;
    if (emitted_delta > (uint64_t)SSIZE_MAX) {
        writer->errored = true;
        return -1;
    }
    return (ssize_t)emitted_delta;
}

int streamWriterFlush(streamWriter *writer) {
    /* Flush after finish is a no-op: frame is already closed. */
    if (writer->finished) return 0;

    if (!writer->envelope_written || !writer->compressor.stream_started) return 0;
    return streamWriterFeedAndEmit(writer, NULL, 0, FLUSH_SYNC);
}

int streamWriterFinish(streamWriter *writer) {
    if (writer->finished) return 0;
    writer->finished = true;

    /* Even an empty stream produces a valid envelope + empty frame so the
     * loader sees a well-formed file. */
    if (streamWriterEnsureEnvelope(writer) != 0) return -1;
    return streamWriterFeedAndEmit(writer, NULL, 0, FLUSH_END);
}

/* ===== Streaming reader ===== */

static void streamReaderSetError(streamReader *reader, streamReaderError error_kind) {
    reader->errored = true;
    if (reader->error_kind == STREAM_READER_ERROR_NONE) reader->error_kind = error_kind;
}

static ssize_t streamReaderFail(streamReader *reader, size_t partial_bytes) {
    streamReaderSetError(reader, STREAM_READER_ERROR_IO);
    return partial_bytes > 0 ? (ssize_t)partial_bytes : -1;
}

static ssize_t streamReaderFailWithError(streamReader *reader,
                                         size_t partial_bytes,
                                         streamReaderError error_kind) {
    streamReaderSetError(reader, error_kind);
    return partial_bytes > 0 ? (ssize_t)partial_bytes : -1;
}

static int streamReaderInitCompressedState(streamReader *reader, size_t buffer_size) {
    if (streamDecompressorInit(&reader->decompressor, reader->probe.algo) != 0) return -1;
    reader->decompressor_initialized = true;
    reader->compressed_buf = zmalloc(buffer_size);
    reader->decompressed_buf = zmalloc(buffer_size);
    return 0;
}

static void streamReaderResetCompressedState(streamReader *reader) {
    if (reader->decompressor_initialized) {
        streamDecompressorFree(&reader->decompressor);
        reader->decompressor_initialized = false;
    }
    if (reader->compressed_buf) {
        zfree(reader->compressed_buf);
        reader->compressed_buf = NULL;
    }
    reader->compressed_buf_pos = 0;
    reader->compressed_buf_len = 0;
    if (reader->decompressed_buf) {
        zfree(reader->decompressed_buf);
        reader->decompressed_buf = NULL;
    }
    reader->decompressed_buf_pos = 0;
    reader->decompressed_buf_len = 0;
}

int streamReaderInit(streamReader *reader, streamReaderConfig *cfg, streamReaderReadFn read_cb, void *read_ctx) {
    assert(cfg->buffer_size != 0);

    memset(reader, 0, sizeof(*reader));
    reader->read_cb = read_cb;
    reader->read_ctx = read_ctx;
    reader->probe_cfg.allow_passthrough = cfg->allow_passthrough;
    reader->probe_cfg.expected_stream_kind = cfg->expected_stream_kind;
    reader->buffer_size = cfg->buffer_size;
    if (reader->buffer_size < STREAM_READER_BUFFER_SIZE_MIN) {
        reader->buffer_size = STREAM_READER_BUFFER_SIZE_MIN;
    }
    return 0;
}

static size_t streamReaderProbeBytesNeeded(streamReader *reader) {
    if (reader->probe.header_len < VCS_MAGIC_SIZE) return VCS_MAGIC_SIZE - reader->probe.header_len;
    return VCS_ENVELOPE_SIZE - reader->probe.header_len;
}

int streamReaderProbe(streamReader *reader) {
    if (reader->errored) return -1;
    if (reader->probe.ready) return 0;

    while (!reader->probe.ready) {
        uint8_t buf[VCS_ENVELOPE_SIZE];
        size_t need = streamReaderProbeBytesNeeded(reader);
        ssize_t got = reader->read_cb(reader->read_ctx, buf, need);
        size_t consumed = 0;

        if (got < 0 || (size_t)got > need) {
            streamReaderSetError(reader, STREAM_READER_ERROR_IO);
            return -1;
        }

        vcsProbeResult status = streamReaderProbeFeed(reader, buf,
                                                      got > 0 ? (size_t)got : 0,
                                                      got == 0, &consumed);
        switch (status) {
        case VCS_PROBE_ERROR:
            streamReaderSetError(reader, STREAM_READER_ERROR_INCOMPATIBLE);
            return -1;
        case VCS_PROBE_NEED_INPUT:
            continue;
        case VCS_PROBE_COMPRESSED:
            if (!reader->decompressor_initialized &&
                streamReaderInitCompressedState(reader, reader->buffer_size) != 0) {
                streamReaderSetError(reader, STREAM_READER_ERROR_IO);
                return -1;
            }
            break;
        case VCS_PROBE_PASSTHROUGH:
            break;
        default:
            streamReaderSetError(reader, STREAM_READER_ERROR_INCOMPATIBLE);
            return -1;
        }
    }
    return 0;
}

static size_t streamReaderProbeAvail(streamReader *reader) {
    if (reader->probe.header_len <= reader->probe_replay_pos) return 0;
    return reader->probe.header_len - reader->probe_replay_pos;
}

/* Replay any probe-buffered bytes before reading from the wrapped source. */
static ssize_t streamReaderReadPassthrough(streamReader *reader, uint8_t *dst, size_t len) {
    size_t total = 0;
    size_t prefix_avail = streamReaderProbeAvail(reader);
    if (prefix_avail > 0) {
        size_t from_prefix = prefix_avail < len ? prefix_avail : len;
        memcpy(dst, reader->probe.header + reader->probe_replay_pos, from_prefix);
        reader->probe_replay_pos += from_prefix;
        dst += from_prefix;
        len -= from_prefix;
        total += from_prefix;
    }
    if (len == 0) return (ssize_t)total;

    ssize_t got = reader->read_cb(reader->read_ctx, dst, len);
    if (got < 0 || (size_t)got > len) return streamReaderFail(reader, total);
    return (ssize_t)(total + (size_t)got);
}

static int streamReaderDrainCompressedBuf(streamReader *reader,
                                          uint8_t *out,
                                          size_t out_size,
                                          size_t *out_written) {
    *out_written = 0;
    while (reader->compressed_buf_len > 0 && *out_written < out_size) {
        size_t consumed = 0;
        size_t feed_len = reader->compressed_buf_len;
        size_t input_hint = reader->decompressor.input_hint;
        if (input_hint > 0 && feed_len > input_hint) feed_len = input_hint;
        ssize_t produced = streamDecompressorFeed(
            &reader->decompressor,
            out + *out_written, out_size - *out_written,
            reader->compressed_buf + reader->compressed_buf_pos,
            feed_len, &consumed);
        if (produced < 0 ||
            consumed > feed_len ||
            (size_t)produced > out_size - *out_written) {
            streamReaderSetError(reader, STREAM_READER_ERROR_CORRUPT);
            return -1;
        }
        *out_written += (size_t)produced;
        reader->compressed_buf_pos += consumed;
        reader->compressed_buf_len -= consumed;
        if (reader->decompressor.frame_done) break;
        if (consumed == 0 && produced == 0) break;
    }
    if (reader->compressed_buf_len == 0) reader->compressed_buf_pos = 0;
    return 0;
}

static size_t streamReaderCompressedBufTailSpace(streamReader *reader) {
    size_t tail_space = reader->buffer_size - reader->compressed_buf_pos - reader->compressed_buf_len;
    if (tail_space > 0) return tail_space;

    if (reader->compressed_buf_len == 0) {
        reader->compressed_buf_pos = 0;
        return reader->buffer_size;
    }

    if (reader->compressed_buf_pos > 0) {
        memmove(reader->compressed_buf, reader->compressed_buf + reader->compressed_buf_pos, reader->compressed_buf_len);
        reader->compressed_buf_pos = 0;
        return reader->buffer_size - reader->compressed_buf_len;
    }

    /* Buffer full and the codec made no progress, treat as corrupt rather
     * than grow buffers indefinitely. */
    streamReaderSetError(reader, STREAM_READER_ERROR_CORRUPT);
    return 0;
}

static int streamReaderRefillCompressedBuf(streamReader *reader) {
    if (reader->decompressor.frame_done) return 0;

    size_t read_size = streamReaderCompressedBufTailSpace(reader);
    size_t input_hint = reader->decompressor.input_hint;
    if (input_hint > 0 && read_size > input_hint) read_size = input_hint;
    if (read_size > (size_t)SSIZE_MAX) read_size = (size_t)SSIZE_MAX;
    if (read_size == 0) return -1;

    ssize_t got = reader->read_cb(reader->read_ctx,
                                  reader->compressed_buf + reader->compressed_buf_pos + reader->compressed_buf_len,
                                  read_size);
    if (got < 0 || (size_t)got > read_size) return -1;
    if (got == 0) return 0;
    reader->compressed_buf_len += (size_t)got;
    return 1;
}

static ssize_t streamReaderFillDecompressedBuf(streamReader *reader) {
    size_t written = 0;

    reader->decompressed_buf_pos = 0;
    reader->decompressed_buf_len = 0;

    while (written < reader->buffer_size) {
        if (reader->compressed_buf_len > 0) {
            size_t chunk_written = 0;
            if (streamReaderDrainCompressedBuf(reader, reader->decompressed_buf + written,
                                               reader->buffer_size - written, &chunk_written) != 0) {
                written += chunk_written;
                break;
            }
            written += chunk_written;
            if (written >= reader->buffer_size) break;
        }

        int read_rc = streamReaderRefillCompressedBuf(reader);
        if (read_rc < 0) {
            streamReaderSetError(reader, STREAM_READER_ERROR_IO);
            if (written == 0) return -1;
            break;
        }
        if (read_rc == 0) break;
    }

    reader->decompressed_buf_len = written;
    return (ssize_t)written;
}

static inline size_t streamReaderDecompressedBufAvail(streamReader *reader) {
    if (reader->decompressed_buf_len <= reader->decompressed_buf_pos) return 0;
    return reader->decompressed_buf_len - reader->decompressed_buf_pos;
}

static size_t streamReaderCopyFromDecompressedBuf(streamReader *reader,
                                                  uint8_t **dst,
                                                  size_t *remaining) {
    size_t avail = streamReaderDecompressedBufAvail(reader);
    if (avail == 0 || *remaining == 0) return 0;

    size_t to_copy = avail < *remaining ? avail : *remaining;
    memcpy(*dst, reader->decompressed_buf + reader->decompressed_buf_pos, to_copy);
    reader->decompressed_buf_pos += to_copy;
    *dst += to_copy;
    *remaining -= to_copy;
    return to_copy;
}

static ssize_t streamReaderReadCompressed(streamReader *reader, uint8_t *dst, size_t len) {
    size_t remaining = len;
    size_t total = 0;

    total += streamReaderCopyFromDecompressedBuf(reader, &dst, &remaining);
    while (remaining > 0) {
        if (streamReaderDecompressedBufAvail(reader) == 0) {
            ssize_t filled = streamReaderFillDecompressedBuf(reader);
            if (filled < 0) {
                return streamReaderFailWithError(
                    reader, total,
                    reader->error_kind == STREAM_READER_ERROR_NONE ? STREAM_READER_ERROR_IO
                                                                   : reader->error_kind);
            }
            if (filled == 0 && !reader->decompressor.frame_done) {
                return streamReaderFailWithError(reader, total, STREAM_READER_ERROR_CORRUPT);
            }
            if (filled == 0) break;
        }

        total += streamReaderCopyFromDecompressedBuf(reader, &dst, &remaining);
        if (reader->errored) break;
    }

    return (ssize_t)total;
}

ssize_t streamReaderRead(streamReader *reader, void *buf, size_t len) {
    if (reader->errored) return -1;
    if (len == 0) return 0;
    if (len > (size_t)SSIZE_MAX) return -1;

    if (!reader->probe.ready && streamReaderProbe(reader) != 0) return -1;

    if (!reader->probe.compressed) {
        return streamReaderReadPassthrough(reader, (uint8_t *)buf, len);
    }
    return streamReaderReadCompressed(reader, (uint8_t *)buf, len);
}

int streamReaderGetInfo(streamReader *reader, streamReaderInfo *info) {
    if (streamReaderProbe(reader) != 0) return -1;

    info->compressed = reader->probe.compressed;
    info->codec_checksum_enabled = reader->probe.compressed ? reader->probe.codec_checksum_enabled : false;
    info->algo = reader->probe.compressed ? reader->probe.algo : ALGO_NONE;
    info->stream_kind = reader->probe.stream_kind;
    return 0;
}

int streamReaderValidateEnd(streamReader *reader) {
    uint8_t buf[4096];

    if (streamReaderProbe(reader) != 0) return -1;
    if (!reader->probe.compressed) return 0;
    if (streamReaderDecompressedBufAvail(reader) > 0) {
        streamReaderSetError(reader, STREAM_READER_ERROR_CORRUPT);
        return -1;
    }

    while (!reader->decompressor.frame_done) {
        ssize_t nread = streamReaderRead(reader, buf, sizeof(buf));
        if (nread < 0) return -1;
        if (nread > 0) {
            streamReaderSetError(reader, STREAM_READER_ERROR_CORRUPT);
            return -1;
        }
    }

    if (reader->compressed_buf_len > 0) {
        streamReaderSetError(reader, STREAM_READER_ERROR_CORRUPT);
        return -1;
    }

    ssize_t got = reader->read_cb(reader->read_ctx, buf, 1);
    if (got < 0) {
        streamReaderSetError(reader, STREAM_READER_ERROR_IO);
        return -1;
    }
    if (got > 0) {
        streamReaderSetError(reader, STREAM_READER_ERROR_CORRUPT);
        return -1;
    }
    return 0;
}

void streamReaderFree(streamReader *reader) {
    streamReaderResetCompressedState(reader);
}
