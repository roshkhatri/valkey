/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_STREAM_H
#define COMPRESSION_STREAM_H

#include "compression.h"
#include "sds.h"

/* VCS envelope:
 *   [0..2] magic "VCS"
 *   [3]    version (currently VCS_VERSION)
 *   [4]    codec id
 *   [5]    flags (bit 0 = codec checksum enabled; other bits reserved)
 *   [6]    stream kind
 *
 * All fields are single-byte in version 1. Future multi-byte fields must use
 * network byte order. */
#define VCS_MAGIC_0 0x56 /* 'V' */
#define VCS_MAGIC_1 0x43 /* 'C' */
#define VCS_MAGIC_2 0x53 /* 'S' */
#define VCS_MAGIC_SIZE 3
#define VCS_ENVELOPE_SIZE 7
#define VCS_VERSION 1
#define VCS_FLAG_CODEC_CHECKSUM (1 << 0)

/* Byte offsets of each envelope field. */
#define VCS_OFFSET_VERSION 3
#define VCS_OFFSET_ALGO 4
#define VCS_OFFSET_FLAGS 5
#define VCS_OFFSET_STREAM_KIND 6

/* Identifies what the compressed bytes decode to. The RDB loader rejects any
 * stream whose kind is not STREAM_KIND_RDB. */
typedef enum {
    STREAM_KIND_RDB = 0x00,
    STREAM_KIND_REPL = 0x01,
} streamKind;

typedef int (*streamWriterEmitFn)(void *ctx, const uint8_t *data, size_t len);
/* Returns >0 bytes read, 0 on EOF, -1 on error. Partial reads allowed. */
typedef ssize_t (*streamReaderReadFn)(void *ctx, void *buf, size_t len);

/* Default reader compressed-input/decompressed-output buffer size. Tiny caller
 * values are clamped up so the LZ4 decoder can always make forward progress
 * without growing internal state. */
#define STREAM_READER_BUFFER_SIZE_DEFAULT (1024 * 1024)
#define STREAM_READER_BUFFER_SIZE_MIN (128 * 1024)

typedef struct {
    compressionAlgo algo;
    int level;
    uint8_t stream_kind;
    bool codec_checksum_enabled;
} streamWriterConfig;

/* When allow_passthrough is set, non-VCS input is forwarded as raw bytes;
 * otherwise it is rejected. */
typedef struct {
    uint8_t expected_stream_kind;
    bool allow_passthrough;
    size_t buffer_size; /* Must be nonzero. */
} streamReaderConfig;

typedef struct {
    compressionAlgo algo;
    uint8_t stream_kind;
    bool compressed;
    bool codec_checksum_enabled;
} streamReaderInfo;

typedef enum {
    STREAM_READER_ERROR_NONE = 0,
    STREAM_READER_ERROR_IO = 1,
    STREAM_READER_ERROR_INCOMPATIBLE = 2,
    STREAM_READER_ERROR_CORRUPT = 3,
    /* Source hit EOF before the codec frame completed: a truncated stream
     * (e.g. replica connection killed mid-transfer), recoverable like an
     * uncompressed short read, as opposed to genuine codec corruption. */
    STREAM_READER_ERROR_TRUNCATED = 4,
} streamReaderError;

typedef struct streamWriter {
    streamCompressor compressor;
    uint8_t *out_buf;
    size_t out_buf_size;
    streamWriterEmitFn emit_fn;
    void *emit_ctx;
    sds *sink; /* When set, compress directly into *sink instead of out_buf + emit. */
    uint8_t stream_kind;
    bool envelope_written;
    bool finished;
    bool errored;
} streamWriter;

typedef struct streamReader {
    streamReaderReadFn read_cb;
    void *read_ctx;

    struct {
        bool allow_passthrough;
        uint8_t expected_stream_kind;
    } probe_cfg;
    struct {
        uint8_t header[VCS_ENVELOPE_SIZE];
        uint8_t stream_kind;
        size_t header_len;
        compressionAlgo algo;
        bool ready;
        bool compressed;
        bool codec_checksum_enabled;
    } probe;
    size_t probe_replay_pos; /* Passthrough bytes left to replay from probe. */
    size_t buffer_size;
    bool errored;
    streamReaderError error_kind;

    streamDecompressor decompressor;
    bool decompressor_initialized;

    uint8_t *compressed_buf;
    size_t compressed_buf_pos;
    size_t compressed_buf_len;

    uint8_t *decompressed_buf;
    size_t decompressed_buf_pos;
    size_t decompressed_buf_len;
} streamReader;

/* The writer pushes compressed bytes to a streamWriterEmitFn sink; the reader
 * pulls from a streamReaderReadFn source. streamWriterFinish must run before
 * freeing, since it emits the frame end; a writer freed without it is
 * truncated. The reader probes the envelope on the first read; callers that
 * need to classify the stream up front can use streamReaderGetInfo. */
int streamWriterInit(streamWriter *writer, streamWriterConfig *cfg, streamWriterEmitFn emit_fn, void *emit_ctx);

/* Redirect compressed output (envelope + frames) straight into *sink, bypassing
 * the internal scratch buffer and emit callback. */
void streamWriterSetSink(streamWriter *writer, sds *sink);

/* Returns 0 on success and -1 on error. */
int streamWriterWrite(streamWriter *writer, const void *buf, size_t len);
int streamWriterFlush(streamWriter *writer);
int streamWriterFinish(streamWriter *writer);
void streamWriterFree(streamWriter *writer);
int streamReadEnvelopeInfo(const uint8_t *buf,
                           size_t len,
                           uint8_t expected_stream_kind,
                           streamReaderInfo *info);

int streamReaderInit(streamReader *reader, streamReaderConfig *cfg, streamReaderReadFn read_cb, void *read_ctx);

/* Full or fail: returns len on success, 0 on EOF, -1 on error. */
ssize_t streamReaderRead(streamReader *reader, void *buf, size_t len);
int streamReaderGetInfo(streamReader *reader, streamReaderInfo *info);

/* Validate a compressed frame without consuming caller-owned trailing framing. */
int streamReaderValidateFrameEnd(streamReader *reader);

/* Validate a compressed frame and require the wrapped source to be exhausted. */
int streamReaderValidateEnd(streamReader *reader);
void streamReaderFree(streamReader *reader);

/* Approximate scratch/codec memory held by the writer, for client-output-buffer
 * accounting. */
size_t streamWriterMemUsage(const streamWriter *writer);

#endif /* COMPRESSION_STREAM_H */
