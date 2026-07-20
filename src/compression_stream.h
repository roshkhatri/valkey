/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_STREAM_H
#define COMPRESSION_STREAM_H

#include "compression.h"

/* VCS envelope:
 *   [0..2] magic "VCS"
 *   [3]    version (currently VCS_VERSION)
 *   [4]    codec id
 *   [5]    reserved (must be zero)
 *   [6]    stream kind
 *
 * All fields are single-byte. Future multi-byte fields must use
 * network byte order. */
#define VCS_MAGIC_0 0x56 /* 'V' */
#define VCS_MAGIC_1 0x43 /* 'C' */
#define VCS_MAGIC_2 0x53 /* 'S' */
#define VCS_MAGIC_SIZE 3
#define VCS_ENVELOPE_SIZE 7
#define VCS_VERSION 1
/* Byte offsets of each envelope field. */
#define VCS_OFFSET_VERSION 3
#define VCS_OFFSET_CODEC 4
#define VCS_OFFSET_RESERVED 5
#define VCS_OFFSET_STREAM_KIND 6

/* Wire identifier; deliberately independent of compressionAlgo values. */
#define VCS_CODEC_LZ4 0x01

/* Identifies an RDB payload in the envelope. */
#define VCS_STREAM_RDB 0x01

typedef int (*streamWriterWriteFn)(void *ctx, const uint8_t *data, size_t len);
/* Returns >0 bytes read, 0 on EOF, -1 on error. Partial reads allowed. */
typedef ssize_t (*streamReaderReadFn)(void *ctx, void *buf, size_t len);

/* Default reader compressed-input/decompressed-output buffer size. Tiny caller
 * values are clamped up so the decoder can always make forward progress
 * without growing internal state. */
#define STREAM_READER_BUFFER_SIZE_DEFAULT (1024 * 1024)
#define STREAM_READER_BUFFER_SIZE_MIN (128 * 1024)

typedef struct {
    compressionAlgo algo;
    int level;
    bool codec_checksum_enabled;
} streamWriterConfig;

/* When allow_passthrough is set, non-VCS input is forwarded as raw bytes;
 * otherwise it is rejected. */
typedef struct {
    bool allow_passthrough;
    bool skip_codec_checksum_validation;
    size_t buffer_size;
} streamReaderConfig;

typedef enum {
    STREAM_READER_ERROR_NONE = 0,
    STREAM_READER_ERROR_IO = 1,
    STREAM_READER_ERROR_INCOMPATIBLE = 2,
    STREAM_READER_ERROR_CORRUPT = 3,
} streamReaderErrorKind;

typedef enum {
    STREAM_WRITER_STATE_INITIAL = 0,
    STREAM_WRITER_STATE_ACTIVE,
    STREAM_WRITER_STATE_FINISHED,
    STREAM_WRITER_STATE_ERROR,
} streamWriterState;

typedef enum {
    STREAM_READER_STATE_INITIAL = 0,
    STREAM_READER_STATE_PASSTHROUGH,
    STREAM_READER_STATE_COMPRESSED,
    STREAM_READER_STATE_FINISHED,
} streamReaderState;

typedef struct streamWriter {
    streamCompressor compressor;
    uint8_t *out_buf;
    size_t out_buf_size;
    streamWriterWriteFn write_cb;
    void *write_ctx;
    streamWriterState state;
} streamWriter;

typedef struct streamReader {
    streamReaderReadFn read_cb;
    void *read_ctx;
    struct {
        uint8_t header[VCS_ENVELOPE_SIZE];
        size_t header_len;
    } probe;
    size_t probe_replay_pos; /* Passthrough bytes left to replay from probe. */
    size_t buffer_size;
    streamReaderErrorKind error_kind;
    streamReaderState state;

    streamDecompressor decompressor;

    uint8_t *compressed_buf;
    size_t compressed_buf_pos;
    size_t compressed_buf_len;

    uint8_t *decompressed_buf;
    size_t decompressed_buf_pos;
    size_t decompressed_buf_len;
} streamReader;

/* The writer pushes compressed bytes to a streamWriterWriteFn sink; the reader
 * pulls from a streamReaderReadFn source. streamWriterFinish must run before
 * freeing, since it emits the frame end; a writer freed without it is
 * truncated. Reader initialization probes the envelope and optionally returns
 * the detected algorithm. */
int streamWriterInit(streamWriter *writer, const streamWriterConfig *cfg, streamWriterWriteFn write_cb, void *write_ctx);

/* Returns 0 on success and -1 on error. Errors are sticky: after a failed
 * write, flush, or finish, later operations fail without emitting bytes. */
int streamWriterWrite(streamWriter *writer, const void *buf, size_t len);
int streamWriterFlush(streamWriter *writer);
int streamWriterFinish(streamWriter *writer);
void streamWriterFree(streamWriter *writer);
int streamReaderInit(streamReader *reader, const streamReaderConfig *cfg, streamReaderReadFn read_cb, void *read_ctx, compressionAlgo *detected_algo);

/* Returns up to len bytes, 0 on EOF, or -1 on error. An error after partial
 * output is reported on the next call. */
ssize_t streamReaderRead(streamReader *reader, void *buf, size_t len);
/* Completes and validates a compressed frame after the logical parser has
 * consumed its payload. It stops at the frame boundary; the caller owns any
 * following transport framing or physical EOF validation. */
int streamReaderFinish(streamReader *reader);
void streamReaderFree(streamReader *reader);

#endif /* COMPRESSION_STREAM_H */
