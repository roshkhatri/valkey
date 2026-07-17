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

/* Protocol identifiers are independent of implementation enum values. */
typedef enum {
    VCS_CODEC_LZ4 = 0x01,
} vcsCodecId;

/* Identifies what the compressed bytes decode to. */
typedef enum {
    VCS_STREAM_RDB = 0x01,
} vcsStreamKind;

bool compressionAlgoToVcsCodec(compressionAlgo algo, vcsCodecId *codec);
bool vcsCodecToCompressionAlgo(uint8_t codec, compressionAlgo *algo);

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
    bool skip_codec_checksum_validation;
    size_t buffer_size; /* Must be nonzero. */
} streamReaderConfig;

typedef struct {
    compressionAlgo algo;
    uint8_t stream_kind;
    bool compressed;
} streamReaderInfo;

typedef enum {
    STREAM_READER_ERROR_NONE = 0,
    STREAM_READER_ERROR_IO = 1,
    STREAM_READER_ERROR_INCOMPATIBLE = 2,
    STREAM_READER_ERROR_CORRUPT = 3,
} streamReaderError;

typedef struct streamWriter {
    streamCompressor compressor;
    uint8_t *out_buf;
    size_t out_buf_size;
    streamWriterEmitFn emit_fn;
    void *emit_ctx;
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
    } probe;
    size_t probe_replay_pos; /* Passthrough bytes left to replay from probe. */
    size_t buffer_size;
    bool skip_codec_checksum_validation;
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

/* Returns 0 on success and -1 on error. Errors are sticky: after a failed
 * write, flush, or finish, later operations fail without emitting bytes. */
int streamWriterWrite(streamWriter *writer, const void *buf, size_t len);
int streamWriterFlush(streamWriter *writer);
int streamWriterFinish(streamWriter *writer);
void streamWriterFree(streamWriter *writer);
int streamReadEnvelopeInfo(const uint8_t *buf,
                           size_t len,
                           uint8_t expected_stream_kind,
                           streamReaderInfo *info);

int streamReaderInit(streamReader *reader, streamReaderConfig *cfg, streamReaderReadFn read_cb, void *read_ctx);

/* Returns up to len bytes, 0 on EOF, or -1 on error. An error after partial
 * output is reported on the next call. */
ssize_t streamReaderRead(streamReader *reader, void *buf, size_t len);
int streamReaderGetInfo(streamReader *reader, streamReaderInfo *info);
int streamReaderValidateEnd(streamReader *reader);
void streamReaderFree(streamReader *reader);

#endif /* COMPRESSION_STREAM_H */
