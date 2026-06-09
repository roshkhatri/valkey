/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_STREAM_H
#define COMPRESSION_STREAM_H

#include "compression.h"
#include "sds.h"

/* VKCS envelope:
 *   [0..3] magic "VKCS"
 *   [4]    version (currently VKCS_VERSION)
 *   [5]    codec id
 *   [6]    flags (bit 0 = codec checksum enabled; other bits reserved)
 *   [7]    stream kind
 *
 * All fields are single-byte in version 1. Future multi-byte fields must use
 * network byte order. */
#define VKCS_MAGIC_0 0x56 /* 'V' */
#define VKCS_MAGIC_1 0x4B /* 'K' */
#define VKCS_MAGIC_2 0x43 /* 'C' */
#define VKCS_MAGIC_3 0x53 /* 'S' */
#define VKCS_MAGIC_SIZE 4
#define VKCS_ENVELOPE_SIZE 8
#define VKCS_VERSION 1
#define VKCS_FLAG_CODEC_CHECKSUM (1 << 0)

/* Byte offsets of each envelope field. */
#define VKCS_OFFSET_VERSION 4
#define VKCS_OFFSET_ALGO 5
#define VKCS_OFFSET_FLAGS 6
#define VKCS_OFFSET_STREAM_KIND 7

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

/* When allow_passthrough is set, non-VKCS input is forwarded as raw bytes;
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
    uint64_t bytes_emitted;
} streamWriter;

typedef struct streamReader {
    streamReaderReadFn read_cb;
    void *read_ctx;

    struct {
        bool allow_passthrough;
        uint8_t expected_stream_kind;
    } probe_cfg;
    struct {
        uint8_t header[VKCS_ENVELOPE_SIZE];
        size_t header_len;
        bool ready;
        bool compressed;
        bool codec_checksum_enabled;
        compressionAlgo algo;
        uint8_t stream_kind;
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

    /* Push-mode (feed) state. Inactive for pull-mode readers created via
     * streamReaderInit (push_mode=false, feed_queue=NULL). */
    bool push_mode;
    sds feed_queue;   /* Buffer of fed-but-unconsumed bytes (push mode only). */
    size_t feed_head; /* Consumed-up-to cursor within feed_queue. */
    bool feed_eof;    /* Set by streamReaderFeedEnd. */
    size_t feed_cap;  /* Max bytes allowed to buffer in feed_queue. */
} streamReader;

/* The writer pushes compressed bytes to a streamWriterEmitFn sink; the reader
 * pulls from a streamReaderReadFn source. streamWriterFinish must run before
 * freeing, since it emits the frame end; a writer freed without it is
 * truncated. The reader probes the envelope on the first read, so
 * streamReaderProbe and streamReaderGetInfo are only needed to classify the
 * stream up front. */
int streamWriterInit(streamWriter *writer, streamWriterConfig *cfg, streamWriterEmitFn emit_fn, void *emit_ctx);

/* Returns compressed bytes emitted to the sink (not input bytes consumed),
 * including the envelope on the first successful write. -1 on error. */
ssize_t streamWriterWrite(streamWriter *writer, const void *buf, size_t len);
int streamWriterFlush(streamWriter *writer);
int streamWriterFinish(streamWriter *writer);
void streamWriterFree(streamWriter *writer);
int streamReadEnvelopeInfo(const uint8_t *buf,
                           size_t len,
                           uint8_t expected_stream_kind,
                           streamReaderInfo *info);

int streamReaderInit(streamReader *reader, streamReaderConfig *cfg, streamReaderReadFn read_cb, void *read_ctx);

/* Drives the wrapped read callback synchronously. Caller must ensure the
 * source can block (file rios are fine; non-blocking sources are not). */
int streamReaderProbe(streamReader *reader);

/* Full or fail: returns len on success, 0 on EOF, -1 on error. */
ssize_t streamReaderRead(streamReader *reader, void *buf, size_t len);
int streamReaderGetInfo(streamReader *reader, streamReaderInfo *info);
int streamReaderValidateEnd(streamReader *reader);
void streamReaderFree(streamReader *reader);

/* Approximate scratch/codec memory held by the writer, for client-output-buffer
 * accounting. */
size_t streamWriterMemUsage(const streamWriter *writer);

/* --- Push-mode (feed) API ---
 * The replica replication link reads from a non-blocking socket in the event
 * loop and cannot use the blocking pull callback. A push-mode reader is fed
 * bytes incrementally via streamReaderFeed and drained opportunistically via
 * streamReaderRead, which returns 0 (not an error) when it needs more input. */
int streamReaderInitPush(streamReader *reader, streamReaderConfig *cfg, size_t feed_cap);
int streamReaderFeed(streamReader *reader, const void *src, size_t len);
void streamReaderFeedEnd(streamReader *reader);
bool streamReaderNeedsInput(const streamReader *reader);
bool streamReaderFrameDone(const streamReader *reader);

#endif /* COMPRESSION_STREAM_H */
