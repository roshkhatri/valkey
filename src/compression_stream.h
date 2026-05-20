/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_STREAM_H
#define COMPRESSION_STREAM_H

#include "compression.h"

/* VKCS envelope:
 *   [0..3] magic "VKCS"
 *   [4]    version (currently VKCS_VERSION)
 *   [5]    codec id
 *   [6]    flags (bit 0 = codec checksum enabled; other bits reserved)
 *   [7]    stream kind */
#define VKCS_MAGIC_0 0x56 /* 'V' */
#define VKCS_MAGIC_1 0x4B /* 'K' */
#define VKCS_MAGIC_2 0x43 /* 'C' */
#define VKCS_MAGIC_3 0x53 /* 'S' */
#define VKCS_MAGIC_SIZE 4
#define VKCS_ENVELOPE_SIZE 8
#define VKCS_VERSION 1
#define VKCS_FLAG_CODEC_CHECKSUM (1 << 0)
#define STREAM_KIND_RDB 0x00
#define STREAM_KIND_REPL 0x01

typedef enum {
    VKCS_CODEC_LZ4 = 0x01,
    VKCS_CODEC_ZSTD = 0x02,
} vkcsCodec;

typedef int (*vkcsEmitFn)(void *ctx, const uint8_t *data, size_t len);

/* Reader compressed-input/decompressed-output buffer size when cfg->buffer_size
 * is unset. Tiny caller values are clamped up so the LZ4 decoder can always
 * make forward progress without growing internal state. */
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
    size_t buffer_size;   /* 0 selects STREAM_READER_BUFFER_SIZE_DEFAULT. */
    size_t push_feed_cap; /* Push-mode only: maximum bytes buffered in the feed queue.
                           * Must be > 0 for push-mode readers; the constructor rejects 0.
                           * Ignored in pull mode. */
} streamReaderConfig;

typedef struct streamWriter streamWriter;
typedef struct streamReader streamReader;

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

/* Returns >0 bytes read, 0 on EOF, -1 on error. Partial reads allowed. */
typedef ssize_t (*streamReaderReadFn)(void *ctx, void *buf, size_t len);

streamWriter *streamWriterCreate(const streamWriterConfig *cfg,
                                 vkcsEmitFn emit_cb,
                                 void *emit_ctx);

/* Returns compressed bytes emitted to the sink (not input bytes consumed),
 * including the envelope on the first successful write. -1 on error. */
ssize_t streamWriterWrite(streamWriter *t, const void *buf, size_t len);
int streamWriterFlush(streamWriter *t);
int streamWriterFinish(streamWriter *t);
void streamWriterDestroy(streamWriter *t);
int streamWriterIsErrored(const streamWriter *t);
void streamWriterSetError(streamWriter *t);
int streamReadEnvelopeInfo(const uint8_t *buf,
                           size_t len,
                           uint8_t expected_stream_kind,
                           streamReaderInfo *info);

streamReader *streamReaderCreate(const streamReaderConfig *cfg,
                                 streamReaderReadFn read_cb,
                                 void *read_ctx);

/* Drives the wrapped read callback synchronously. Caller must ensure the
 * source can block (file rios are fine; non-blocking sources are not). */
int streamReaderProbe(streamReader *t);

/* Read up to len bytes into buf.
 * len must fit in ssize_t; larger requests return -1 without consuming input.
 * Returns:
 * - >0: bytes produced (decompressed or passthrough)
 * -  0: pull mode: EOF. push mode: either "need more feed" or "EOF after
 *       FeedEnd + full drain" — use streamReaderNeedsInput() to distinguish.
 * - -1: error */
ssize_t streamReaderRead(streamReader *t, void *buf, size_t len);
int streamReaderGetInfo(streamReader *t, streamReaderInfo *info);
streamReaderError streamReaderGetError(const streamReader *t);
int streamReaderValidateEnd(streamReader *t);
void streamReaderDestroy(streamReader *t);

/* Parse a VKCS envelope header. Returns 0 on success, -1 on error. */
int readVkcsEnvelope(const uint8_t *buf, size_t len, vkcsCodec *codec, uint8_t *stream_kind, bool *codec_checksum_enabled);

/* --- Push-mode (feed) API --- */

/* Create a push-mode reader. The caller drives input via streamReaderFeed;
 * no read callback is invoked. Returns NULL on invalid config (e.g.
 * push_feed_cap == 0) or allocation failure. */
streamReader *streamReaderCreatePush(const streamReaderConfig *cfg);

/* Feed compressed (or passthrough) bytes into a push-mode reader.
 * Returns 0 on success; -1 on error (sticky).
 * Errors:
 *   - reader is not in push mode
 *   - reader has latched an error
 *   - called after streamReaderFeedEnd with len > 0
 *   - internal feed queue would exceed cfg->push_feed_cap */
int streamReaderFeed(streamReader *t, const void *src, size_t len);

/* Signal end of input for a push-mode reader. After this call, streamReaderRead
 * will drain any remaining buffered output and then return 0 (EOF).
 * Safe to call multiple times. */
void streamReaderFeedEnd(streamReader *t);

/* Returns true if a push-mode reader has no buffered input available and more
 * input is needed before it can produce more output. Use this to distinguish
 * "need more feed" from "truly EOF" when streamReaderRead returns 0. */
bool streamReaderNeedsInput(const streamReader *t);

/* Returns true if the decompressor has reached the end of its frame.
 * For one-shot streams (RDB) this is the normal terminal state.
 * For long-lived streams (replication), frame completion mid-stream
 * indicates protocol corruption and the caller should disconnect. */
bool streamReaderFrameDone(const streamReader *t);

#endif /* COMPRESSION_STREAM_H */
