/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_STREAM_H
#define COMPRESSION_STREAM_H

#include "compression.h"

/* --- VKCS stream envelope --- */
#define VKCS_MAGIC_0 0x56 /* 'V' */
#define VKCS_MAGIC_1 0x4B /* 'K' */
#define VKCS_MAGIC_2 0x43 /* 'C' */
#define VKCS_MAGIC_3 0x53 /* 'S' */
#define VKCS_ENVELOPE_SIZE 8
#define VKCS_VERSION 1
#define VKCS_FLAG_CODEC_CHECKSUM (1 << 0)
#define STREAM_KIND_RDB 0x00
#define STREAM_KIND_REPL 0x01

typedef enum {
    VKCS_CODEC_LZ4 = 0x01,
} vkcs_codec_t;

/* Emit callback used by the VKCS envelope and streaming writer. */
typedef int (*vkcs_emit_fn)(void *ctx, const uint8_t *data, size_t len);

/* Default fixed buffer size for stream_reader when cfg->batch_size == 0.
 * The reader uses one compressed input buffer and one decompressed output
 * window of this size. */
#define STREAM_READER_BATCH_SIZE_DEFAULT (1024 * 1024)
/* Tiny caller-provided batches are rounded up so the current LZ4 streaming
 * decoder can always make forward progress without growing internal state. */
#define STREAM_READER_BATCH_SIZE_MIN (128 * 1024)

/* Streaming writer config. */
typedef struct {
    compression_algo_t algo;
    int level;
    uint8_t stream_kind; /* Concrete on-wire stream kind. */
    bool codec_checksum; /* Enable codec-native integrity checks when supported. */
} stream_writer_config_t;

/* Streaming reader config.
 * - auto-detect VKCS envelope, decode if compressed
 * - allow_passthrough: forward non-VKCS bytes as-is
 * - expected_stream_kind: enforce envelope stream kind when compressed
 * - batch_size=0: uses the internal default fixed buffer/window size
 * - batch_size>0: clamped to an internal minimum before allocating buffers */
typedef struct {
    uint8_t expected_stream_kind; /* Concrete stream kind to enforce for compressed input. */
    bool allow_passthrough;       /* true => non-VKCS input is passed through */
    size_t batch_size;            /* Fixed compressed read buffer and output window size; 0 => internal default */
} stream_reader_config_t;

/* Opaque writer context owned by the streaming writer API. */
typedef struct stream_writer stream_writer_t;
/* Opaque reader context owned by the streaming reader API. */
typedef struct stream_reader stream_reader_t;

typedef struct {
    bool compressed;             /* true => stream is VKCS+codec compressed, false => passthrough */
    bool codec_checksum_enabled; /* Parsed VKCS checksum policy. Ignore when compressed is false. */
    compression_algo_t algo;
    uint8_t stream_kind; /* Parsed VKCS kind. Ignore when compressed is false. */
} stream_reader_info_t;

typedef enum {
    STREAM_READER_ERROR_NONE = 0,
    STREAM_READER_ERROR_IO = 1,
    STREAM_READER_ERROR_INCOMPATIBLE = 2,
    STREAM_READER_ERROR_CORRUPT = 3,
} stream_reader_error_t;

/* Caller-provided input callback.
 * Returns:
 * - >0: bytes read into buf (partial reads allowed)
 * -  0: EOF
 * - -1: read error */
typedef ssize_t (*stream_reader_read_fn)(void *ctx, void *buf, size_t len);

/* Write VKCS envelope via callback. Returns 0 on success, -1 on error
 * (invalid codec or emit_cb failure). */
int write_vkcs_envelope(vkcs_emit_fn emit_cb,
                        void *ctx,
                        vkcs_codec_t codec,
                        uint8_t stream_kind,
                        bool codec_checksum_enabled);

/* Parse VKCS envelope from buffer. Returns 0 on success, -1 on error.
 * On success, *codec and *stream_kind are populated when corresponding pointers
 * are non-NULL. */
int read_vkcs_envelope(const uint8_t *buf,
                       size_t len,
                       vkcs_codec_t *codec,
                       uint8_t *stream_kind,
                       bool *codec_checksum_enabled);

/* Streaming writer API.
 * Ownership: returned context is owned by caller and must be destroyed.
 * Threading: stream_writer_t is NOT thread-safe; all API calls on a given
 * instance must be externally serialized and single-owner at any instant. */
stream_writer_t *stream_writer_create(const stream_writer_config_t *cfg,
                                      vkcs_emit_fn emit_cb,
                                      void *emit_ctx);
/* Returns emitted bytes for this call (>=0), -1 on error.
 * After stream_writer_finish(), write returns -1 and does not emit bytes. */
ssize_t stream_writer_write(stream_writer_t *t, const void *buf, size_t len);
/* Returns 0 on success, -1 on error.
 * Flush-after-finish is a no-op success. */
int stream_writer_flush(stream_writer_t *t);
/* Returns 0 on success, -1 on error.
 * Calling finish more than once is a no-op success. */
int stream_writer_finish(stream_writer_t *t);
void stream_writer_destroy(stream_writer_t *t);
/* Snapshot only; cross-thread readers must synchronize externally
 * (for example via waitForClientIO-equivalent quiesce). */
int stream_writer_is_errored(const stream_writer_t *t);
void stream_writer_set_error(stream_writer_t *t);

/* Streaming reader API.
 * Ownership: returned context is owned by caller and must be destroyed. */
stream_reader_t *stream_reader_create(const stream_reader_config_t *cfg,
                                      stream_reader_read_fn read_cb,
                                      void *read_ctx);
/* Ensure stream mode is detected and metadata is available.
 * Safe to call more than once.
 * Returns 0 on success, -1 on error. */
int stream_reader_probe(stream_reader_t *t);
/* Read up to len bytes into buf.
 * len must fit in ssize_t; larger requests return -1 without consuming input.
 * Returns:
 * - >0: bytes produced (decompressed or passthrough)
 * -  0: EOF
 * - -1: error */
ssize_t stream_reader_read(stream_reader_t *t, void *buf, size_t len);
/* Populate stream metadata after probing.
 * For passthrough streams: compressed=0, algo=ALGO_NONE, stream_kind=0.
 * Returns 0 on success, -1 on error. */
int stream_reader_get_info(stream_reader_t *t, stream_reader_info_t *info);
stream_reader_error_t stream_reader_get_error(const stream_reader_t *t);
/* Drain and discard any remaining decompressed bytes in the current frame.
 * After success, any pending raw input exposed by stream_reader_get_pending_input()
 * belongs to data following the compressed frame. */
int stream_reader_finish(stream_reader_t *t);
/* Finish the current frame/prefix and expose any raw input already pulled from
 * the source but not yet consumed by the reader. This is the handoff point for
 * callers that need to continue reading from the wrapped transport. */
int stream_reader_detach(stream_reader_t *t, const uint8_t **buf, size_t *len);
/* Expose unread raw input bytes that were pulled from the source but not yet
 * consumed by the reader. For passthrough streams this is the unread probe
 * prefix. For compressed streams, callers that need bytes following the
 * current frame should use stream_reader_detach() or stream_reader_finish()
 * first; calling this mid-frame may return compressed input that still belongs
 * to the active frame. */
int stream_reader_get_pending_input(stream_reader_t *t, const uint8_t **buf, size_t *len);
void stream_reader_destroy(stream_reader_t *t);

#endif /* COMPRESSION_STREAM_H */
