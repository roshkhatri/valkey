/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_STREAM_H
#define COMPRESSION_STREAM_H

#include "compression.h"

/* Default decode/read window for stream_reader when cfg->batch_size == 0. */
#define STREAM_READER_BATCH_SIZE_DEFAULT (1024 * 1024)

/* Generic caller-agnostic streaming writer config.
 * - raw_frame=0: emits VKCS envelope + codec frame
 * - raw_frame=1: emits codec frame only (no envelope)
 * - block_checksum: codec checksum toggle (algorithm-specific) */
typedef struct {
    compression_algo_t algo;
    int level;
    uint8_t stream_kind;              /* STREAM_KIND_RDB or STREAM_KIND_REPL */
    bool block_checksum;              /* Codec checksum toggle (LZ4 block checksum) */
    bool stable_src;                  /* Caller guarantees input remains valid until the
                                       * next write/flush on this writer. */
    bool raw_frame;                   /* true => emit raw codec frame (no VKCS envelope) */
    compress_block_mode_t block_mode; /* LZ4 block mode (default: independent) */
} stream_writer_config_t;

/* Generic caller-agnostic streaming reader config.
 * - raw_frame=0: auto-detect VKCS envelope, decode if compressed
 * - raw_frame=1: treat input as raw codec frame for cfg->algo
 * - allow_passthrough: when raw_frame=0, forward non-VKCS bytes as-is
 * - expected_stream_kind: enforce envelope stream kind when compressed
 * - batch_size=0: uses internal default window/read size */
typedef struct {
    compression_algo_t algo;      /* Required only when raw_frame=1 */
    uint8_t expected_stream_kind; /* STREAM_KIND_RDB/REPL or STREAM_KIND_ANY */
    bool raw_frame;               /* true => input is raw codec frame (no VKCS envelope) */
    bool allow_passthrough;       /* true => non-VKCS input is passed through */
    size_t batch_size;            /* Decode/read batch size; 0 => internal default */
} stream_reader_config_t;

/* Opaque writer context owned by the streaming writer API. */
typedef struct stream_writer stream_writer_t;
/* Opaque reader context owned by the streaming reader API. */
typedef struct stream_reader stream_reader_t;

typedef struct {
    bool compressed; /* true => stream is VKCS+codec compressed, false => passthrough */
    compression_algo_t algo;
    uint8_t stream_kind;         /* STREAM_KIND_RDB/REPL when compressed, STREAM_KIND_ANY otherwise */
    bool codec_checksum_enabled; /* Parsed from VKCS flags when compressed.
                                  * For LZ4 this tracks block checksum mode. */
} stream_reader_info_t;

/* Caller-provided input callback.
 * Returns:
 * - >0: bytes read into buf (partial reads allowed)
 * -  0: EOF
 * - -1: read error */
typedef ssize_t (*stream_reader_read_fn)(void *ctx, void *buf, size_t len);

/* Generic streaming writer API.
 * Ownership: returned context is owned by caller and must be destroyed.
 * Threading: stream_writer_t is NOT thread-safe; all API calls on a given
 * instance must be externally serialized and single-owner at any instant. */
stream_writer_t *stream_writer_create(const stream_writer_config_t *cfg,
                                      vkcsEmitFn emit_cb,
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
/* Returns cumulative bytes successfully passed to emit_cb for this writer. */
uint64_t stream_writer_bytes_emitted(const stream_writer_t *t);
/* Snapshot only; cross-thread readers must synchronize externally
 * (for example via waitForClientIO-equivalent quiesce). */
int stream_writer_is_errored(const stream_writer_t *t);
void stream_writer_set_error(stream_writer_t *t);

/* Generic streaming reader API.
 * Ownership: returned context is owned by caller and must be destroyed. */
stream_reader_t *stream_reader_create(const stream_reader_config_t *cfg,
                                      stream_reader_read_fn read_cb,
                                      void *read_ctx);
/* Ensure stream mode is detected and metadata is available.
 * Safe to call more than once.
 * Returns 0 on success, -1 on error. */
int stream_reader_probe(stream_reader_t *t);
/* Read up to len bytes into buf.
 * Returns:
 * - >0: bytes produced (decompressed or passthrough)
 * -  0: EOF
 * - -1: error */
ssize_t stream_reader_read(stream_reader_t *t, void *buf, size_t len);
/* Populate stream metadata after probing.
 * For passthrough streams: compressed=0, algo=ALGO_NONE,
 * stream_kind=STREAM_KIND_ANY, codec_checksum_enabled=false.
 * Returns 0 on success, -1 on error. */
int stream_reader_get_info(stream_reader_t *t, stream_reader_info_t *info);
void stream_reader_destroy(stream_reader_t *t);

#endif /* COMPRESSION_STREAM_H */
