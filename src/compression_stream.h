/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_STREAM_H
#define COMPRESSION_STREAM_H

#include "compression.h"

/* Generic caller-agnostic streaming writer config.
 * Defaults:
 * - block_checksum: 0 (off)
 * - raw_frame: 0 (emit VKCS envelope before compressed frame) */
typedef struct {
    compression_algo_t algo;
    int level;
    uint8_t stream_kind; /* STREAM_KIND_RDB or STREAM_KIND_REPL */
    int block_checksum;  /* Codec checksum toggle (LZ4 block checksum) */
    int raw_frame;       /* 1 => emit raw codec frame (no VKCS envelope) */
} stream_writer_config_t;

/* Opaque writer context owned by the streaming writer API. */
typedef struct stream_writer stream_writer_t;

/* Generic streaming writer API. */
stream_writer_t *stream_writer_create(const stream_writer_config_t *cfg,
                                      vkcsEmitFn emit_cb,
                                      void *emit_ctx);
/* Returns 0 on success, -1 on error.
 * After stream_writer_finish(), write returns -1 and does not emit bytes. */
int stream_writer_write(stream_writer_t *t, const void *buf, size_t len);
/* Returns 0 on success, -1 on error.
 * Flush-after-finish is a no-op success. */
int stream_writer_flush(stream_writer_t *t);
/* Returns 0 on success, -1 on error.
 * Calling finish more than once is a no-op success. */
int stream_writer_finish(stream_writer_t *t);
void stream_writer_destroy(stream_writer_t *t);
int stream_writer_is_errored(const stream_writer_t *t);
int stream_writer_is_finished(const stream_writer_t *t);
void stream_writer_set_error(stream_writer_t *t);

#endif /* COMPRESSION_STREAM_H */
