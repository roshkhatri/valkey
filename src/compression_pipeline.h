/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_PIPELINE_H
#define COMPRESSION_PIPELINE_H

#include "compression.h"
#include "rio.h"
#include "sds.h"

#include <stdatomic.h>

/* --- Sync compress config --- */
typedef struct {
    compression_algo_t algo;
    int level;
    uint8_t stream_kind; /* STREAM_KIND_RDB or STREAM_KIND_REPL */
} sync_compress_config_t;

/* --- Sync compress context (fork child, bgIteration) ---
 * Used internally by compress_rio_t. Fork-safe by design: each child
 * creates a fresh context with its own algorithm state. No shared state. */
typedef struct {
    compression_algo_t algo;
    int compression_level;
    stream_compressor_t compressor;
    uint8_t *out_buf;    /* Reusable output buffer, sized via streamCompressOutputBound */
    size_t out_buf_size; /* Current allocation size of out_buf */
    vkcsEmitFn emit_cb;  /* Returns 0 on success, -1 on error */
    void *emit_ctx;
    uint8_t stream_kind; /* STREAM_KIND_RDB or STREAM_KIND_REPL */
    int envelope_written;
    int finished; /* Set by sync_compress_finish — blocks further writes.
                   * Prevents accidental multi-frame output under one envelope. */
    int errored;  /* Sticky error flag — once set, all writes fail */
} sync_compress_ctx_t;

/* --- Compression rio decorator (RDB save) --- */
typedef struct {
    rio base; /* Must be first — allows casting to (rio *) */
    rio *inner;
    sync_compress_ctx_t compressor;
    int finalized;
} compress_rio_t;

/* --- Decompression rio decorator (RDB load) --- */
typedef struct {
    rio base; /* Must be first */
    rio *inner;
    stream_decompressor_t decompressor;
    uint8_t *read_buf;
    size_t read_buf_size;
    size_t read_buf_fill; /* Bytes of valid compressed data in read_buf (may
                           * include unconsumed remainder from previous
                           * streamDecompressFeed call). */
    uint8_t *decomp_buf;
    size_t decomp_buf_size;
    size_t decomp_buf_pos;
    size_t decomp_buf_len;
} decompress_rio_t;

/* --- Prefix-replay rio decorator (format detection) --- */
typedef struct {
    rio base; /* Must be first */
    rio *inner;
    char prefix[8];
    size_t prefix_len;
    size_t prefix_pos;
} prefix_replay_rio_t;

/* --- Accumulator (async path) --- */
typedef struct {
    sds buf;
    size_t target_size;
    long long first_byte_time; /* Monotonic microseconds */
} accumulator_t;

/* Forward declaration for async compress context */
typedef struct async_compress_ctx_t async_compress_ctx_t;

/* --- Async compress config --- */
typedef struct {
    compression_algo_t algo;
    int level;
    size_t accumulator_size;
    void (*completion_cb)(void *ctx, uint8_t *data, size_t len);
    void *cb_ctx;
    void *replica; /* client* — forward declared to avoid circular include */
} async_compress_config_t;

/* --- Async compress context (replication) --- */
struct async_compress_ctx_t {
    uint64_t generation;
    atomic_int refcount;
    atomic_int closed;
    compression_algo_t algo;
    int compression_level;
    size_t accumulator_size;
    accumulator_t acc;
    stream_compressor_t compressor;
    int envelope_written;
    void (*completion_cb)(void *ctx, uint8_t *data, size_t len);
    void *cb_ctx;
    void *replica; /* client* */
    int inflight_count;
    size_t inflight_bytes;
};

/* --- Rio Decorator API --- */
int rioInitWithCompress(compress_rio_t *cr, rio *inner, const sync_compress_config_t *cfg);
int compress_rio_finish(compress_rio_t *cr);
void compress_rio_destroy(compress_rio_t *cr);

void decompress_rio_init(decompress_rio_t *dr, rio *inner, compression_algo_t algo);
void decompress_rio_destroy(decompress_rio_t *dr);

void prefix_replay_rio_init(prefix_replay_rio_t *pr, rio *inner, const char *prefix, size_t prefix_len);

/* --- Stream Format Detection --- */
int vkcsDetectFormat(rio *inner,
                     uint8_t *header_out,
                     compression_algo_t *algo_out,
                     uint8_t *stream_kind_out);

/* --- Sync Compress API --- */
sync_compress_ctx_t *sync_compress_create(const sync_compress_config_t *cfg,
                                          vkcsEmitFn emit_cb,
                                          void *emit_ctx);
void sync_compress_write(sync_compress_ctx_t *t, const void *buf, size_t len);
void sync_compress_finish(sync_compress_ctx_t *t);
void sync_compress_destroy(sync_compress_ctx_t *t);

/* --- Async Compress API --- */
async_compress_ctx_t *async_compress_create(const async_compress_config_t *cfg);
size_t async_compress_write(async_compress_ctx_t *t, const void *buf, size_t len);
void async_compress_finish(async_compress_ctx_t *t);
void async_compress_destroy(async_compress_ctx_t *t);
void async_compress_check_timeout(async_compress_ctx_t *t, long long now_us);
void async_compress_retain(async_compress_ctx_t *t);
void async_compress_release(async_compress_ctx_t *t);

#endif /* COMPRESSION_PIPELINE_H */
