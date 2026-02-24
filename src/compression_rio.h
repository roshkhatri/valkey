/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_RIO_H
#define COMPRESSION_RIO_H

#include "compression_stream.h"
#include "rio.h"

/* --- Compression rio decorator (RDB save) --- */
typedef struct {
    rio base; /* Must be first — allows casting to (rio *) */
    rio *inner;
    stream_writer_t *compressor;
    int finalized;
} compress_rio_t;

/* --- Decompression rio decorator (RDB load) --- */
typedef struct {
    rio base; /* Must be first */
    rio *inner;
    stream_decompressor_t decompressor;
    uint8_t *read_buf;
    size_t read_buf_size;
    size_t read_buf_pos;  /* Start offset of valid data in read_buf */
    size_t read_buf_fill; /* Bytes of valid compressed data in read_buf
                           * starting at read_buf_pos. */
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

/* --- Rio Decorator API --- */
int rioInitWithCompress(compress_rio_t *cr, rio *inner, const stream_writer_config_t *cfg);
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

#endif /* COMPRESSION_RIO_H */
