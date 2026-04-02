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
    bool finalized;
} compress_rio_t;

/* --- Decompression rio decorator (RDB load) --- */
typedef struct {
    rio base; /* Must be first */
    rio *inner;
    stream_reader_t *reader;
    bool info_ready;
} decompress_rio_t;

/* --- Rio Decorator API --- */
int rioInitWithCompress(compress_rio_t *cr, rio *inner, const stream_writer_config_t *cfg);
int compress_rio_finish(compress_rio_t *cr);
void compress_rio_destroy(compress_rio_t *cr);

/* Initialize with explicit reader config.
 * The wrapped source must provide synchronous reads. */
int decompress_rio_init_with_config(decompress_rio_t *dr, rio *inner, const stream_reader_config_t *cfg);
/* Retrieve probed stream metadata (compressed/algo/kind). */
int decompress_rio_get_info(decompress_rio_t *dr, stream_reader_info_t *info);
void decompress_rio_destroy(decompress_rio_t *dr);

#endif /* COMPRESSION_RIO_H */
