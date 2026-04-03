/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef REPL_STREAM_H
#define REPL_STREAM_H

#include "compression.h"
#include "sds.h"

typedef struct repl_stream_decoder repl_stream_decoder_t;

/* Incremental replication transport decoder.
 * The decoder auto-detects an initial VKCS envelope, falls back to
 * passthrough for uncompressed streams, and preserves compressed-state
 * across multiple feed calls. */
repl_stream_decoder_t *replStreamDecoderCreate(void);
int replStreamDecoderFeed(repl_stream_decoder_t *decoder, const void *src, size_t len, sds *dst);
void replStreamDecoderDestroy(repl_stream_decoder_t *decoder);

#endif /* REPL_STREAM_H */
