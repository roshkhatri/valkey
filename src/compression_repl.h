/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_REPL_H
#define COMPRESSION_REPL_H

/* Replication compression adapter.
 *
 * This is the replication-stream counterpart to compression_rio.c: both are
 * thin adapters on top of compression_stream. compression_rio wraps a blocking
 * rio (used for RDB save/load); this file wraps the non-blocking replication
 * transport.
 *
 * Why a separate adapter (and why push-mode for the replica): the RDB path can
 * use the pull-mode streamReader because a file rio blocks until the requested
 * bytes are available. The replica instead reads its primary link from a
 * non-blocking socket inside the event loop, where a read returns whatever
 * bytes happen to be ready. A blocking pull callback cannot work there, so the
 * decoder is driven in push-mode: the event loop feeds incremental bytes and
 * drains whatever decoded output is currently available. This adapter owns that
 * push-mode reader plus its scratch buffer so the buffering lives in one place
 * rather than being spread across networking.c. */

#include "compression.h"
#include "compression_stream.h"
#include "sds.h"

/* ===== Primary-side per-replica compressor ===== */

/* Owns the streamWriter (VKCS/LZ4 frame encoder) and the staging buffer of
 * compressed bytes pending socket write for one replica connection. The
 * networking layer still drives which replication-backlog bytes get compressed
 * and performs the socket writes; this adapter owns the codec state and the
 * staging buffer so that ownership is not scattered through the client struct. */
typedef struct replCompressor {
    streamWriter writer; /* VKCS/LZ4 frame encoder (STREAM_KIND_REPL). */
    sds out_buf;         /* Compressed bytes staged for the socket. */
    size_t out_buf_pos;  /* Next unsent byte offset within out_buf. */
    size_t raw_bytes;    /* Raw backlog bytes represented by out_buf. */
} replCompressor;

/* Create a per-replica compressor. Returns NULL on error. */
replCompressor *replCompressorCreate(compressionAlgo algo, int level);

/* Destroy a compressor created by replCompressorCreate. NULL-safe. */
void replCompressorDestroy(replCompressor *rc);

/* Feed raw replication bytes into the encoder. The emit callback appends the
 * resulting compressed bytes to rc->out_buf. Returns compressed bytes emitted
 * (>=0) or -1 on error. */
ssize_t replCompressorWrite(replCompressor *rc, const void *buf, size_t len);

/* Flush the encoder so any buffered input is materialized into out_buf.
 * Returns 0 on success, -1 on error. */
int replCompressorFlush(replCompressor *rc);

/* Clear the staging buffer for a new batch and release oversized scratch so a
 * long-lived replica does not permanently retain peak allocation. */
void replCompressorResetBatch(replCompressor *rc);

/* Approximate heap usage (codec state + scratch + staging buffer) for
 * client-output-buffer accounting. */
size_t replCompressorMemUsage(const replCompressor *rc);

/* ===== Replica-side decompressor ===== */

/* Outcome of replDecompressorDecode. */
typedef enum {
    REPL_DECODE_OK = 0,          /* Decoded (possibly 0) bytes; need more input next tick. */
    REPL_DECODE_ERR = -1,        /* IO/feed/decoder error: disconnect. */
    REPL_DECODE_FRAME_DONE = -2, /* Frame ended on a live link: protocol corruption. */
    REPL_DECODE_OVERFLOW = -3,   /* Decoded output exceeded the bomb-guard cap. */
} replDecodeResult;

/* Owns the push-mode streamReader and decode scratch buffer for the single
 * primary link. See the file header for why push-mode is required here. */
typedef struct replDecompressor {
    streamReader reader; /* Push-mode VKCS/LZ4 decoder (STREAM_KIND_REPL). */
    sds decode_buf;      /* Scratch buffer holding the most recent decoded bytes. */
} replDecompressor;

/* Create a replica-side decompressor with the given feed-queue cap.
 * Returns NULL on error. */
replDecompressor *replDecompressorCreate(size_t feed_cap);

/* Destroy a decompressor created by replDecompressorCreate. NULL-safe. */
void replDecompressorDestroy(replDecompressor *rd);

/* Feed `len` raw transport bytes and drain all currently-available decoded
 * output into rd->decode_buf (which is cleared first). On REPL_DECODE_OK,
 * *out_len is set to the decoded byte count (0 means "buffered a partial frame,
 * resume next tick"). output_max bounds the decoded output as a decompression
 * bomb guard. */
replDecodeResult replDecompressorDecode(replDecompressor *rd,
                                        const void *src,
                                        size_t len,
                                        size_t output_max,
                                        size_t *out_len);

/* Access the decode scratch buffer (valid bytes = sdslen) after a decode. */
sds replDecompressorBuf(replDecompressor *rd);

#endif /* COMPRESSION_REPL_H */
