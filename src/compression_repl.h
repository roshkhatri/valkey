/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_REPL_H
#define COMPRESSION_REPL_H

/* Replication compression adapter on top of compression_stream. The rio-based
 * streamWriter/streamReader path serves blocking RDB save/load; replication
 * reads a non-blocking socket in the event loop where a pull-mode reader
 * cannot work, so this adapter is push-mode. The incoming stream may be
 * compressed (VCS envelope) or plaintext (primary compresses only if its own
 * config is enabled) and is classified from its leading bytes. */

#include "compression.h"
#include "compression_stream.h"
#include "sds.h"

/* Max raw replication-backlog bytes compressed per write dispatch cycle;
 * bounds per-batch latency and the staging buffer. */
#define REPL_COMPRESSION_BATCH_LIMIT (1024 * 1024)

/* ===== Primary-side per-replica compressor ===== */

typedef struct replCompressor {
    streamWriter writer; /* VCS/LZ4 frame encoder (VCS_STREAM_REPL). */
    sds out_buf;         /* Compressed bytes staged for the socket. */
    size_t out_buf_pos;  /* Next unsent byte offset within out_buf. */
    size_t raw_bytes;    /* Raw backlog bytes represented by out_buf. */
} replCompressor;

/* Compressor API. Write and Flush return C_OK on success and C_ERR on error;
 * the encoder compresses directly into out_buf. Create returns NULL on error;
 * Destroy is NULL-safe. */
replCompressor *replCompressorCreate(compressionAlgo algo);
void replCompressorDestroy(replCompressor *rc);
int replCompressorWrite(replCompressor *rc, const void *buf, size_t len);
int replCompressorFlush(replCompressor *rc);
/* Clears the staging buffer for a new batch; reclaims only oversized payloads. */
void replCompressorResetBatch(replCompressor *rc);
/* Approximate heap usage for client-output-buffer accounting. */
size_t replCompressorMemUsage(const replCompressor *rc);
/* Algorithm the compressor was initialized with; ALGO_NONE if rc is NULL. */
compressionAlgo replCompressorAlgo(const replCompressor *rc);

/* ===== Replica-side decompressor ===== */

typedef enum {
    REPL_DECODE_OK = 0,          /* Decoded (possibly 0) bytes; need more input next tick. */
    REPL_DECODE_ERR = -1,        /* IO/feed/decoder error: disconnect. */
    REPL_DECODE_FRAME_DONE = -2, /* Frame ended on a live link: protocol corruption. */
    REPL_DECODE_OVERFLOW = -3,   /* Decoded output exceeded the bomb-guard cap. */
} replDecodeResult;

typedef enum {
    REPL_DECODE_MODE_PROBE = 0,  /* Still classifying the stream. */
    REPL_DECODE_MODE_COMPRESSED, /* VCS envelope seen; decoder initialized. */
    REPL_DECODE_MODE_PASSTHROUGH /* Non-VCS stream; bytes forwarded as-is. */
} replDecodeMode;

/* Decoder for the single primary link. Accumulates the leading bytes (they may
 * span several reads), classifies the stream, then either feeds the codec or
 * forwards plaintext untouched. */
typedef struct replDecompressor {
    streamDecompressor decompressor; /* Valid once mode == COMPRESSED. */
    replDecodeMode mode;
    uint8_t envelope[VCS_ENVELOPE_SIZE]; /* Leading bytes gathered during PROBE. */
    size_t envelope_len;
    sds decode_buf; /* Most recent decoded bytes. */
} replDecompressor;

/* Decoder API. Create returns NULL on error; Destroy is NULL-safe. Decode
 * feeds len transport bytes and drains decoded output into decode_buf
 * (cleared first). On REPL_DECODE_OK, *out_len is the decoded byte count
 * (0 means a partial frame was buffered; resume next tick). output_max caps
 * decoded output as a decompression-bomb guard. */
replDecompressor *replDecompressorCreate(void);
void replDecompressorDestroy(replDecompressor *rd);
replDecodeResult replDecompressorDecode(replDecompressor *rd,
                                        const void *src,
                                        size_t len,
                                        size_t output_max,
                                        size_t *out_len);
/* Decode scratch buffer (valid bytes = sdslen) after a decode. */
sds replDecompressorBuf(replDecompressor *rd);
/* True once the probe classified the stream as plaintext (non-VCS). */
bool replDecompressorIsPassthrough(const replDecompressor *rd);
/* True once the probe classified the stream as compressed (VCS envelope). */
bool replDecompressorIsCompressed(const replDecompressor *rd);

#endif /* COMPRESSION_REPL_H */
