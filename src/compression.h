/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_H
#define COMPRESSION_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/* --- Algorithm identifiers --- */
typedef enum {
    ALGO_NONE = 0x00, /* Disabled */
    ALGO_LZF = 0x01,  /* Per-string LZF (RDB only, existing behavior) */
    ALGO_LZ4 = 0x02,
} compression_algo_t;

/* --- VKCS Stream Envelope --- */
#define VKCS_MAGIC_0 0x56 /* 'V' */
#define VKCS_MAGIC_1 0x4B /* 'K' */
#define VKCS_MAGIC_2 0x43 /* 'C' */
#define VKCS_MAGIC_3 0x53 /* 'S' */
#define VKCS_ENVELOPE_SIZE 8
#define VKCS_VERSION 1
#define VKCS_FLAG_STREAM_KIND 0x01
#define VKCS_FLAG_CODEC_CHECKSUM 0x02

#define STREAM_KIND_RDB 0x00
#define STREAM_KIND_REPL 0x01
#define STREAM_KIND_ANY 0xFF

/* --- Emit callback type --- */
typedef int (*vkcsEmitFn)(void *ctx, const uint8_t *data, size_t len);

/* --- Flush modes for streaming compression --- */
typedef enum {
    FLUSH_CONTINUE = 0, /* Buffer internally */
    FLUSH_SYNC = 1,     /* Emit all buffered data, keep frame open */
    FLUSH_END = 2,      /* Finalize frame */
} compress_flush_mode_t;

/* --- LZ4 block mode (maps to LZ4F_blockMode_t) --- */
typedef enum {
    COMPRESS_BLOCK_INDEPENDENT = 0, /* LZ4F_blockIndependent — no inter-block deps */
    COMPRESS_BLOCK_LINKED = 1,      /* LZ4F_blockLinked — better ratio, needs history */
} compress_block_mode_t;

/* --- Streaming compressor context --- */
typedef struct {
    compression_algo_t algo;
    int level;
    union {
        void *lz4f; /* LZ4F_cctx* */
    } ctx;
    bool frame_started;
    bool errored;                     /* Permanently failed — algorithm state is undefined after
                                       * an error. All subsequent streamCompressFeed calls return
                                       * -1 immediately. The caller must tear down the stream
                                       * (disconnect replica / abort RDB save). No mid-stream
                                       * retry is possible because already-emitted frame bytes
                                       * cannot be unsent. */
    bool stable_src;                  /* LZ4F optimization: set to true only when the caller
                                       * guarantees the input buffer remains valid and
                                       * unmodified until the next streamCompressFeed call.
                                       * Default false (safe). Deferred async replication
                                       * paths may set this to true when feeding from
                                       * per-call input slices whose lifetime is guaranteed
                                       * across the compressor call boundary. */
    bool block_checksum;              /* Codec integrity checksum toggle.
                                       * For LZ4 streams this enables per-block checksums
                                       * (checksumming compressed block payloads). */
    compress_block_mode_t block_mode; /* LZ4 block mode: independent (default) or
                                       * linked. Independent blocks allow random
                                       * access; linked blocks improve ratio for
                                       * small values at the cost of history. */
} stream_compressor_t;

/* --- Streaming decompressor context --- */
typedef struct {
    compression_algo_t algo;
    union {
        void *lz4f; /* LZ4F_dctx* */
    } ctx;
    bool errored;    /* Permanently failed — algorithm state is undefined after
                      * an error. All subsequent streamDecompressFeed calls
                      * return -1 immediately until reinitialized. */
    bool frame_done; /* Set when the codec frame is fully decoded.
                      * Subsequent streamDecompressFeed calls return 0
                      * immediately (no more output). */
} stream_decompressor_t;

/* --- Envelope API --- */

/* Returns 1 if the algorithm supports streaming codec framing. */
int compressionAlgoSupportsStreaming(compression_algo_t algo);

/* Returns a stable name for logging/debugging. */
const char *compressionAlgoName(compression_algo_t algo);

/* Write VKCS envelope via callback.  Returns 0 on success, -1 on error
 * (invalid algo or emit_cb failure). */
int writeVkcsEnvelope(vkcsEmitFn emit_cb,
                      void *ctx,
                      compression_algo_t algo,
                      uint8_t stream_kind,
                      int codec_checksum_enabled);

/* Parse VKCS envelope from buffer.  Returns 0 on success, -1 on error.
 * On success, *algo, *stream_kind, and *codec_checksum_enabled are populated
 * when corresponding pointers are non-NULL. */
int readVkcsEnvelope(const uint8_t *buf,
                     size_t len,
                     compression_algo_t *algo,
                     uint8_t *stream_kind,
                     int *codec_checksum_enabled);

/* --- Streaming compression API --- */

/* Initialize/destroy streaming compressor. */
int streamCompressorInit(stream_compressor_t *sc, compression_algo_t algo, int level);
void streamCompressorDestroy(stream_compressor_t *sc);

/* Initialize/destroy streaming decompressor. */
int streamDecompressorInit(stream_decompressor_t *sd, compression_algo_t algo);
void streamDecompressorDestroy(stream_decompressor_t *sd);

/* Return upper bound on compressed output size.
 * frame_started: whether the algorithm frame header has already been written.
 * flush_mode: FLUSH_CONTINUE, FLUSH_SYNC, or FLUSH_END. */
size_t streamCompressOutputBound(compression_algo_t algo, size_t input_len, int frame_started, compress_flush_mode_t flush_mode);

/* Feed data through streaming compressor.
 * flush_mode: FLUSH_CONTINUE, FLUSH_SYNC, or FLUSH_END.
 * Returns bytes written to output, 0 for no output, -1 on error. */
ssize_t streamCompressFeed(stream_compressor_t *sc,
                           uint8_t *output,
                           size_t output_capacity,
                           const uint8_t *input,
                           size_t input_len,
                           compress_flush_mode_t flush_mode);

/* Feed compressed data through streaming decompressor.
 * Returns bytes written to output, 0 for no output, -1 on error.
 * *input_consumed is set to the number of compressed bytes consumed.
 * Fatal errors latch stream_decompressor_t.errored until reinit. */
ssize_t streamDecompressFeed(stream_decompressor_t *sd,
                             uint8_t *output,
                             size_t output_capacity,
                             const uint8_t *input,
                             size_t input_len,
                             size_t *input_consumed);

#endif /* COMPRESSION_H */
