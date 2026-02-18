/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Rio decorators (compress_rio, decompress_rio, prefix_replay_rio) and
 * async compress context for replication compression pipeline. */

#include "compression_pipeline.h"
#include "zmalloc.h"
#include <string.h>

/* ===================================================================
 * Sync Compress API
 * Used internally by compress_rio_t. Fork-safe by design.
 * =================================================================== */

/* Allocate a sync compress context with a fresh algorithm context.
 * No shared state — each context is independent and fork-safe.
 * Returns NULL on error (bad config, algorithm init failure). */
sync_compress_ctx_t *sync_compress_create(const sync_compress_config_t *cfg,
                                          vkcsEmitFn emit_cb,
                                          void *emit_ctx) {
    if (!cfg || !emit_cb) return NULL;
    /* Only LZ4 is supported for now. */
    if (cfg->algo != ALGO_LZ4) return NULL;

    sync_compress_ctx_t *t = zmalloc(sizeof(*t));
    memset(t, 0, sizeof(*t));
    t->algo = cfg->algo;
    t->compression_level = cfg->level;
    t->emit_cb = emit_cb;
    t->emit_ctx = emit_ctx;
    t->stream_kind = cfg->stream_kind;
    t->envelope_written = 0;
    t->errored = 0;

    if (streamCompressorInit(&t->compressor, cfg->algo, cfg->level) != 0) {
        zfree(t);
        return NULL;
    }

    /* Pre-allocate output buffer for typical writes. Will be resized
     * as needed in sync_compress_write. */
    t->out_buf_size = 0;
    t->out_buf = NULL;

    return t;
}

/* Destroy a sync compress context, freeing the compressor and buffers.
 * Does NOT finalize the frame — call sync_compress_finish first.
 * Safe to call on NULL. */
void sync_compress_destroy(sync_compress_ctx_t *t) {
    if (!t) return;
    streamCompressorDestroy(&t->compressor);
    if (t->out_buf) {
        zfree(t->out_buf);
        t->out_buf = NULL;
    }
    zfree(t);
}

/* Ensure the output buffer is large enough for the given input.
 * Reuses the existing buffer when possible to avoid per-write allocation.
 * zmalloc aborts on OOM, so this cannot fail. */
static void syncCompressEnsureOutBuf(sync_compress_ctx_t *t, size_t input_len, compress_flush_mode_t flush_mode) {
    size_t needed = streamCompressOutputBound(t->algo, input_len,
                                              t->compressor.frame_started, flush_mode);
    if (needed == 0) {
        /* Ensure a minimal valid buffer so streamCompressFeed never gets NULL */
        if (t->out_buf == NULL) {
            t->out_buf = zmalloc(64);
            t->out_buf_size = 64;
        }
        return;
    }
    if (needed > t->out_buf_size) {
        zfree(t->out_buf);
        t->out_buf = zmalloc(needed);
        t->out_buf_size = needed;
    }
}

/* Write data through the sync compressor.
 * On first call, emits the VKCS stream envelope before any compressed data.
 * Feeds data through streamCompressFeed with flush_mode=FLUSH_CONTINUE.
 * Emits compressed output via the emit callback.
 * On error, sets the sticky error flag — all subsequent writes fail. */
void sync_compress_write(sync_compress_ctx_t *t, const void *buf, size_t len) {
    if (!t || t->errored || t->finished) return;
    if (len == 0) return;

    /* Emit envelope on first write */
    if (!t->envelope_written) {
        if (writeVkcsEnvelope(t->emit_cb, t->emit_ctx, t->algo, t->stream_kind) != 0) {
            t->errored = 1;
            return;
        }
        t->envelope_written = 1;
    }

    /* Ensure output buffer is large enough */
    syncCompressEnsureOutBuf(t, len, FLUSH_CONTINUE);

    uint8_t *out_ptr = t->out_buf;
    ssize_t compressed = streamCompressFeed(&t->compressor, &out_ptr,
                                            t->out_buf_size,
                                            (const uint8_t *)buf, len,
                                            FLUSH_CONTINUE);
    if (compressed < 0) {
        t->errored = 1;
        return;
    }

    /* Emit compressed output if any was produced */
    if (compressed > 0) {
        if (t->emit_cb(t->emit_ctx, out_ptr, (size_t)compressed) != 0) {
            t->errored = 1;
            return;
        }
    }
}

/* Finalize the compression frame (flush_mode=FLUSH_END).
 * Emits the final compressed output including the frame end mark.
 * After this call, the compressor cannot be used for further writes. */
void sync_compress_finish(sync_compress_ctx_t *t) {
    if (!t || t->errored || t->finished) return;
    t->finished = 1;

    /* If nothing was ever written, emit envelope + empty frame end */
    if (!t->envelope_written) {
        if (writeVkcsEnvelope(t->emit_cb, t->emit_ctx, t->algo, t->stream_kind) != 0) {
            t->errored = 1;
            return;
        }
        t->envelope_written = 1;
    }

    /* Ensure output buffer is large enough for finalization */
    syncCompressEnsureOutBuf(t, 0, FLUSH_END);

    uint8_t *out_ptr = t->out_buf;
    ssize_t compressed = streamCompressFeed(&t->compressor, &out_ptr,
                                            t->out_buf_size,
                                            NULL, 0, FLUSH_END);
    if (compressed < 0) {
        t->errored = 1;
        return;
    }

    if (compressed > 0) {
        if (t->emit_cb(t->emit_ctx, out_ptr, (size_t)compressed) != 0) {
            t->errored = 1;
            return;
        }
    }
}

/* ===================================================================
 * Compression Rio Decorator
 * Wraps an inner rio for transparent compression on write.
 * Used by BGSAVE (fork child) and diskless sync.
 *
 * RDB CHECKSUM SEMANTICS: rdbSaveRio() computes CRC64 via rioWrite()
 * on the outer rio. The decorator's base.update_cksum checksums the
 * UNCOMPRESSED bytes before compression. On load, decompress_rio_t
 * returns uncompressed bytes, so the RDB CRC64 matches.
 * =================================================================== */

/* Emit callback for compress_rio: writes compressed bytes to inner rio.
 * Returns 0 on success, -1 on error. */
static int compressRioEmit(void *ctx, const uint8_t *data, size_t len) {
    compress_rio_t *cr = (compress_rio_t *)ctx;
    if (rioWrite(cr->inner, data, len) == 0) return -1;
    return 0;
}

/* rio vtable: write callback — compress then delegate to inner rio */
static size_t compressRioWrite(rio *r, const void *buf, size_t len) {
    compress_rio_t *cr = (compress_rio_t *)r;
    if (cr->finalized || cr->compressor.errored) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    sync_compress_write(&cr->compressor, buf, len);
    if (cr->compressor.errored) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    return len; /* rio write returns 0 on error, non-zero on success */
}

/* rio vtable: read callback — compress_rio is write-only */
static size_t compressRioRead(rio *r, void *buf, size_t len) {
    (void)r;
    (void)buf;
    (void)len;
    return 0; /* Not supported — compress_rio is write-only */
}

/* rio vtable: tell callback — returns processed bytes from base */
static off_t compressRioTell(rio *r) {
    return (off_t)r->processed_bytes;
}

/* rio vtable: flush callback — algorithm flush (emit buffered data,
 * keep frame open) + inner flush. Does NOT end the frame.
 * This is critical because some call sites flush mid-stream. */
static int compressRioFlush(rio *r) {
    compress_rio_t *cr = (compress_rio_t *)r;
    if (cr->finalized || cr->compressor.errored) return 0;

    /* Only flush if we've started writing (envelope + frame exist) */
    if (cr->compressor.envelope_written && cr->compressor.compressor.frame_started) {
        syncCompressEnsureOutBuf(&cr->compressor, 0, FLUSH_SYNC);

        uint8_t *out_ptr = cr->compressor.out_buf;
        ssize_t compressed = streamCompressFeed(&cr->compressor.compressor,
                                                &out_ptr,
                                                cr->compressor.out_buf_size,
                                                NULL, 0, FLUSH_SYNC);
        if (compressed < 0) {
            cr->compressor.errored = 1;
            return 0;
        }
        if (compressed > 0) {
            if (rioWrite(cr->inner, out_ptr, (size_t)compressed) == 0) {
                cr->compressor.errored = 1;
                return 0;
            }
        }
    }

    /* Flush inner rio */
    if (cr->inner->flush) {
        return cr->inner->flush(cr->inner);
    }
    return 1;
}

/* Initialize a compression rio decorator wrapping an inner rio.
 * Sets up the rio vtable so callers can use standard rioWrite/rioFlush.
 * The compressor is initialized with a fresh algorithm context (fork-safe). */
/* Returns 0 on success, -1 on failure (e.g., compressor init failed). */
int rioInitWithCompress(compress_rio_t *cr, rio *inner, const sync_compress_config_t *cfg) {
    if (!cr || !inner || !cfg) return -1;

    memset(cr, 0, sizeof(*cr));

    /* Set up rio vtable */
    cr->base.read = compressRioRead;
    cr->base.write = compressRioWrite;
    cr->base.tell = compressRioTell;
    cr->base.flush = compressRioFlush;
    /* Checksum the UNCOMPRESSED bytes (RDB CRC64 integrity).
     * Do NOT propagate to inner rio — avoid double-checksumming. */
    cr->base.update_cksum = rioGenericUpdateChecksum;
    cr->base.cksum = 0;
    cr->base.flags = RIO_FLAG_STREAMING_COMPRESSION;
    cr->base.processed_bytes = 0;
    cr->base.max_processing_chunk = 0;

    cr->inner = inner;
    cr->finalized = 0;

    /* Initialize the sync compressor inline (not heap-allocated).
     * We initialize the compressor fields directly since the struct
     * is embedded, not heap-allocated via sync_compress_create. */
    cr->compressor.algo = cfg->algo;
    cr->compressor.compression_level = cfg->level;
    cr->compressor.emit_cb = compressRioEmit;
    cr->compressor.emit_ctx = cr;
    cr->compressor.stream_kind = cfg->stream_kind;
    cr->compressor.envelope_written = 0;
    cr->compressor.errored = 0;
    cr->compressor.out_buf = NULL;
    cr->compressor.out_buf_size = 0;

    if (streamCompressorInit(&cr->compressor.compressor, cfg->algo, cfg->level) != 0) {
        cr->compressor.errored = 1;
        return -1;
    }
    return 0;
}

/* Finalize the compression frame and flush inner rio.
 * Must be called exactly once at end of stream.
 * Idempotent: safe to call multiple times (second call is a no-op). */
/* Returns 0 on success, -1 if the compressor or inner flush errored. */
int compress_rio_finish(compress_rio_t *cr) {
    if (!cr) return -1;
    if (cr->finalized) return cr->compressor.errored ? -1 : 0;
    cr->finalized = 1;

    sync_compress_finish(&cr->compressor);

    /* Flush inner rio to ensure all bytes reach the destination.
     * Propagate flush failure to the compressor error state so
     * callers can detect it. */
    if (cr->inner->flush) {
        if (cr->inner->flush(cr->inner) == 0) {
            cr->compressor.errored = 1;
        }
    }
    return cr->compressor.errored ? -1 : 0;
}

/* Free compressor context and buffers. Does NOT finalize the frame.
 * Call compress_rio_finish() first on all exit paths. */
void compress_rio_destroy(compress_rio_t *cr) {
    if (!cr) return;
    streamCompressorDestroy(&cr->compressor.compressor);
    if (cr->compressor.out_buf) {
        zfree(cr->compressor.out_buf);
        cr->compressor.out_buf = NULL;
    }
}

/* ===================================================================
 * VKCS Format Detection
 * =================================================================== */

/* Detect whether a rio stream starts with a VKCS envelope.
 * Reads VKCS_ENVELOPE_SIZE bytes from `inner` into `header_out`.
 * If the magic matches, parses the envelope and populates `algo_out`
 * and `stream_kind_out`.
 * Returns 1 if VKCS detected and parsed, 0 if not VKCS (header still
 * populated for caller to inspect), -1 on read error. */
int vkcsDetectFormat(rio *inner,
                     uint8_t *header_out,
                     compression_algo_t *algo_out,
                     uint8_t *stream_kind_out) {
    if (rioRead(inner, header_out, VKCS_ENVELOPE_SIZE) == 0) {
        return -1;
    }

    if (header_out[0] == VKCS_MAGIC_0 && header_out[1] == VKCS_MAGIC_1 &&
        header_out[2] == VKCS_MAGIC_2 && header_out[3] == VKCS_MAGIC_3) {
        if (readVkcsEnvelope(header_out, VKCS_ENVELOPE_SIZE, algo_out, stream_kind_out) != 0) {
            return -1;
        }
        return 1;
    }

    return 0;
}

/* ===================================================================
 * Decompression Rio Decorator
 * Wraps an inner rio for transparent decompression on read.
 * Used by RDB load.
 *
 * Implements a state machine:
 * 1. If decomp_buf has available bytes, serve them immediately
 * 2. Else, read more compressed bytes from inner rio
 * 3. Decompress into decomp_buf
 * 4. Repeat until requested bytes are available or EOF/error
 * =================================================================== */

#define DECOMPRESS_INITIAL_BUF_SIZE (64 * 1024)     /* 64KB initial buffer */
#define DECOMPRESS_READ_CHUNK_SIZE (64 * 1024)      /* 64KB read chunk */
#define DECOMPRESS_MAX_BUF_SIZE (256 * 1024 * 1024) /* 256MB safety limit */

/* Read up to `len` bytes from the inner rio into `buf`.
 * Returns the number of bytes actually read (may be less than len).
 * Returns 0 on EOF or error.
 *
 * For file rios, uses fread(buf, 1, len, fp) which returns partial reads
 * correctly. For other rio types (buffer, conn, fd), falls back to the
 * standard rioRead which is all-or-nothing per call.
 *
 * Note: this bypasses the inner rio's update_cksum for file rios. That's
 * correct because the inner rio is the raw compressed stream — checksumming
 * happens on the decompressed bytes in the outer rio (rdbLoadRioWithLoadingCtx). */
static size_t decompressRioReadPartial(rio *inner, void *buf, size_t len) {
    if (rioCheckType(inner) == RIO_TYPE_FILE) {
        /* fread(buf, 1, len, fp) returns the number of bytes actually read,
         * handling short reads correctly without losing data. */
        size_t got = fread(buf, 1, len, inner->io.file.fp);
        if (got > 0) inner->processed_bytes += got;
        return got;
    }
    /* For buffer/conn/fd rios: rioRead is all-or-nothing but buffer rios
     * don't consume bytes on failure. Clear the error flag for buffer rios
     * so subsequent reads can retry. For conn/fd rios, a failed read is
     * a real error — don't mask it. */
    if (rioRead(inner, buf, len) != 0) return len;
    if (rioCheckType(inner) == RIO_TYPE_BUFFER) {
        inner->flags &= ~RIO_FLAG_READ_ERROR;
    }
    return 0;
}

/* Decompress data in read_buf into decomp_buf. Grows decomp_buf as needed.
 * Preserves unconsumed bytes at the front of read_buf.
 * Returns -1 on decompressor error, 0 otherwise. */
static int decompressDrainReadBuf(decompress_rio_t *dr) {
    size_t src_offset = 0;
    while (src_offset < dr->read_buf_fill) {
        if (dr->decomp_buf_len >= dr->decomp_buf_size) {
            size_t new_size = dr->decomp_buf_size * 2;
            if (new_size < dr->decomp_buf_size || new_size > DECOMPRESS_MAX_BUF_SIZE)
                return -1;
            dr->decomp_buf = zrealloc(dr->decomp_buf, new_size);
            dr->decomp_buf_size = new_size;
        }
        size_t out_space = dr->decomp_buf_size - dr->decomp_buf_len;
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &dr->decompressor,
            dr->decomp_buf + dr->decomp_buf_len, out_space,
            dr->read_buf + src_offset,
            dr->read_buf_fill - src_offset, &consumed);
        if (produced < 0) return -1;
        dr->decomp_buf_len += (size_t)produced;
        src_offset += consumed;
        if (consumed == 0 && produced == 0) break;
    }
    /* Preserve unconsumed bytes at front of read_buf */
    if (src_offset > 0 && src_offset < dr->read_buf_fill) {
        memmove(dr->read_buf, dr->read_buf + src_offset,
                dr->read_buf_fill - src_offset);
    }
    if (src_offset > 0) dr->read_buf_fill -= src_offset;
    return 0;
}

/* Read compressed data from inner rio and decompress into decomp_buf.
 * Returns 0 on success (decomp_buf has data), or signals EOF/error. */
static int decompressFillBuf(decompress_rio_t *dr) {
    dr->decomp_buf_pos = 0;
    dr->decomp_buf_len = 0;

    /* Ensure read_buf has room for a chunk */
    size_t need = dr->read_buf_fill + DECOMPRESS_READ_CHUNK_SIZE;
    if (need < dr->read_buf_fill) return -1; /* overflow */
    if (need > dr->read_buf_size) {
        if (need > DECOMPRESS_MAX_BUF_SIZE) return -1;
        dr->read_buf = zrealloc(dr->read_buf, need);
        dr->read_buf_size = need;
    }

    /* Try bulk read first */
    size_t got = decompressRioReadPartial(
        dr->inner,
        dr->read_buf + dr->read_buf_fill,
        DECOMPRESS_READ_CHUNK_SIZE);
    if (got > 0) {
        dr->read_buf_fill += got;
        if (decompressDrainReadBuf(dr) < 0) return -1;
        if (dr->decomp_buf_len > 0) return 0;
    }

    /* Bulk read got nothing (or produced no output). Accumulate byte
     * by byte until the decompressor can make progress or EOF. */
    while (dr->decomp_buf_len == 0) {
        if (dr->read_buf_fill >= dr->read_buf_size) {
            size_t new_size = dr->read_buf_size * 2;
            if (new_size < dr->read_buf_size || new_size > DECOMPRESS_MAX_BUF_SIZE)
                return -1;
            dr->read_buf_size = new_size;
            dr->read_buf = zrealloc(dr->read_buf, dr->read_buf_size);
        }
        if (rioRead(dr->inner, dr->read_buf + dr->read_buf_fill, 1) == 0) {
            if (rioCheckType(dr->inner) == RIO_TYPE_BUFFER)
                dr->inner->flags &= ~RIO_FLAG_READ_ERROR;
            break;
        }
        dr->read_buf_fill++;
        if (decompressDrainReadBuf(dr) < 0) return -1;
    }

    if (dr->decomp_buf_len == 0 && dr->read_buf_fill == 0) return -1;
    return 0;
}

static size_t decompressRioRead(rio *r, void *buf, size_t len) {
    decompress_rio_t *dr = (decompress_rio_t *)r;
    uint8_t *dst = (uint8_t *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        /* Serve from decomp_buf if available */
        size_t avail = dr->decomp_buf_len - dr->decomp_buf_pos;
        if (avail > 0) {
            size_t to_copy = avail < remaining ? avail : remaining;
            memcpy(dst, dr->decomp_buf + dr->decomp_buf_pos, to_copy);
            dr->decomp_buf_pos += to_copy;
            dst += to_copy;
            remaining -= to_copy;
            continue;
        }

        /* Refill decomp_buf from inner rio */
        if (decompressFillBuf(dr) < 0) return 0;
    }

    return len;
}

/* rio vtable: write callback — decompress_rio is read-only */
static size_t decompressRioWrite(rio *r, const void *buf, size_t len) {
    (void)r;
    (void)buf;
    (void)len;
    return 0; /* Not supported — decompress_rio is read-only */
}

/* rio vtable: tell callback — return inner (compressed) rio position
 * so loading progress reports compressed_bytes / compressed_file_size,
 * not decompressed_bytes / compressed_file_size (which would exceed 100%). */
static off_t decompressRioTell(rio *r) {
    decompress_rio_t *dr = (decompress_rio_t *)r;
    return rioTell(dr->inner);
}

/* rio vtable: flush callback — no-op for read-only rio */
static int decompressRioFlush(rio *r) {
    (void)r;
    return 1;
}

/* Initialize a decompression rio decorator wrapping an inner rio.
 * The VKCS envelope must already be consumed by the caller.
 * Buffers start small (64KB) and grow on demand. */
void decompress_rio_init(decompress_rio_t *dr, rio *inner, compression_algo_t algo) {
    if (!dr || !inner) return;

    memset(dr, 0, sizeof(*dr));

    /* Set up rio vtable */
    dr->base.read = decompressRioRead;
    dr->base.write = decompressRioWrite;
    dr->base.tell = decompressRioTell;
    dr->base.flush = decompressRioFlush;
    dr->base.update_cksum = NULL;
    dr->base.cksum = 0;
    dr->base.flags = 0;
    dr->base.processed_bytes = 0;
    dr->base.max_processing_chunk = 0;

    dr->inner = inner;

    /* Initialize decompressor */
    if (streamDecompressorInit(&dr->decompressor, algo) != 0) {
        dr->base.flags |= RIO_FLAG_READ_ERROR;
        /* Allocate minimal buffers so destroy() and accidental reads
         * don't dereference NULL. */
        dr->read_buf = zmalloc(1);
        dr->read_buf_size = 1;
        dr->decomp_buf = zmalloc(1);
        dr->decomp_buf_size = 1;
        return;
    }

    /* Start with small buffers — grow on demand (PERF) */
    dr->read_buf = zmalloc(DECOMPRESS_INITIAL_BUF_SIZE);
    dr->read_buf_size = DECOMPRESS_INITIAL_BUF_SIZE;
    dr->decomp_buf = zmalloc(DECOMPRESS_INITIAL_BUF_SIZE);
    dr->decomp_buf_size = DECOMPRESS_INITIAL_BUF_SIZE;
    dr->decomp_buf_pos = 0;
    dr->decomp_buf_len = 0;
}

/* Free decompressor context and internal buffers.
 * Safe to call on a zero-initialized or already-destroyed decorator. */
void decompress_rio_destroy(decompress_rio_t *dr) {
    if (!dr) return;
    streamDecompressorDestroy(&dr->decompressor);
    if (dr->read_buf) {
        zfree(dr->read_buf);
        dr->read_buf = NULL;
        dr->read_buf_size = 0;
    }
    if (dr->decomp_buf) {
        zfree(dr->decomp_buf);
        dr->decomp_buf = NULL;
        dr->decomp_buf_size = 0;
    }
    dr->decomp_buf_pos = 0;
    dr->decomp_buf_len = 0;
}

/* ===================================================================
 * Prefix Replay Rio Decorator
 * Serves buffered prefix bytes before delegating to inner rio.
 * Used for uncompressed RDB files where header bytes were consumed
 * for format detection and need to be replayed.
 * No dynamic allocations — no destroy needed.
 * =================================================================== */

/* rio vtable: read callback — serve prefix first, then inner rio */
static size_t prefixReplayRioRead(rio *r, void *buf, size_t len) {
    prefix_replay_rio_t *pr = (prefix_replay_rio_t *)r;
    uint8_t *dst = (uint8_t *)buf;
    size_t remaining = len;

    /* Serve from prefix buffer first */
    size_t prefix_avail = pr->prefix_len - pr->prefix_pos;
    if (prefix_avail > 0) {
        size_t to_copy = prefix_avail < remaining ? prefix_avail : remaining;
        memcpy(dst, pr->prefix + pr->prefix_pos, to_copy);
        pr->prefix_pos += to_copy;
        dst += to_copy;
        remaining -= to_copy;
    }

    /* Delegate remainder to inner rio */
    if (remaining > 0) {
        if (rioRead(pr->inner, dst, remaining) == 0) return 0;
    }

    return len;
}

/* rio vtable: write callback — prefix_replay_rio is read-only */
static size_t prefixReplayRioWrite(rio *r, const void *buf, size_t len) {
    (void)r;
    (void)buf;
    (void)len;
    return 0;
}

/* rio vtable: tell callback */
static off_t prefixReplayRioTell(rio *r) {
    return (off_t)r->processed_bytes;
}

/* rio vtable: flush callback — delegate to inner */
static int prefixReplayRioFlush(rio *r) {
    prefix_replay_rio_t *pr = (prefix_replay_rio_t *)r;
    if (pr->inner->flush) {
        return pr->inner->flush(pr->inner);
    }
    return 1;
}

/* Initialize a prefix-replay rio decorator.
 * Stores the consumed header bytes and serves them before inner rio.
 * prefix_len must be <= 8 (size of the prefix buffer). */
void prefix_replay_rio_init(prefix_replay_rio_t *pr, rio *inner, const char *prefix, size_t prefix_len) {
    if (!pr || !inner) return;

    memset(pr, 0, sizeof(*pr));

    /* Set up rio vtable */
    pr->base.read = prefixReplayRioRead;
    pr->base.write = prefixReplayRioWrite;
    pr->base.tell = prefixReplayRioTell;
    pr->base.flush = prefixReplayRioFlush;
    pr->base.update_cksum = NULL;
    pr->base.cksum = 0;
    pr->base.flags = 0;
    pr->base.processed_bytes = 0;
    pr->base.max_processing_chunk = 0;

    pr->inner = inner;

    /* Copy prefix bytes — must fit in the fixed-size buffer.
     * Callers must pass prefix_len <= sizeof(pr->prefix). */
    if (prefix_len > sizeof(pr->prefix)) {
        /* Invariant violation — should never happen. Set error flag
         * so reads fail immediately rather than corrupting data. */
        pr->base.flags |= RIO_FLAG_READ_ERROR;
        pr->prefix_len = 0;
        return;
    }
    if (prefix && prefix_len > 0) {
        memcpy(pr->prefix, prefix, prefix_len);
    }
    pr->prefix_len = prefix_len;
    pr->prefix_pos = 0;
}

/* ===================================================================
 * Async Compress API (stubs)
 * =================================================================== */

async_compress_ctx_t *async_compress_create(const async_compress_config_t *cfg) {
    /* Stub — async replication compression not yet implemented. */
    (void)cfg;
    return NULL;
}

size_t async_compress_write(async_compress_ctx_t *t, const void *buf, size_t len) {
    /* Stub */
    (void)t;
    (void)buf;
    (void)len;
    return 0;
}

void async_compress_finish(async_compress_ctx_t *t) {
    /* Stub */
    (void)t;
}

void async_compress_destroy(async_compress_ctx_t *t) {
    /* Stub */
    (void)t;
}

void async_compress_check_timeout(async_compress_ctx_t *t, long long now_us) {
    /* Stub */
    (void)t;
    (void)now_us;
}

void async_compress_retain(async_compress_ctx_t *t) {
    /* Stub */
    (void)t;
}

void async_compress_release(async_compress_ctx_t *t) {
    /* Stub */
    (void)t;
}
