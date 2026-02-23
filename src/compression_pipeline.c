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

/* Shared initializer for sync compression contexts.
 * Used by both heap-allocated sync_compress_ctx_t and embedded
 * compress_rio_t::compressor to keep behavior identical. */
static int syncCompressInitContext(sync_compress_ctx_t *t,
                                   const sync_compress_config_t *cfg,
                                   vkcsEmitFn emit_cb,
                                   void *emit_ctx,
                                   int block_checksum) {
    if (!t || !cfg || !emit_cb) return -1;
    /* Only LZ4 is supported for now. */
    if (cfg->algo != ALGO_LZ4) return -1;

    memset(t, 0, sizeof(*t));
    t->emit_cb = emit_cb;
    t->emit_ctx = emit_ctx;
    t->stream_kind = cfg->stream_kind;

    if (streamCompressorInit(&t->compressor, cfg->algo, cfg->level) != 0) {
        return -1;
    }
    t->compressor.block_checksum = block_checksum != 0;
    return 0;
}

/* Emit envelope lazily on first write/finish.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int syncCompressEnsureEnvelope(sync_compress_ctx_t *t) {
    if (t->envelope_written) return 0;
    if (writeVkcsEnvelope(t->emit_cb, t->emit_ctx, t->compressor.algo, t->stream_kind) != 0) {
        t->errored = 1;
        return -1;
    }
    t->envelope_written = 1;
    return 0;
}

/* Emit compressed bytes to the output sink.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int syncCompressEmit(sync_compress_ctx_t *t, const uint8_t *buf, size_t len) {
    if (len == 0) return 0;
    if (t->emit_cb(t->emit_ctx, buf, len) != 0) {
        t->errored = 1;
        return -1;
    }
    return 0;
}

static void syncCompressEnsureOutBuf(sync_compress_ctx_t *t, size_t input_len, compress_flush_mode_t flush_mode);

/* Release stream compressor state owned by sync_compress_ctx_t.
 * Does not free the context object itself. */
static void syncCompressReleaseContext(sync_compress_ctx_t *t) {
    if (!t) return;
    streamCompressorDestroy(&t->compressor);
    if (t->out_buf) {
        zfree(t->out_buf);
        t->out_buf = NULL;
    }
    t->out_buf_size = 0;
}

/* Compress one chunk with the requested flush mode and emit produced bytes.
 * Returns 0 on success, -1 on error (and sets t->errored). */
static int syncCompressFeedAndEmit(sync_compress_ctx_t *t,
                                   const uint8_t *input,
                                   size_t input_len,
                                   compress_flush_mode_t flush_mode) {
    syncCompressEnsureOutBuf(t, input_len, flush_mode);

    uint8_t *out_ptr = t->out_buf;
    ssize_t compressed = streamCompressFeed(&t->compressor, &out_ptr,
                                            t->out_buf_size,
                                            input, input_len, flush_mode);
    if (compressed < 0) {
        t->errored = 1;
        return -1;
    }
    return syncCompressEmit(t, out_ptr, (size_t)compressed);
}

/* Flush the wrapped inner rio and map failure to the sync compressor's
 * sticky error state. */
static int syncCompressFlushInner(sync_compress_ctx_t *t, rio *inner) {
    if (inner->flush && inner->flush(inner) == 0) {
        t->errored = 1;
        return -1;
    }
    return 0;
}

/* Shared rio callbacks for unsupported/no-op operations. */
static size_t rioReadUnsupported(rio *r, void *buf, size_t len) {
    (void)r;
    (void)buf;
    (void)len;
    return 0;
}

static size_t rioWriteUnsupported(rio *r, const void *buf, size_t len) {
    (void)r;
    (void)buf;
    (void)len;
    return 0;
}

static int rioFlushNoop(rio *r) {
    (void)r;
    return 1;
}

/* Shared rio base initializer used by all decorators in this file. */
static void rioInitBase(rio *base,
                        size_t (*read_fn)(rio *, void *, size_t),
                        size_t (*write_fn)(rio *, const void *, size_t),
                        off_t (*tell_fn)(rio *),
                        int (*flush_fn)(rio *),
                        uint64_t flags) {
    base->read = read_fn;
    base->write = write_fn;
    base->tell = tell_fn;
    base->flush = flush_fn;
    base->update_cksum = NULL;
    base->cksum = 0;
    base->flags = flags;
    base->processed_bytes = 0;
    base->max_processing_chunk = 0;
}

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
    sync_compress_ctx_t *t = zmalloc(sizeof(*t));
    if (syncCompressInitContext(t, cfg, emit_cb, emit_ctx, 0) != 0) {
        zfree(t);
        return NULL;
    }
    return t;
}

/* Destroy a sync compress context, freeing the compressor and buffers.
 * Does NOT finalize the frame — call sync_compress_finish first.
 * Safe to call on NULL. */
void sync_compress_destroy(sync_compress_ctx_t *t) {
    if (!t) return;
    syncCompressReleaseContext(t);
    zfree(t);
}

/* Ensure the output buffer is large enough for the given input.
 * Reuses the existing buffer when possible to avoid per-write allocation.
 * zmalloc aborts on OOM, so this cannot fail. */
static void syncCompressEnsureOutBuf(sync_compress_ctx_t *t, size_t input_len, compress_flush_mode_t flush_mode) {
    size_t needed = streamCompressOutputBound(t->compressor.algo, input_len,
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
        t->out_buf = zrealloc(t->out_buf, needed);
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
    if (syncCompressEnsureEnvelope(t) != 0) return;
    if (syncCompressFeedAndEmit(t, (const uint8_t *)buf, len, FLUSH_CONTINUE) != 0) return;
}

/* Finalize the compression frame (flush_mode=FLUSH_END).
 * Emits the final compressed output including the frame end mark.
 * After this call, the compressor cannot be used for further writes. */
void sync_compress_finish(sync_compress_ctx_t *t) {
    if (!t || t->errored || t->finished) return;
    t->finished = 1;

    /* If nothing was ever written, emit envelope + empty frame end */
    if (syncCompressEnsureEnvelope(t) != 0) return;
    if (syncCompressFeedAndEmit(t, NULL, 0, FLUSH_END) != 0) return;
}

/* ===================================================================
 * Compression Rio Decorator
 * Wraps an inner rio for transparent compression on write.
 * Used by BGSAVE (fork child) and diskless sync.
 *
 * RDB CHECKSUM SEMANTICS: When streaming compression is active, the
 * RDB CRC64 is NOT computed on uncompressed bytes. Instead, integrity
 * is provided by codec-native frame checksums (for LZ4, block checksums),
 * validated automatically during decompression.
 * The RDB footer CRC64 will be 0, which the loader treats as
 * "checksum disabled". This avoids hashing ~1GB of decompressed data
 * on load.
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

/* rio vtable: tell callback — returns processed bytes from base */
static off_t compressRioTell(rio *r) {
    return (off_t)r->processed_bytes;
}

/* rio vtable: flush callback — algorithm flush (emit buffered data,
 * keep frame open) + inner flush. Does NOT end the frame.
 * This is critical because some call sites flush mid-stream. */
static int compressRioFlush(rio *r) {
    compress_rio_t *cr = (compress_rio_t *)r;
    if (cr->compressor.errored) return 0;
    if (cr->finalized) return 1;

    /* Only flush if we've started writing (envelope + frame exist) */
    if (cr->compressor.envelope_written && cr->compressor.compressor.frame_started) {
        if (syncCompressFeedAndEmit(&cr->compressor, NULL, 0, FLUSH_SYNC) != 0) {
            return 0;
        }
    }

    /* Flush inner rio */
    if (syncCompressFlushInner(&cr->compressor, cr->inner) != 0) return 0;
    return 1;
}

/* Initialize a compression rio decorator wrapping an inner rio.
 * Sets up the rio vtable so callers can use standard rioWrite/rioFlush.
 * The compressor is initialized with a fresh algorithm context (fork-safe). */
/* Returns 0 on success, -1 on failure (e.g., compressor init failed). */
int rioInitWithCompress(compress_rio_t *cr, rio *inner, const sync_compress_config_t *cfg, int codec_checksum) {
    if (!cr || !inner || !cfg) return -1;

    memset(cr, 0, sizeof(*cr));

    /* Checksum strategy: streaming-compressed paths use codec-native
     * frame checksums, so we do not compute RDB CRC64 on this wrapper. */
    rioInitBase(&cr->base, rioReadUnsupported, compressRioWrite, compressRioTell,
                compressRioFlush, RIO_FLAG_STREAMING_COMPRESSION);

    cr->inner = inner;
    cr->finalized = 0;

    return syncCompressInitContext(&cr->compressor, cfg, compressRioEmit, cr, codec_checksum);
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
    syncCompressFlushInner(&cr->compressor, cr->inner);
    return cr->compressor.errored ? -1 : 0;
}

/* Free compressor context and buffers. Does NOT finalize the frame.
 * Call compress_rio_finish() first on all exit paths. */
void compress_rio_destroy(compress_rio_t *cr) {
    if (!cr) return;
    syncCompressReleaseContext(&cr->compressor);
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

/* Decompression buffer sizing: one base constant drives everything.
 * DECOMPRESS_BATCH_SIZE controls the decode window, compressed read chunk,
 * and initial buffer sizes. */
#define DECOMPRESS_BATCH_SIZE (256 * 1024)          /* 256KB: ~4x 64KB LZ4 blocks per window fill */
#define DECOMPRESS_MAX_BUF_SIZE (256 * 1024 * 1024) /* 256MB safety limit for read_buf growth */

/* Read up to `len` bytes from the inner rio.
 * Returns bytes actually read (may be less than len). 0 on EOF/error.
 * Handles partial reads for all rio types: file (fread), buffer (copy
 * available bytes), conn/fd (all-or-nothing via rioRead). */
static size_t decompressRioReadPartial(rio *inner, void *buf, size_t len) {
    if (rioCheckType(inner) == RIO_TYPE_FILE) {
        size_t got = fread(buf, 1, len, inner->io.file.fp);
        if (got > 0) inner->processed_bytes += got;
        return got;
    }
    if (rioCheckType(inner) == RIO_TYPE_BUFFER) {
        /* True partial read: copy min(available, requested) bytes. */
        size_t avail = sdslen(inner->io.buffer.ptr) - inner->io.buffer.pos;
        if (avail == 0) return 0;
        size_t n = avail < len ? avail : len;
        memcpy(buf, inner->io.buffer.ptr + inner->io.buffer.pos, n);
        inner->io.buffer.pos += n;
        inner->processed_bytes += n;
        return n;
    }
    /* conn/fd rios: all-or-nothing */
    if (rioRead(inner, buf, len) != 0) return len;
    return 0;
}

/* Decode primitive: decompress from read_buf into `out[0..out_size)`.
 * Advances read_buf_pos/read_buf_fill.
 * Returns -1 on decompressor error, 0 otherwise. */
static int decompressDrainReadBuf(decompress_rio_t *dr,
                                  uint8_t *out,
                                  size_t out_size,
                                  size_t *out_written) {
    *out_written = 0;
    while (dr->read_buf_fill > 0 && *out_written < out_size) {
        size_t consumed = 0;
        ssize_t produced = streamDecompressFeed(
            &dr->decompressor,
            out + *out_written, out_size - *out_written,
            dr->read_buf + dr->read_buf_pos,
            dr->read_buf_fill, &consumed);
        if (produced < 0) return -1;
        *out_written += (size_t)produced;
        dr->read_buf_pos += consumed;
        dr->read_buf_fill -= consumed;
        if (consumed == 0 && produced == 0) break;
    }
    if (dr->read_buf_fill == 0) dr->read_buf_pos = 0;
    return 0;
}

/* Ensure read_buf has room for `need` more bytes after existing data.
 * Compacts (memmove) only when tail space is insufficient. */
static int decompressEnsureReadBuf(decompress_rio_t *dr, size_t need) {
    size_t tail_space = dr->read_buf_size - dr->read_buf_pos - dr->read_buf_fill;
    if (tail_space >= need) return 0;

    if (dr->read_buf_pos > 0) {
        memmove(dr->read_buf, dr->read_buf + dr->read_buf_pos, dr->read_buf_fill);
        dr->read_buf_pos = 0;
        tail_space = dr->read_buf_size - dr->read_buf_fill;
        if (tail_space >= need) return 0;
    }

    size_t new_size = dr->read_buf_size;
    while (new_size - dr->read_buf_fill < need) {
        size_t doubled = new_size * 2;
        if (doubled < new_size || doubled > DECOMPRESS_MAX_BUF_SIZE) return -1;
        new_size = doubled;
    }
    dr->read_buf = zrealloc(dr->read_buf, new_size);
    dr->read_buf_size = new_size;
    return 0;
}

/* Unified pump: read compressed input and decompress into out[0..out_size).
 * Loops until output buffer is full, decoder stalls, or EOF.
 * Returns 0 on success, -1 on error. *out_written==0 means EOF. */
static int decompressPump(decompress_rio_t *dr,
                          uint8_t *out,
                          size_t out_size,
                          size_t *out_written) {
    *out_written = 0;

    while (*out_written < out_size) {
        /* Drain any buffered compressed data first */
        if (dr->read_buf_fill > 0) {
            size_t written = 0;
            if (decompressDrainReadBuf(dr, out + *out_written,
                                       out_size - *out_written, &written) < 0)
                return -1;
            *out_written += written;
            if (*out_written >= out_size) return 0;
            /* Drain consumed everything but produced nothing — need more input */
        }

        /* Fixed-size compressed reads keep the pump loop predictable. */
        size_t read_size = DECOMPRESS_BATCH_SIZE;

        if (decompressEnsureReadBuf(dr, read_size) < 0) return -1;

        size_t got = decompressRioReadPartial(
            dr->inner,
            dr->read_buf + dr->read_buf_pos + dr->read_buf_fill,
            read_size);
        if (got == 0) break; /* EOF */
        dr->read_buf_fill += got;
    }

    return 0;
}

/* Fill the decode window by pumping decompressed data into decomp_buf.
 * Resets the window position and length, then fills up to decomp_buf_size.
 * Returns bytes decoded into the window, 0 on EOF, -1 on error. */
static ssize_t decompressFillWindow(decompress_rio_t *dr) {
    dr->decomp_buf_pos = 0;
    dr->decomp_buf_len = 0;
    size_t written = 0;
    if (decompressPump(dr, dr->decomp_buf, dr->decomp_buf_size, &written) < 0)
        return -1;
    dr->decomp_buf_len = written;
    return (ssize_t)written;
}

/* Return available decoded bytes in the window. */
static inline size_t decompressWindowAvail(const decompress_rio_t *dr) {
    return dr->decomp_buf_len - dr->decomp_buf_pos;
}

/* Copy available decoded bytes from window to caller buffer.
 * Updates dst/remaining and returns copied bytes. */
static size_t decompressCopyFromWindow(decompress_rio_t *dr,
                                       uint8_t **dst,
                                       size_t *remaining) {
    size_t avail = decompressWindowAvail(dr);
    if (avail == 0 || *remaining == 0) return 0;

    size_t to_copy = avail < *remaining ? avail : *remaining;
    memcpy(*dst, dr->decomp_buf + dr->decomp_buf_pos, to_copy);
    dr->decomp_buf_pos += to_copy;
    *dst += to_copy;
    *remaining -= to_copy;
    return to_copy;
}

static size_t decompressRioRead(rio *r, void *buf, size_t len) {
    decompress_rio_t *dr = (decompress_rio_t *)r;
    if (dr->base.flags & RIO_FLAG_READ_ERROR) return 0;
    if (!dr->decomp_buf || dr->decomp_buf_size == 0) return 0;
    uint8_t *dst = (uint8_t *)buf;
    size_t remaining = len;

    /* Serve any existing decoded bytes first. */
    decompressCopyFromWindow(dr, &dst, &remaining);
    if (remaining == 0) return len;

    /* Decode into the window and serve via memcpy.
     * This keeps the read path simple and predictable. */
    while (remaining > 0) {
        if (decompressWindowAvail(dr) == 0) {
            ssize_t filled = decompressFillWindow(dr);
            if (filled < 0) return 0;
            if (filled == 0) return 0; /* EOF */
        }

        decompressCopyFromWindow(dr, &dst, &remaining);
    }

    return len;
}

/* rio vtable: tell callback — return inner (compressed) rio position
 * so loading progress reports compressed_bytes / compressed_file_size,
 * not decompressed_bytes / compressed_file_size (which would exceed 100%). */
static off_t decompressRioTell(rio *r) {
    decompress_rio_t *dr = (decompress_rio_t *)r;
    return rioTell(dr->inner);
}

/* Initialize a decompression rio decorator wrapping an inner rio.
 * The VKCS envelope must already be consumed by the caller.
 * Both read_buf and decomp_buf are sized to DECOMPRESS_BATCH_SIZE (256KB).
 * The decode window (decomp_buf) is the key performance structure: LZ4F
 * decodes full blocks into it, and multiple rioRead calls are served via
 * memcpy. At 10KB values, ~25 reads per window fill. */
void decompress_rio_init(decompress_rio_t *dr, rio *inner, compression_algo_t algo) {
    if (!dr || !inner) return;

    memset(dr, 0, sizeof(*dr));

    rioInitBase(&dr->base, decompressRioRead, rioWriteUnsupported, decompressRioTell,
                rioFlushNoop, RIO_FLAG_STREAMING_COMPRESSION);

    dr->inner = inner;

    /* Initialize decompressor */
    if (streamDecompressorInit(&dr->decompressor, algo) != 0) {
        dr->base.flags |= RIO_FLAG_READ_ERROR;
        return;
    }

    /* Both buffers sized to DECOMPRESS_BATCH_SIZE. read_buf batches
     * compressed input; decomp_buf is the decode window serving
     * multiple rioRead calls per fill via memcpy. */
    dr->read_buf = zmalloc(DECOMPRESS_BATCH_SIZE);
    dr->read_buf_size = DECOMPRESS_BATCH_SIZE;
    dr->decomp_buf = zmalloc(DECOMPRESS_BATCH_SIZE);
    dr->decomp_buf_size = DECOMPRESS_BATCH_SIZE;
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

    rioInitBase(&pr->base, prefixReplayRioRead, rioWriteUnsupported,
                prefixReplayRioTell, prefixReplayRioFlush, 0);

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
