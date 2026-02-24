/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Rio decorators (compress_rio, decompress_rio, prefix_replay_rio). */

#include "compression_rio.h"
#include "zmalloc.h"
#include <string.h>

/* Flush the wrapped inner rio and map failure to the stream writer's
 * sticky error state. */
static int compressRioFlushInner(stream_writer_t *t, rio *inner) {
    if (inner->flush && inner->flush(inner) == 0) {
        stream_writer_set_error(t);
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
 * Compression Rio Decorator
 * Wraps an inner rio for transparent compression on write.
 * Used by BGSAVE (fork child) and diskless sync.
 *
 * RDB CHECKSUM SEMANTICS: When streaming compression is active, integrity
 * may come from either:
 * - codec-native frame checksums (RIO_FLAG_STREAMING_CODEC_CHECKSUM), or
 * - the standard RDB CRC64 footer.
 *
 * The save/load paths decide which checksum source to use based on flags.
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
    if (!cr->compressor || cr->finalized || stream_writer_is_errored(cr->compressor)) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    if (stream_writer_write(cr->compressor, buf, len) != 0) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    return 1; /* rio write callback contract: 0 on error, non-zero on success */
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
    if (!cr->compressor || stream_writer_is_errored(cr->compressor)) return 0;
    if (cr->finalized) return 1;

    if (stream_writer_flush(cr->compressor) != 0) return 0;

    /* Flush inner rio */
    if (compressRioFlushInner(cr->compressor, cr->inner) != 0) return 0;
    return 1;
}

/* Initialize a compression rio decorator wrapping an inner rio.
 * Sets up the rio vtable so callers can use standard rioWrite/rioFlush.
 * The compressor is initialized with a fresh algorithm context (fork-safe). */
/* Returns 0 on success, -1 on failure (e.g., compressor init failed). */
int rioInitWithCompress(compress_rio_t *cr, rio *inner, const stream_writer_config_t *cfg) {
    if (!cr || !inner || !cfg) return -1;

    memset(cr, 0, sizeof(*cr));

    uint64_t flags = RIO_FLAG_STREAMING_COMPRESSION;
    if (cfg->block_checksum) flags |= RIO_FLAG_STREAMING_CODEC_CHECKSUM;

    rioInitBase(&cr->base, rioReadUnsupported, compressRioWrite, compressRioTell,
                compressRioFlush, flags);

    cr->inner = inner;
    cr->finalized = 0;
    cr->compressor = stream_writer_create(cfg, compressRioEmit, cr);
    return cr->compressor ? 0 : -1;
}

/* Finalize the compression frame and flush inner rio.
 * Must be called exactly once at end of stream.
 * Idempotent: safe to call multiple times (second call is a no-op). */
/* Returns 0 on success, -1 if the compressor or inner flush errored. */
int compress_rio_finish(compress_rio_t *cr) {
    if (!cr) return -1;
    if (!cr->compressor) return -1;
    if (cr->finalized) return stream_writer_is_errored(cr->compressor) ? -1 : 0;
    cr->finalized = 1;

    if (stream_writer_finish(cr->compressor) != 0) {
        return -1;
    }

    /* Flush inner rio to ensure all bytes reach the destination.
     * Propagate flush failure to the compressor error state so
     * callers can detect it. */
    compressRioFlushInner(cr->compressor, cr->inner);
    return stream_writer_is_errored(cr->compressor) ? -1 : 0;
}

/* Free compressor context and buffers. Does NOT finalize the frame.
 * Call compress_rio_finish() first on all exit paths. */
void compress_rio_destroy(compress_rio_t *cr) {
    if (!cr) return;
    if (cr->compressor) {
        stream_writer_destroy(cr->compressor);
        cr->compressor = NULL;
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

/* Decompression buffer sizing: one base constant drives everything.
 * DECOMPRESS_BATCH_SIZE controls the decode window, compressed read chunk,
 * and initial buffer sizes. */
#define DECOMPRESS_BATCH_SIZE (1024 * 1024) /* 1MB: ~16x 64KB LZ4 blocks per window fill */

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

/* Return writable bytes at the tail of read_buf.
 * Compacts buffered data to the front only when the tail is full. */
static size_t decompressReadBufTailSpace(decompress_rio_t *dr) {
    size_t tail_space = dr->read_buf_size - dr->read_buf_pos - dr->read_buf_fill;
    if (tail_space > 0) return tail_space;

    if (dr->read_buf_fill == 0) {
        dr->read_buf_pos = 0;
        return dr->read_buf_size;
    }

    if (dr->read_buf_pos > 0) {
        memmove(dr->read_buf, dr->read_buf + dr->read_buf_pos, dr->read_buf_fill);
        dr->read_buf_pos = 0;
        return dr->read_buf_size - dr->read_buf_fill;
    }

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

        size_t read_size = decompressReadBufTailSpace(dr);
        if (read_size == 0) {
            /* With 64KB LZ4 blocks and a 1MB read buffer, this should not
             * happen for valid streams. Treat as decode/input corruption. */
            return -1;
        }

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
    if (!dr || !dr->decomp_buf || dr->decomp_buf_size == 0) return -1;
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
    if (!dr || dr->decomp_buf_len <= dr->decomp_buf_pos) return 0;
    return dr->decomp_buf_len - dr->decomp_buf_pos;
}

/* Copy available decoded bytes from window to caller buffer.
 * Updates dst/remaining and returns copied bytes. */
static size_t decompressCopyFromWindow(decompress_rio_t *dr,
                                       uint8_t **dst,
                                       size_t *remaining) {
    if (!dr || !dst || !*dst || !remaining || !dr->decomp_buf) return 0;
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
        /* For large reads, decode directly into caller buffer to avoid one
         * extra memcpy through the decode window. */
        if (decompressWindowAvail(dr) == 0 &&
            dr->decomp_buf_size > 0 &&
            remaining >= dr->decomp_buf_size) {
            size_t direct_written = 0;
            if (decompressPump(dr, dst, remaining, &direct_written) < 0) return 0;
            if (direct_written == 0) return 0; /* EOF */
            dst += direct_written;
            remaining -= direct_written;
            continue;
        }

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
 * Both read_buf and decomp_buf are sized to DECOMPRESS_BATCH_SIZE (1MB).
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
