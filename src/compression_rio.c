/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Rio decorators (compress_rio, decompress_rio). */

#include "compression_rio.h"
#include <string.h>
#include <unistd.h>

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
                        uint64_t flags,
                        uint8_t type) {
    base->read = read_fn;
    base->write = write_fn;
    base->tell = tell_fn;
    base->flush = flush_fn;
    base->update_cksum = NULL;
    base->cksum = 0;
    base->flags = flags;
    base->processed_bytes = 0;
    base->max_processing_chunk = 0;
    base->type = type;
}

/* ===================================================================
 * Compression Rio Decorator
 * Wraps an inner rio for transparent compression on write.
 * Currently used by file-backed RDB save paths. Replication wiring will
 * reuse the same writer abstraction once that path lands.
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
    if (stream_writer_write(cr->compressor, buf, len) < 0) {
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
    flags |= inner->flags & RIO_FLAG_SKIP_RDB_CHECKSUM;

    rioInitBase(&cr->base, rioReadUnsupported, compressRioWrite, compressRioTell,
                compressRioFlush, flags, rioCheckType(inner));
    /* When codec checksums are enabled, also track the checksum of the
     * uncompressed byte stream so decoded-to-disk RDBs keep a valid CRC64
     * footer. Honor skip-checksum requests from the wrapped rio. */
    if ((flags & RIO_FLAG_STREAMING_CODEC_CHECKSUM) &&
        !(flags & RIO_FLAG_SKIP_RDB_CHECKSUM)) {
        cr->base.update_cksum = rioGenericUpdateChecksum;
    }

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
 * Decompression Rio Decorator
 * Thin rio adapter around generic stream_reader_t.
 * =================================================================== */

/* Read up to `len` bytes from the inner rio (partial reads allowed).
 * Returns >0 bytes, 0 on EOF, -1 on error. */
static ssize_t decompressRioReadPartial(void *ctx, void *buf, size_t len) {
    decompress_rio_t *dr = (decompress_rio_t *)ctx;
    rio *inner = dr->inner;
    uint8_t inner_type = rioCheckType(inner);

    if (inner_type == RIO_TYPE_FILE) {
        size_t got = fread(buf, 1, len, inner->io.file.fp);
        if (got > 0) inner->processed_bytes += got;
        return (ssize_t)got;
    }
    if (inner_type == RIO_TYPE_BUFFER) {
        size_t avail = sdslen(inner->io.buffer.ptr) - inner->io.buffer.pos;
        if (avail == 0) return 0;
        size_t n = avail < len ? avail : len;
        memcpy(buf, inner->io.buffer.ptr + inner->io.buffer.pos, n);
        inner->io.buffer.pos += n;
        inner->processed_bytes += n;
        return (ssize_t)n;
    }

    /* conn rios: use connRead directly for partial-read support.
     * rioRead is all-or-nothing and would block on a live connection
     * when the compressed stream has ended but the connection stays
     * open (e.g. diskless replication full sync). */
    if (inner_type == RIO_TYPE_CONN) {
        int nread = connRead(inner->io.conn.conn, buf, len);
        if (nread > 0) {
            inner->processed_bytes += nread;
            return (ssize_t)nread;
        }
        /* 0 = EOF, <0 = error or EAGAIN */
        return nread == 0 ? 0 : -1;
    }

    /* fd rios are all-or-nothing via rioRead() */
    if (rioRead(inner, buf, len) != 0) return (ssize_t)len;
    return rioGetReadError(inner) ? -1 : 0;
}

static size_t decompressRioRead(rio *r, void *buf, size_t len) {
    decompress_rio_t *dr = (decompress_rio_t *)r;
    if (dr->base.flags & RIO_FLAG_READ_ERROR) return 0;
    if (!dr->reader) {
        dr->base.flags |= RIO_FLAG_READ_ERROR;
        return 0;
    }

    uint8_t *dst = (uint8_t *)buf;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t nread = stream_reader_read(dr->reader, dst, remaining);
        if (nread < 0) {
            dr->base.flags |= RIO_FLAG_READ_ERROR;
            return 0;
        }
        if (nread == 0) {
            /* rio contract: partial read is failure for requested len. */
            dr->base.flags |= RIO_FLAG_READ_ERROR;
            return 0;
        }
        remaining -= (size_t)nread;
        dst += nread;
    }
    return len;
}

/* rio vtable: tell callback — return inner position so loading progress
 * remains based on source (compressed) bytes. */
static off_t decompressRioTell(rio *r) {
    decompress_rio_t *dr = (decompress_rio_t *)r;
    return rioTell(dr->inner);
}

static void decompressRioPreservePendingInput(decompress_rio_t *dr) {
    const uint8_t *pending = NULL;
    size_t pending_len = 0;

    if (!dr || !dr->reader || !dr->inner) return;
    if (stream_reader_finish(dr->reader) != 0) return;
    if (stream_reader_get_pending_input(dr->reader, &pending, &pending_len) != 0) return;
    if (!pending || pending_len == 0) return;

    rio *inner = dr->inner;
    switch (rioCheckType(inner)) {
    case RIO_TYPE_CONN:
        if ((size_t)inner->io.conn.pos < pending_len || inner->io.conn.read_so_far < pending_len) return;
        inner->io.conn.pos -= pending_len;
        inner->io.conn.read_so_far -= pending_len;
        if (inner->processed_bytes >= pending_len) inner->processed_bytes -= pending_len;
        break;
    case RIO_TYPE_BUFFER:
        if ((size_t)inner->io.buffer.pos < pending_len) return;
        inner->io.buffer.pos -= pending_len;
        if (inner->processed_bytes >= pending_len) inner->processed_bytes -= pending_len;
        break;
    case RIO_TYPE_FILE:
        if (fseeko(inner->io.file.fp, -(off_t)pending_len, SEEK_CUR) == -1) return;
        if (inner->processed_bytes >= pending_len) inner->processed_bytes -= pending_len;
        break;
    case RIO_TYPE_FD:
        if (lseek(inner->io.fd.fd, -(off_t)pending_len, SEEK_CUR) == (off_t)-1) return;
        if (inner->processed_bytes >= pending_len) inner->processed_bytes -= pending_len;
        break;
    default:
        break;
    }
}

int decompress_rio_init_with_config(decompress_rio_t *dr, rio *inner, const stream_reader_config_t *cfg) {
    if (!dr || !inner || !cfg) return -1;

    memset(dr, 0, sizeof(*dr));
    rioInitBase(&dr->base, decompressRioRead, rioWriteUnsupported, decompressRioTell,
                rioFlushNoop, RIO_FLAG_STREAMING_DECOMPRESSION, rioCheckType(inner));
    dr->inner = inner;

    dr->reader = stream_reader_create(cfg, decompressRioReadPartial, dr);
    if (!dr->reader) {
        dr->base.flags |= RIO_FLAG_READ_ERROR;
        return -1;
    }

    stream_reader_info_t info;
    if (stream_reader_get_info(dr->reader, &info) != 0) {
        stream_reader_destroy(dr->reader);
        dr->reader = NULL;
        dr->base.flags |= RIO_FLAG_READ_ERROR;
        return -1;
    }
    if (info.compressed) {
        dr->base.flags |= RIO_FLAG_STREAMING_COMPRESSION;
        if (info.codec_checksum_enabled) {
            dr->base.flags |= RIO_FLAG_STREAMING_CODEC_CHECKSUM;
        }
    }

    return 0;
}

int decompress_rio_get_info(decompress_rio_t *dr, stream_reader_info_t *info) {
    if (!dr || !dr->reader || !info) return -1;
    return stream_reader_get_info(dr->reader, info);
}

void decompress_rio_destroy(decompress_rio_t *dr) {
    if (!dr) return;
    if (dr->reader) {
        decompressRioPreservePendingInput(dr);
        stream_reader_destroy(dr->reader);
        dr->reader = NULL;
    }
}
