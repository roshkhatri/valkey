/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "compression_rio.h"
#include <string.h>
#include <unistd.h>

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
    base->read_some = NULL;
    base->update_cksum = NULL;
    base->cksum = 0;
    base->flags = flags;
    base->processed_bytes = 0;
    base->max_processing_chunk = 0;
}

/* ===== compressRio ===== */

static int compressRioEmit(void *ctx, const uint8_t *data, size_t len) {
    compressRio *cr = (compressRio *)ctx;
    /* streamWriter has finished transforming this chunk. Send it directly to
     * the original destination. Sending it through cr->base would call
     * compressRioWrite again and recursively compress the output.
     * streamWriter sinks return 0 on success and -1 on failure, while rioWrite
     * returns nonzero on success and 0 on failure, so adapt that here. */
    return rioWrite(cr->inner, data, len) == 0 ? -1 : 0;
}

static size_t compressRioWrite(rio *r, const void *buf, size_t len) {
    compressRio *cr = (compressRio *)r;
    /* RDB writes plain bytes to cr->base. The base is the first struct member,
     * so the rio pointer is also a pointer to the containing compressRio. */
    if (cr->finalized) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    if (streamWriterWrite(&cr->writer, buf, len) != 0) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    return 1;
}

static off_t compressRioTell(rio *r) {
    return (off_t)r->processed_bytes;
}

/* Drains buffered data and the inner rio, but keeps the frame open so callers
 * that flush mid-stream don't accidentally close it. */
static int compressRioFlush(rio *r) {
    compressRio *cr = (compressRio *)r;
    if (cr->writer.errored) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    if (cr->finalized) return 1;

    if (streamWriterFlush(&cr->writer) != 0) {
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    if (cr->inner->flush && cr->inner->flush(cr->inner) == 0) {
        cr->writer.errored = true;
        r->flags |= RIO_FLAG_WRITE_ERROR;
        return 0;
    }
    return 1;
}

int rioInitWithRdbCompression(compressRio *cr,
                              rio *inner,
                              compressionAlgo algo,
                              bool codec_checksum_enabled) {
    streamWriterConfig cfg = {
        .algo = algo,
        .level = 0,
        .stream_kind = VCS_STREAM_RDB,
        .codec_checksum_enabled = codec_checksum_enabled,
    };

    /* inner is the rio RDB would have used without compression. Keep it
     * unchanged and put this new outward-facing rio in front of it. */
    memset(cr, 0, sizeof(*cr));
    rioInitBase(&cr->base, rioReadUnsupported, compressRioWrite, compressRioTell,
                compressRioFlush,
                RIO_FLAG_STREAMING_COMPRESSION | (inner->flags & RIO_FLAG_CONN_BACKED));

    cr->inner = inner;
    if (streamWriterInit(&cr->writer, &cfg, compressRioEmit, cr) != 0) {
        /* Self-clean so the failure contract matches decompression init:
         * on a nonzero return the compressRio is left zeroed and the caller
         * must not call compressRioFree. */
        memset(cr, 0, sizeof(*cr));
        return -1;
    }
    return 0;
}

/* Closing a codec frame is different from flushing it. Flush allows more RDB
 * bytes to follow; finish writes the frame ending and rejects later writes.
 * Idempotent: subsequent calls report cached error state. */
int compressRioFinish(compressRio *cr) {
    if (cr->finalized) {
        if (cr->writer.errored) {
            cr->base.flags |= RIO_FLAG_WRITE_ERROR;
            return -1;
        }
        return 0;
    }
    cr->finalized = 1;

    if (streamWriterFinish(&cr->writer) != 0) {
        cr->base.flags |= RIO_FLAG_WRITE_ERROR;
        return -1;
    }
    if (cr->inner->flush && cr->inner->flush(cr->inner) == 0) {
        cr->writer.errored = true;
        cr->base.flags |= RIO_FLAG_WRITE_ERROR;
    }
    if (cr->writer.errored) {
        cr->base.flags |= RIO_FLAG_WRITE_ERROR;
        return -1;
    }
    return 0;
}

void compressRioFree(compressRio *cr) {
    streamWriterFree(&cr->writer);
}

/* ===== decompressRio ===== */

static ssize_t decompressRioReadPartial(void *ctx, void *buf, size_t len) {
    decompressRio *dr = (decompressRio *)ctx;
    /* The decoder cannot predict how many encoded bytes will produce the
     * plain bytes requested by RDB. It therefore pulls up to len bytes from
     * the original source and decides after decoding whether it needs more. */
    return rioReadPartial(dr->inner, buf, len);
}

static size_t decompressRioRead(rio *r, void *buf, size_t len) {
    decompressRio *dr = (decompressRio *)r;
    if (dr->base.flags & RIO_FLAG_READ_ERROR) return 0;

    /* streamReader may produce a partial result, but the outward rio contract
     * used by RDB is exact-length-or-error. Keep reading until that contract is
     * satisfied. */
    uint8_t *dst = (uint8_t *)buf;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t nread = streamReaderRead(&dr->reader, dst, remaining);
        if (nread <= 0) {
            /* rio contract is full-or-fail; partial reads are an error. */
            dr->base.flags |= RIO_FLAG_READ_ERROR;
            return 0;
        }
        remaining -= (size_t)nread;
        dst += nread;
    }
    return len;
}

/* Reports transport bytes from the wrapped rio so progress tracks the source
 * stream rather than the decoded byte count. */
static off_t decompressRioTell(rio *r) {
    decompressRio *dr = (decompressRio *)r;
    return (off_t)dr->inner->processed_bytes;
}

streamReaderError decompressRioGetError(decompressRio *dr) {
    return dr->reader.error_kind;
}

int decompressRioValidateEnd(decompressRio *dr) {
    /* The RDB EOF opcode only ends the decoded payload. Also require the outer
     * codec frame to end cleanly, with no buffered or trailing bytes. */
    return streamReaderValidateEnd(&dr->reader);
}

decompressRioInitResult rioInitWithRdbDecompression(decompressRio *dr,
                                                    rio *inner,
                                                    bool skip_codec_checksum_validation,
                                                    compressionAlgo *algo) {
    streamReaderConfig cfg = {
        .expected_stream_kind = VCS_STREAM_RDB,
        .allow_passthrough = true,
        .skip_codec_checksum_validation = skip_codec_checksum_validation,
        .buffer_size = STREAM_READER_BUFFER_SIZE_DEFAULT,
    };
    streamReaderInfo info = {0};

    memset(dr, 0, sizeof(*dr));
    rioInitBase(&dr->base, decompressRioRead, rioWriteUnsupported, decompressRioTell,
                rioFlushNoop,
                RIO_FLAG_STREAMING_DECOMPRESSION |
                    (inner->flags & (RIO_FLAG_SKIP_RDB_CHECKSUM | RIO_FLAG_CONN_BACKED)));
    dr->inner = inner;

    if (algo) *algo = ALGO_NONE;

    /* The reader probes inner before RDB starts parsing:
     *
     *   plain input: replay the probe bytes and pass the rest through
     *   VCS input:   consume the envelope and return decoded RDB bytes
     *
     * In both cases callers can pass dr->base to the unchanged RDB parser. */
    if (streamReaderInit(&dr->reader, &cfg, decompressRioReadPartial, dr) != 0) return DECOMPRESS_RIO_INIT_ERROR;
    if (streamReaderGetInfo(&dr->reader, &info) != 0) {
        streamReaderError error_kind = dr->reader.error_kind;
        decompressRioFree(dr);
        return error_kind == STREAM_READER_ERROR_INCOMPATIBLE
                   ? DECOMPRESS_RIO_INIT_INCOMPATIBLE
                   : DECOMPRESS_RIO_INIT_ERROR;
    }

    if (info.compressed) {
        dr->base.flags |= RIO_FLAG_STREAMING_COMPRESSION | RIO_FLAG_SKIP_RDB_CHECKSUM;
        if (algo) *algo = info.algo;
    }
    return DECOMPRESS_RIO_INIT_OK;
}

void decompressRioFree(decompressRio *dr) {
    streamReaderFree(&dr->reader);
}
