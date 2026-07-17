/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_RIO_H
#define COMPRESSION_RIO_H

#include "compression_stream.h"
#include "rio.h"

/* These types are rio decorators: they add a transformation in front of an
 * already initialized rio without changing that inner rio.
 *
 * Save path (arrows show the direction of bytes):
 *
 *   rdbSaveRio -> compressRio.base -> streamWriter -> compressRio.inner -> file
 *                  plain RDB bytes       VCS/LZ4 bytes
 *
 * Load path:
 *
 *   rdbLoadRio <- decompressRio.base <- streamReader <- decompressRio.inner <- file
 *                  plain RDB bytes          file bytes
 *
 * The base rio is the interface presented to RDB. The inner rio is the
 * existing file, buffer, or connection backend that performs the physical I/O.
 * The wrappers do not own inner, so it must remain valid until the wrapper is
 * freed. Keeping base and inner separate also prevents transformed bytes from
 * being sent through the wrapper again.
 *
 * Streaming compression has three implementation layers:
 * - compression.c/compression_*.c wrap codec-specific streaming APIs.
 * - compression_stream.c owns VCS framing, probing, buffering, and validation.
 * - compression_rio.c adapts those streams to rio so RDB code can keep using rio.
 */
typedef struct {
    rio base;            /* Outward rio that accepts plain RDB bytes. Must be first. */
    rio *inner;          /* Existing destination for VCS/LZ4 bytes; not owned. */
    streamWriter writer; /* Transforms plain bytes before emitting to inner. */
    bool finalized;      /* No writes are allowed after the frame is finished. */
} compressRio;

typedef struct {
    rio base;            /* Outward rio that returns plain RDB bytes. Must be first. */
    rio *inner;          /* Existing source of plain or VCS/LZ4 bytes; not owned. */
    streamReader reader; /* Probes, passes through, or decompresses inner bytes. */
} decompressRio;

typedef enum {
    DECOMPRESS_RIO_INIT_ERROR = -1,
    DECOMPRESS_RIO_INIT_OK = 0,
    DECOMPRESS_RIO_INIT_INCOMPATIBLE = 1,
} decompressRioInitResult;

/* RDB wrappers keep VCS stream-kind, probing, buffer-size and checksum-policy
 * details out of RDB callers. */
int rioInitWithRdbCompression(compressRio *cr,
                              rio *inner,
                              compressionAlgo algo,
                              bool codec_checksum_enabled);
int compressRioFinish(compressRio *cr);
void compressRioFree(compressRio *cr);

/* Sets *algo to the compressed stream algorithm, or ALGO_NONE for plain input.
 * Compressed RDB input sets RIO_FLAG_SKIP_RDB_CHECKSUM on dr->base because VCS
 * uses codec-frame checksums instead of the logical RDB CRC64 trailer. */
decompressRioInitResult rioInitWithRdbDecompression(decompressRio *dr,
                                                    rio *inner,
                                                    bool skip_codec_checksum_validation,
                                                    compressionAlgo *algo);
streamReaderError decompressRioGetError(decompressRio *dr);
int decompressRioValidateEnd(decompressRio *dr);
void decompressRioFree(decompressRio *dr);

#endif /* COMPRESSION_RIO_H */
