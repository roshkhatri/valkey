/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_RIO_H
#define COMPRESSION_RIO_H

#include "compression_stream.h"
#include "rio.h"

/* Streaming compression has three layers:
 * - compression.c/compression_*.c wrap codec-specific streaming APIs.
 * - compression_stream.c owns VCS framing, probing, buffering, and validation.
 * - compression_rio.c adapts those streams to rio so RDB code can keep using rio.
 */
typedef struct {
    rio base; /* Must be first, allows casting to (rio *). */
    rio *inner;
    streamWriter writer;
    bool finalized;
} compressRio;

typedef struct {
    rio base; /* Must be first. */
    rio *inner;
    streamReader reader;
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
