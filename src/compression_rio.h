/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef COMPRESSION_RIO_H
#define COMPRESSION_RIO_H

#include "compression_stream.h"
#include "rio.h"

typedef struct {
    rio base; /* Must be first — allows casting to (rio *). */
    rio *inner;
    streamWriter *writer;
    bool finalized;
} compressRio;

typedef struct {
    rio base; /* Must be first. */
    rio *inner;
    streamReader *reader;
} decompressRio;

typedef enum {
    DECOMPRESS_RIO_INIT_ERROR = -1,
    DECOMPRESS_RIO_INIT_OK = 0,
    DECOMPRESS_RIO_INIT_INCOMPATIBLE = 1,
} decompressRioInitResult;

int rioInitWithCompression(compressRio *cr, rio *inner, const streamWriterConfig *cfg);
int compressRioFinish(compressRio *cr);
void compressRioFree(compressRio *cr);

/* Probes the wrapped rio so the caller learns up front whether it is plain,
 * compressed, or carrying an envelope this build cannot read. */
decompressRioInitResult rioInitWithDecompression(decompressRio *dr,
                                                 rio *inner,
                                                 const streamReaderConfig *cfg,
                                                 streamReaderInfo *info);
streamReaderError rioGetDecompressionError(const rio *r);
int decompressRioValidateEnd(decompressRio *dr);
void decompressRioFree(decompressRio *dr);

#endif /* COMPRESSION_RIO_H */
