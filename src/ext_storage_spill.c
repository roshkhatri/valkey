/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Data tiering: the spill path, which moves values from memory to the storage
 * engine. Candidate selection lives in evict.c, next to the eviction pool. */

#include "server.h"

#ifdef USE_EXT_STORAGE

#include "ext_storage.h"

/* Whether a key's value can be spilled. An embedded or INT-encoded value has no
 * allocation of its own to free, and a value with other references can't be
 * freed under them. Module types are not supported yet (#81). */
int isSpillCandidate(const robj *o) {
    return getStorageState(o) == STORAGE_STATE_ONLY_MEMORY && !o->hasembval &&
           objectGetEncoding(o) != OBJ_ENCODING_INT && objectGetRefcount(o) == 1 && objectGetType(o) != OBJ_MODULE;
}

#endif /* USE_EXT_STORAGE */
