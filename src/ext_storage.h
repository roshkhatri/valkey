/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Data tiering storage-engine-side entry points. Present only in a build with
 * BUILD_EXT_STORAGE, which defines USE_EXT_STORAGE. Without it, the hooks the
 * rest of the server calls are empty. */

#ifndef VALKEY_EXT_STORAGE_H
#define VALKEY_EXT_STORAGE_H

#include "server.h"

#ifdef USE_EXT_STORAGE

#include "storage/storage.h"

/* Opens the configured storage engine. */
void extStorageInit(void);

/* Closes the storage engine. */
void extStorageDeinit(void);

/* Crash handler only. Stops the storage engine threads with no cleanup. */
void extStorageKillThreads(void);

/* Whether a storage engine is open and ready. */
int extStorageIsActive(void);

/* Appends the ext_storage INFO section to the given sds. */
sds extStorageInfoString(sds info);

/* The serialization callbacks. */
const storageSerializer *extStorageSerializer(void);

/* Polls finished requests and hands each one to the code that submitted it. */
int extStorageProcessCompletions(void);

storageState getStorageState(const robj *o);
void setStorageState(robj *o, storageState state);
robj *spillSelectCandidate(int *dbid);
storageStatus spillItem(int dbid, robj *entry);
size_t spillInflightRamBytes(void);
void debugSpillCommand(client *c);
void spillCancel(robj *entry);
void spillRemove(robj *entry);
void spillDrain(void);
bool spillCanEvict(const robj *o);
size_t spillToFree(size_t mem_tofree);
void spillCron(void);

/* Call before a write changes a key's value in place. */
static inline void spillBeforeWrite(robj *o) {
    if (o->storage_state == STORAGE_STATE_COPYING_TO_FLASH) spillCancel(o);
}

/* Call before a key is deleted or its value replaced. */
static inline void spillBeforeRemove(robj *o) {
    if (o->storage_state != STORAGE_STATE_ONLY_MEMORY) spillRemove(o);
}

/* Whether the key's value is on flash and not in DRAM. */
static inline bool spillIsOnFlash(const robj *o) {
    return o->storage_state == STORAGE_STATE_ONLY_FLASH;
}

#else

static inline void spillBeforeWrite(robj *o) {
    UNUSED(o);
}
static inline void spillBeforeRemove(robj *o) {
    UNUSED(o);
}
static inline void spillDrain(void) {
}
static inline bool spillCanEvict(const robj *o) {
    UNUSED(o);
    return true;
}
static inline size_t spillToFree(size_t mem_tofree) {
    return mem_tofree;
}
static inline bool spillIsOnFlash(const robj *o) {
    UNUSED(o);
    return false;
}
static inline int extStorageProcessCompletions(void) {
    return 0;
}
static inline void spillCron(void) {
}

#endif /* USE_EXT_STORAGE */

#endif /* VALKEY_EXT_STORAGE_H */
