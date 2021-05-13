/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_MNODE_COMPACT_H
#define TDENGINE_MNODE_COMPACT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "mnode.h"
#include "twal.h"
#include "mnodeSdb.h"

// for wal compact structure
typedef struct {
  char *    name;
  ESdbTable id;
  ESdbKey   keyType;
  int32_t   hashSessions;

  int32_t (*fpDecode)(SSdbRow *pRow);  
} SSdbTableCompactDesc;

int32_t mnodeCompactWal();
int64_t sdbOpenCompactTable(SSdbTableCompactDesc *pDesc);

#ifdef __cplusplus
}
#endif

#endif