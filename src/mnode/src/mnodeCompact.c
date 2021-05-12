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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taoserror.h"
#include "taosdef.h"
#include "tarray.h"
#include "tglobal.h"
#include "mnodeSdb.h"

// this file implement offline compact mnode wal function

static int32_t _processCompactWal(void *wparam, void *hparam, int32_t qtype, void *unused);
static void*   _getCompactTableFromId(int32_t tableId);
static int     _compactAndSaveWal();

typedef struct {
  ESdbKey   keyType;
  int32_t (*fpDecode)(SSdbRow *pRow); 
  void *    iHandle;
} SSdbCompactTable;

typedef struct {
  void *     wal;
  void *     wal_tmp;
  int32_t    numOfTables;
  SArray*    walHead;
  SSdbCompactTable *tableList[SDB_TABLE_MAX];  
} SSdbCompactMgmt;

static SSdbCompactMgmt   tsSdbComMgmt = {0};

int32_t mnodeCompactWal() {
  // init SSdbCompactMgmt
  tsSdbComMgmt.walHead = taosArrayInit(1024, sizeof(SWalHead));

  // first step: open wal
  SWalCfg walCfg = {.vgId = 1, .walLevel = TAOS_WAL_FSYNC, .keep = TAOS_WAL_KEEP, .fsyncPeriod = 0};
  char    temp[TSDB_FILENAME_LEN] = {0};
  sprintf(temp, "%s/wal", tsMnodeDir);
  tsSdbComMgmt.wal = walOpen(temp, &walCfg);
  if (tsSdbComMgmt.wal == NULL) {
    sdbError("vgId:1, failed to open wal in %s", tsMnodeDir);
    return -1;
  }

  // second step: create wal tmp dir
  sprintf(temp, "%s/wal_tmp", tsMnodeDir);

  if (dnodeCreateDir(temp) < 0) {
   dError("failed to create mnode tmp wal dir: %s, reason: %s", tsMnodeDir, strerror(errno));
   return -1;
  }
  tsSdbComMgmt.wal_tmp = walOpen(temp, &walCfg);

  sdbInfo("vgId:1, open sdb wal for compact");  
  int32_t code = walRestore(tsSdbComMgmt.wal, NULL, _processCompactWal);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, failed to open wal for restore since %s", tstrerror(code));
    return -1;
  }

  sdbInfo("vgId:1, sdb wal load for compact success");

  // third step: save compacted wal
  _compactAndSaveWal();
  return 0;  
}

static void *_getCompactTableFromId(int32_t tableId) {
  return tsSdbComMgmt.tableList[tableId];
}

static int32_t _processCompactWal(void *wparam, void *hparam, int32_t qtype, void *unused) {
  SSdbRow *pRow = wparam;
  SWalHead *pHead = hparam;
  int32_t tableId = pHead->msgType / 10;
  int32_t action = pHead->msgType % 10;

  SSdbCompactTable *pTable = _getCompactTableFromId(tableId);
  assert(pTable != NULL);

  void *  key = sdbGetObjKey(pTable, pRow->pObj);
  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = (int32_t)strlen((char *)key);
  }

  if (action == SDB_ACTION_DELETE) {
    taosHashRemove(pTable->iHandle, key, keySize);
  } else {
    taosHashPut(pTable->iHandle, key, keySize, &pRow->pObj, sizeof(int64_t));
  }

  taosArrayPush(tsSdbComMgmt.walHead, pHead);

  return 0;
}

int _compactAndSaveWal() {
  size_t size = taosArrayGetSize(tsSdbComMgmt.walHead);

  for (size_t i = 0; i < size; ++i) {
    SWalHead *pHead = taosArrayGet(tsSdbComMgmt.walHead, i);
    if (pHead == NULL) {
      break;
    }

    int32_t tableId = pHead->msgType / 10;
    SSdbCompactTable *pTable = _getCompactTableFromId(tableId);
    
    SSdbRow row = {.rowSize = pHead->len, .rowData = pHead->cont, .pTable = pTable};
    (*pTable->fpDecode)(&row);

    void *  key = sdbGetObjKey(pTable, row.pObj);
    int32_t keySize = sizeof(int32_t);
    if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
      keySize = (int32_t)strlen((char *)key);
    }

    void* p = taosHashGet(pTable->iHandle, key, keySize);
    if (p == NULL) {
      continue;
    }

    walWrite(tsSdbComMgmt.wal_tmp, pHead);
  }

  // rename wal dir

  return 0;
}