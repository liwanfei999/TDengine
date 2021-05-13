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

#include <sys/stat.h>
#include <sys/types.h>
#include "os.h"
#include "hash.h"
#include "tutil.h"
#include "tref.h"
#include "taoserror.h"
#include "taosdef.h"
#include "tarray.h"
#include "tglobal.h"
#include "mnodeCompact.h"
#include "mnodeInt.h"
#include "mnodeSdb.h"

//int32_t tsCompactSdbRid;

// this file implement offline compact mnode wal function
static int     _mnodeCreateDir(const char *dir);
static int32_t _processCompactWal(void *wparam, void *hparam, int32_t qtype, void *unused);
static void*   _getCompactTableFromId(int32_t tableId);
static int32_t _saveCompactWal(void *wparam, void *hparam, int32_t qtype, void *unused);
//static int     _compactAndSaveWal();

typedef struct {
  char      name[12];
  ESdbKey   keyType;
  ESdbTable id;
  int32_t   hashSessions;
  int32_t (*fpDecode)(SSdbRow *pRow); 
  void *    iHandle;
} SSdbCompactTable;

typedef struct {
  void *     wal;
  void *     wal_tmp;
  int32_t    newWalCount;
  int32_t    numOfTables;
  SArray*    walHead;
  SSdbCompactTable *tableList[SDB_TABLE_MAX];  
} SSdbCompactMgmt;

static SSdbCompactMgmt   tsSdbComMgmt = {0};

int32_t mnodeCompactWal() {
  sdbInfo("vgId:1, start compact mnode wal...");

  // init SSdbCompactMgmt
  walInit();
  mnodeInitMnodeCompactTables();
  tsSdbComMgmt.walHead = taosArrayInit(1024, sizeof(SWalHead));
  tsSdbComMgmt.newWalCount = 0;

  // first step: open wal
  //tsCompactSdbRid = taosOpenRef(10, _compactSdbCloseTableObj);
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

  if (_mnodeCreateDir(temp) < 0) {
   sdbError("failed to create mnode tmp wal dir: %s, reason: %s", tsMnodeDir, strerror(errno));
   return -1;
  }
  tsSdbComMgmt.wal_tmp = walOpen(temp, &walCfg);
  walRenew(tsSdbComMgmt.wal_tmp);

  sdbInfo("vgId:1, open sdb wal for compact");  
  int32_t code = walRestore(tsSdbComMgmt.wal, NULL, _processCompactWal);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, failed to open wal for restore since %s", tstrerror(code));
    return -1;
  }
  walClose(tsSdbComMgmt.wal);
  tsSdbComMgmt.wal = walOpen(temp, &walCfg);

  // third step: save compacted wal
  //_compactAndSaveWal();
  code = walRestore(tsSdbComMgmt.wal, NULL, _saveCompactWal);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, failed to open wal for restore since %s", tstrerror(code));
    return -1;
  }

  walFsync(tsSdbComMgmt.wal_tmp, true);
  walClose(tsSdbComMgmt.wal_tmp);

  sdbInfo("vgId:1, sdb wal load for compact success,wal count:%d", tsSdbComMgmt.newWalCount);
  return 0;  
}

int64_t sdbOpenCompactTable(SSdbTableCompactDesc *pDesc) {
  SSdbCompactTable *pTable = (SSdbCompactTable *)calloc(1, sizeof(SSdbCompactTable));

  if (pTable == NULL) return -1;

  tstrncpy(pTable->name, pDesc->name, sizeof(pTable->name));
  pTable->keyType      = pDesc->keyType;
  pTable->id           = pDesc->id;
  pTable->fpDecode     = pDesc->fpDecode;

  _hash_fn_t hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  }
  pTable->iHandle = taosHashInit(pTable->hashSessions, hashFp, true, HASH_ENTRY_LOCK);

  tsSdbComMgmt.numOfTables++;
  tsSdbComMgmt.tableList[pTable->id] = pTable;

  return 0;
}

static int _mnodeCreateDir(const char *dir) {
  if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
    return -1;
  }
  
  return 0;
}

static void *_getCompactTableFromId(int32_t tableId) {
  return tsSdbComMgmt.tableList[tableId];
}

static void *sdbCompactGetObjKey(void* table, void *key) {
  SSdbCompactTable *pTable = (SSdbCompactTable *)table;
  if (pTable->keyType == SDB_KEY_VAR_STRING) {
    return *(char **)key;
  }

  return key;
}

static int32_t _processCompactWal(void *wparam, void *hparam, int32_t qtype, void *unused) {
  //sdbInfo("_processCompactWal %p %d", hparam, qtype);

  ASSERT(wparam == NULL);

  SSdbRow *pRow;
  SWalHead *pHead = hparam;
  int32_t tableId = pHead->msgType / 10;
  int32_t action = pHead->msgType % 10;

  SSdbCompactTable *pTable = _getCompactTableFromId(tableId);
  assert(pTable != NULL);

  SSdbRow row = {.rowSize = pHead->len, .rowData = pHead->cont, .pTable = pTable};
  (*pTable->fpDecode)(&row);
  pRow = &row;

  void *  key = sdbCompactGetObjKey(pTable, pRow->pObj);
  if (key == NULL) {
    sdbInfo("key NULL, type:%d", pTable->keyType);
    return 0;
  }
  
  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = (int32_t)strlen((char *)key);
  }

  sdbInfo("add key %s, keySize: %d, type:%d, action:%d,cont:%p", (char*)key, keySize, pTable->keyType, action,pRow->pObj);

  if (keySize > 0) {
    if (action == SDB_ACTION_DELETE) {
      taosHashRemove(pTable->iHandle, key, keySize);
    } else {
      taosHashPut(pTable->iHandle, key, keySize, &pRow->pObj, sizeof(int64_t));
    }
  }

  taosArrayPush(tsSdbComMgmt.walHead, pHead);

  return 0;
}

static int32_t _saveCompactWal(void *wparam, void *hparam, int32_t qtype, void *unused) {
  //sdbInfo("_processCompactWal %p %d", hparam, qtype);

  ASSERT(wparam == NULL);

  SSdbRow *pRow;
  SWalHead *pHead = hparam;
  int32_t tableId = pHead->msgType / 10;
  int32_t action = pHead->msgType % 10;

  SSdbCompactTable *pTable = _getCompactTableFromId(tableId);
  assert(pTable != NULL);

  SSdbRow row = {.rowSize = pHead->len, .rowData = pHead->cont, .pTable = pTable};
  (*pTable->fpDecode)(&row);
  pRow = &row;

  void *  key = sdbCompactGetObjKey(pTable, pRow->pObj);
  if (key == NULL) {
    sdbInfo("key NULL, type:%d", pTable->keyType);
    return 0;
  }
  
  int32_t keySize = sizeof(int32_t);
  if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
    keySize = (int32_t)strlen((char *)key);
  }

  sdbInfo("new add key %s, keySize: %d, type:%d, action:%d,cont:%p", (char*)key, keySize, pTable->keyType, action,pRow->pObj);

  if (keySize > 0) {
    void* p = taosHashGet(pTable->iHandle, key, keySize);
    if (p == NULL) {
      return 0;
    }
  }

  walWrite(tsSdbComMgmt.wal_tmp, pHead);
  ++tsSdbComMgmt.newWalCount;

  return 0;
}

/*
int _compactAndSaveWal() {
  size_t size = taosArrayGetSize(tsSdbComMgmt.walHead);
  size_t count = 0;
  sdbInfo("wal size:%ld", size);

  for (size_t i = 0; i < size; ++i) {
    SWalHead *pHead = taosArrayGet(tsSdbComMgmt.walHead, i);
    if (pHead == NULL) {
      break;
    }

    int32_t tableId = pHead->msgType / 10;
    SSdbCompactTable *pTable = _getCompactTableFromId(tableId);
    
    SSdbRow row = {.rowSize = pHead->len, .rowData = pHead->cont, .pTable = pTable};
    (*pTable->fpDecode)(&row);

    void *  key = sdbCompactGetObjKey(pTable, row.pObj);
    int32_t keySize = sizeof(int32_t);
    if (pTable->keyType == SDB_KEY_STRING || pTable->keyType == SDB_KEY_VAR_STRING) {
      keySize = (int32_t)strlen((char *)key);
    }

    if (keySize > 0) {
      void* p = taosHashGet(pTable->iHandle, key, keySize);
      if (p == NULL) {
        continue;
      }
    }

    sdbInfo("key:%s, keySize:%d,type:%d,head:%p", (char*)key, keySize, pTable->keyType,pHead);

    count += 1;
    walWrite(tsSdbComMgmt.wal_tmp, pHead);
  }

  sdbInfo("new wal size:%ld", count);

  walFsync(tsSdbComMgmt.wal_tmp, true);
  walClose(tsSdbComMgmt.wal_tmp);
  // rename wal dir

  return 0;
}
*/