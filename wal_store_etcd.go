// Copyright 2015 ZeroStack, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// The StoreEtcd implements the etcd backend for the WAL implementation.
// An etcd backend provides a distributed WAL implementation, allowing
// distributed consumers (spread across multiple nodes) to work on top of it.
//
// The WAL comprises of :
// a) Data entries : These store the potentially uncommitted state changes.
//    They are stored as key-value pairs, where the key is "<lsn>_data"
//    (where "<lsn>" is an integer logical-sequence-number), and the value
//    comprises of the state changes a producer wants to save.
//
// b) Checkpoint entries : These store a checkpoint of the system state.
//    During recovery all the WAL entries before a committed checkpoint can
//    be ignored (and removed). Furthermore all checkpoint entries corresponding
//    to a partial (not finalized) checkpoint can also be dropped (and removed).
//    The checkpoint entries are of the form "<lsn>_cp".
//
// c) Checkpoint-done entries: These entries mark the checkpoints which
//    were successfully committed. These entries are of the form
//    "<lsn>_cp_done" where <lsn> is the logical-sequence-number corresponding
//    to the first entry in the checkpoint and the value stored in this key
//    is the logical-sequence-number corresponding to the last entry in the
//    checkpoint.
//
// Consider the WAL :
//   /wal/zerostack_wal/0_data
//                     /1_data
//                     /2_data
//                     /3_data
//                     /4_cp_done = "6"
//                     /4_cp
//                     /5_cp
//                     /6_cp
//                     /7_data
//                     /8_data
//                     /9_cp
//                     /10_cp
//
// It consists of a WAL comprising of
// a) Data records 0,1,2 and 3 capturing delta state changes.
// b) Finalized (committed) checkpoint records 4,5 and 6.
// c) Data records 7 and 8 capturing delta state changes after the first
//    checkpoint.
// d) Un-finalized (partial) checkpoint records 9 and 10.
// We know that the first checkpoint has been finalized due to the presence of
// the "4_cp_done" key which stores the value of LSN corresponding to the
// final checkpoint entry in this checkpoint (=6).
// On recovery, all records with LSN < the last valid checkpoint and all partial
// checkpoints will be discarded. Therefore before recovery the WAL will be
// modified to look like.
//   /wal/zerostack_wal/4_cp_done = "6"
//                     /4_cp
//                     /5_cp
//                     /6_cp
//                     /7_data
//                     /8_data
//
// We use protocol buffers to serialize/deserialize the WAL entries.
//
// Locking notes : We expect each consumer (reader or writer) of the WAL
// to use its own WAL object working on top of the StoreEtcd backend.
// Therefore the "StoreEtcd" backend is not shared between multiple
// threads. It is only used by a single WAL controller thread.
// Furthermore, the backend is synchronous (does not invoke any
// go-routines), thereby avoiding the need for synchronization between
// go-routines.

// Performance notes : Right now, on WAL initialization, the whole WAL
// is read into memory. This is because a namespace get in guru
// (which internally does a directory get on etcd) reads in both object keys as
// well as values. There is no call to return only the keys.
// This is not a concern for now, as we expect only as much as a few MB of
// data to be stored in the WAL. If we start storing a lot of data in the WAL,
// we will have to revisit this.

package wal

import (
  "fmt"
  "hash/adler32"
  "sort"
  "strconv"
  "strings"
  "sync"

  "github.com/gogo/protobuf/proto"
  "github.com/golang/glog"

  "zerostack/common/constants"
  "zerostack/common/guru"
  "zerostack/common/protofiles"
  "zerostack/common/util"
)

// cNextLSNKey is The key which stores the lsn at which the next record should
// be written at. To guarantee that there is no "lsn" conflict on the backend,
// between the various concurrent writers; the "lsn" is stored inside etcd
// itself. Each writer is required to atomically increment it successfully to
// guarantee unique ownership of the "lsn" being written to.
const cNextLSNKey string = "nextLSN"

type walMapEntry struct {
  dataType EntryType
  value    string
}

// StoreEtcd implements the etcd backend for the "WAL" type
type StoreEtcd struct {
  // Fields required to initialize the persistent distributed backend.
  mu   sync.Mutex // Mutex for in-memory state.
  uuid string     // uuid to be used for the guru library.
  addr string     // IP address of the etcd service.
  root string     // The root directory where the WAL resides.

  guru guru.Guru // Guru instance for reading/writing the WAL.

  // Fields used while reading the wal.
  readIndex int // index into the "lsns" slice, used
  // to read the lsns in sorted order.
  lsns util.Uint64Slice // the lsns found in the wal in sorted
  // order. This is required as it is possible
  // to have some missing lsns.
  keyMap  map[uint64]*walMapEntry // wal records read into memory.
  nextLSN uint64                  // the lsn at which the next wal entry
  // should be written.

  cpStart uint64 // The LSN at which checkpoint was started.
  cpEnd   uint64 // The LSN at which the checkpoint was finished.
}

// NewStoreEtcd creates a new StoreEtcd instance and returns it.
func NewStoreEtcd(uuid, addr, root string) (*StoreEtcd, error) {

  store := &StoreEtcd{
    uuid:   uuid,
    root:   fmt.Sprintf("wal/%s", root),
    addr:   addr,
    keyMap: make(map[uint64]*walMapEntry),
  }

  if err := store.init(); err != nil {
    glog.Errorf("failed initialization: %v", err)
    return nil, err
  }
  return store, nil
}

// StoreExists returns true if the store at "root" exists at the etcd address
// "addr".
func StoreExists(uuid, addr, root string) (bool, error) {
  g, err := guru.MakeMyGuru(uuid, addr, &guru.CBFunctions{
    RefreshFailCB: guruRefreshFailCB,
    LockBrokenCB:  guruLockBrokenCB,
  })
  if err != nil {
    glog.Errorf("error making guru for WAL %s :: %v", root, err)
    return false, err
  }
  defer g.Close()

  if _, err = g.GetNamespaceTimeout(root, false,
    constants.EtcdTimeout); err != nil {
    return true, nil
  }
  return false, nil
}

// Write() takes "op" and writes it down to the storage backend of the WAL.
func (w *StoreEtcd) Write(op *walStoreOp) error {
  id := op.lsn
  var keyName string

  switch op.dataType {
  case CTypeData:
    keyName = fmt.Sprintf("%d_data", id)
  case CTypeCheckpoint:
    if w.cpStart == 0 {
      w.initializeCheckpoint(op.lsn)
    }
    keyName = fmt.Sprintf("%d_cp", id)
  default:
    return fmt.Errorf("invalid operation datatype=%v", op.dataType)
  }

  // Generate the WAL entry.
  etcdWalEntry := new(protofiles.WALStoreEtcdEntry)
  if !op.skipChecksum {
    etcdWalEntry.Adler32Cksum = proto.Uint32(adler32.Checksum(op.data))
  }
  etcdWalEntry.Lsn = proto.Uint64(op.lsn)
  etcdWalEntry.Data = op.data

  // Serialize the WAL entry into  a string for writing into the backend
  // storage.
  value := etcdWalEntry.String()
  doesntExist := guru.NewConditionAlreadyExist(false)
  _, errWrite := w.guru.CompareAndSwapTimeout(w.root, keyName, value,
    doesntExist, constants.EtcdTimeout)
  if errWrite != nil {
    glog.Errorf("setting key %s/%s failed: %v", w.root, keyName, errWrite)
    return errWrite
  }
  glog.V(1).Infof("wrote WAL entry=%s", keyName)
  if op.dataType == CTypeCheckpoint {
    w.cpEnd = op.lsn
  }
  return nil
}

// ReadNext reads the next entry from the WAL and returns its contents
func (w *StoreEtcd) ReadNext() ([]byte, EntryType, uint64, error) {
  index := w.readIndex
  if index >= len(w.lsns) {
    endLSN := uint64(0)
    if len(w.lsns) > 0 {
      endLSN = w.lsns[len(w.lsns)-1]
    }
    glog.V(1).Infof("Reached end of WAL at lsn:=%d", endLSN)
    return nil, CTypeDone, 0, nil
  }
  lsn := w.lsns[index]
  walMapEntry, ok := w.keyMap[lsn]
  if !ok {
    return nil, CTypeError, 0,
      fmt.Errorf("Could not find lsn=%d", lsn)
  }

  etcdWalEntry := new(protofiles.WALStoreEtcdEntry)
  if err := proto.UnmarshalText(walMapEntry.value, etcdWalEntry); err != nil {
    glog.Infof("error while unmarshalling %s: %v", walMapEntry.value, err)
    return nil, CTypeError, 0, err
  }

  if etcdWalEntry.Adler32Cksum != nil {
    if adler32.Checksum(etcdWalEntry.Data) != *(etcdWalEntry.Adler32Cksum) {
      glog.Infof("checksum mismatch for lsn=%d, expected=%d, got=%d",
        lsn, adler32.Checksum(etcdWalEntry.Data), *(etcdWalEntry.Adler32Cksum))
      return nil, CTypeError, 0, fmt.Errorf("checskum mismatch")
    }
  }

  if *(etcdWalEntry.Lsn) != lsn {
    glog.Errorf("got lsn:%d while trying to read lsn:%d", *(etcdWalEntry.Lsn),
      lsn)
    return nil, CTypeError, 0,
      fmt.Errorf("got lsn:%d while reading lsn:%d", *(etcdWalEntry.Lsn),
        lsn)
  }
  glog.V(2).Infof("read WAL entry with LSN=%d", *(etcdWalEntry.Lsn))
  w.readIndex++

  return etcdWalEntry.Data, walMapEntry.dataType, *(etcdWalEntry.Lsn), nil
}

// StartCheckpoint is unimplemented.
func (w *StoreEtcd) StartCheckpoint() error {
  return nil
}

// FinalizeCheckpoint finalizes and commits an in-progress
// checkpoint. It does it by publishing a key with the name
// "<cpStart>_cp_done" with the value "<cpEnd>" where
// "<cpStart>" is the LSN of the first record belonging to this checkpoint.
// "<cpEnd>" is the LSN of the last record belonging to this checkpoint.
func (w *StoreEtcd) FinalizeCheckpoint() error {
  keyName := fmt.Sprintf("%d_cp_done", w.cpStart)
  value := fmt.Sprintf("%d", w.cpEnd)
  doesntExist := guru.NewConditionAlreadyExist(false)
  _, errWrite := w.guru.CompareAndSwapTimeout(w.root, keyName, value,
    doesntExist, constants.EtcdTimeout)
  if errWrite != nil {
    glog.Errorf("finalizing checkpoint %s/%s failed: %v", w.root, keyName, errWrite)
    return errWrite
  }
  glog.V(1).Infof("finalized checkpoint %s up-to LSN=%d", keyName, w.cpEnd)

  // Done with the current checkpoint.
  w.cpStart = 0
  w.cpEnd = 0
  return nil
}

// StartRecovery sets up the WAL for serving read requests for recovery.
func (w *StoreEtcd) StartRecovery() error {
  w.readIndex = 0
  return nil
}

// FinishRecovery indicates that the recovery using the WAL has finished.
func (w *StoreEtcd) FinishRecovery() error {
  return nil
}

// Flush flushes all pending ops to the backend storage.
// Need to do nothing, as there is no caching
// before etcd.
func (w *StoreEtcd) Flush() error {
  // nothing to do.
  return nil
}

// Remove removes the WAL contents from the backend storage.
func (w *StoreEtcd) Remove() error {
  if err := w.guru.DeleteNamespaceTimeout(w.root, false,
    constants.EtcdTimeout); err != nil {
    glog.Errorf("error while deleting %s: %v", w.root, err)
    return err
  }
  if err := w.initRoot(); err != nil {
    glog.Errorf("Error while initializing the root=%s: %v", w.root, err)
    return err
  }
  return nil
}

// INTERNAL methods

// init() initialize the etcd backend for the WAL.
// It creates the root directory for the WAL and updates the fields inside
// the "StoreEtcd" structure to make it ready for reading/writing.
func (w *StoreEtcd) init() error {

  cbFuncs := &guru.CBFunctions{
    RefreshFailCB: guruRefreshFailCB,
    LockBrokenCB:  guruLockBrokenCB,
  }

  guru, errMake := guru.MakeMyGuru(w.uuid, w.addr, cbFuncs)
  if errMake != nil {
    glog.Infof("could not create guru for WAL %s: %v", w.root, errMake)
    return errMake
  }

  w.guru = guru

  if err := w.initRoot(); err != nil {
    glog.Infof("could not initialize root:%v", err)
    return err
  }

  return nil
}

// initRoot() initializes the directory corresponding to this WAL and prepares
// the internal fields in the "StoreEtcd" structure to make it ready for
// reading or writing.
func (w *StoreEtcd) initRoot() error {
  var rollback bool
  dirName := w.root
  // first check if the directory already exists.
  _, errU := w.guru.GetNamespaceTimeout(dirName, false, constants.EtcdTimeout)
  if errU != nil {
    glog.V(1).Infof("could not get the directory %s: %v", dirName, errU)
    // The directory does not exist. Try to create it.
    if errC := w.guru.CreateNamespaceTimeout(dirName,
      constants.EtcdTimeout); errC != nil {
      // failed to create the directory.
      glog.Infof("could not create etcd directory %s: %v", dirName, errC)
      return errC
    }
    // succeeded in creating the directory.
    defer func() {
      if rollback {
        if errD := w.guru.DeleteNamespaceTimeout(dirName, false,
          constants.EtcdTimeout); errD != nil {
          glog.Warningf("could not delete %s on rollback", dirName)
        }
      }
    }()
    glog.V(1).Infof("WAL will use the etcd directory %s", dirName)
  }

  // Directory exists.
  if err := w.initWAL(); err != nil {
    rollback = true
    glog.Infof("could not initialize the WAL: %v", err)
    return err
  }

  return nil
}

// initWAL cleans up the WAL by removing stale entries and partial checkpoints
// and initializes the internal fields of "w" so that the WAL
// can be read from or written to.
func (w *StoreEtcd) initWAL() error {
  // First read the contents of the root directory.
  dirName := w.root
  nodes, errGet := w.guru.GetNamespaceTimeout(dirName, true,
    constants.EtcdTimeout)
  if errGet != nil {
    glog.Infof("failed to get the contents of the directory %s: %v", dirName,
      errGet)
    return errGet
  }
  var lastDataLSN, lastCPStart, lastCPEnd uint64

  for _, node := range nodes {
    if strings.Contains(node.Key, cNextLSNKey) {
      lsn, errParse := strconv.ParseUint(node.Value, 10, 64)
      if errParse != nil {
        return errParse
      }
      glog.Infof("parsing wal contents; got %s:%s", cNextLSNKey,
        node.Value)
      w.nextLSN = lsn
      continue
    }
    lsn, keyType, errP := w.parseKey(node.Key)
    glog.V(1).Infof("parsing wal contents: got key:%s", node.Key)
    if errP != nil {
      glog.Warningf("key %s with unrecognized format: %v", node.Key, errP)
      continue
    }

    if lsn > w.nextLSN {
      w.nextLSN = lsn
    }

    if keyType == 0 {
      // data record.
      if lastDataLSN < lsn {
        lastDataLSN = lsn
      }
      w.keyMap[lsn] = &walMapEntry{CTypeData, node.Value}
    }

    if keyType == 1 {
      // checkpoint record
      w.keyMap[lsn] = &walMapEntry{CTypeCheckpoint, node.Value}
    }

    if keyType == 2 {
      // "checkpoint done" record
      if lastCPStart < lsn {
        lastCPStart = lsn
        lastCPEnd, errP = strconv.ParseUint(node.Value, 10, 64)
        if errP != nil {
          glog.Errorf("Invalid checkpoint-done record %s with value=%s",
            node.Key, node.Value)
          return errP
        }
      }
    }
  }
  glog.V(1).Infof("initialized the WAL, valid checkpoint range=(%d, %d), "+
    "lastDataLSN=%d len(keyMap)=%d", lastCPStart, lastCPEnd, lastDataLSN,
    len(w.keyMap))

  // Cleanup all stale keys. Includes both the stale data and checkpoint
  // entries, and partial checkpoint entries.
  entriesToDelete := make(map[uint64]struct{})
  validCheckpoint := lastCPEnd >= lastCPStart
  for lsn, entry := range w.keyMap {
    if validCheckpoint && lsn < lastCPStart {
      // stale entry
      entriesToDelete[lsn] = struct{}{}
    } else if entry.dataType == CTypeCheckpoint {
      if (validCheckpoint && lsn > lastCPEnd) ||
        (!validCheckpoint && lsn >= lastCPStart) {
        // partial checkpoint
        entriesToDelete[lsn] = struct{}{}
      }
    }
  }

  // delete the stale entries.
  for lsn := range entriesToDelete {
    entry, ok := w.keyMap[lsn]
    if ok {
      key, errG := w.generateKey(entry.dataType, lsn)
      if errG != nil {
        glog.Warningf("could not generate key for LSN=%d", lsn)
        continue
      }
      if err := w.guru.DeleteNamespaceTimeout(
        fmt.Sprintf("%s/%s", w.root, key),
        false, constants.EtcdTimeout); err != nil {

        glog.Warningf("could not remove key %s: %v", key, err)
      }
      delete(w.keyMap, lsn)
    }
  }

  // Set the "cNextLSN" key appropriately.
  // This should work for older wals as well,
  // which do not have this key set.
  doesntExist := guru.NewConditionAlreadyExist(false)
  value := fmt.Sprintf("%d", w.nextLSN)
  _, errWrite := w.guru.CompareAndSwapTimeout(w.root, cNextLSNKey, value,
    doesntExist, constants.EtcdTimeout)
  if errWrite != nil {
    glog.Infof("key %s/%s already exists in the wal: %v", w.root,
      cNextLSNKey, errWrite)
  } else {
    glog.Infof("key %s/%s created with value:%s in the wal", w.root,
      cNextLSNKey, value)
  }

  var firstLSN, lastLSN uint64

  // Initialize the various fields.
  if len(w.keyMap) > 0 {
    // initialize the lsn.
    lsns := make(util.Uint64Slice, len(w.keyMap))
    ii := 0
    for lsn := range w.keyMap {
      lsns[ii] = lsn
      ii++
    }
    sort.Sort(lsns)
    firstLSN = lsns[0]
    lastLSN = lsns[len(lsns)-1]
    w.lsns = lsns
  } else {
    firstLSN = 0
    lastLSN = 0
    w.lsns = make(util.Uint64Slice, 0)
  }

  glog.Infof("finished parsing WAL:%s. valid checkpoint range=(%d, %d)"+
    ", firstLSN=%d, lastLSN=%d, num entries=%d", w.root, lastCPStart, lastCPEnd,
    firstLSN, lastLSN, len(w.keyMap))
  return nil
}

// parseKey() parses an entry "key" in the WAL and returns its constituent
// parts as a tuple of the form (<lsn>, <type>, <error>).
// <lsn> is the logical-sequence-number corresponding to the WAL entry.
// <type> is a simple encoding of the type of WAL entry. It is
// 0 : for entries of the form "<id>_data" corresponding to data entries.
// 1:  for entries of the form "<id>_cp" corresponding to checkpoint entries.
// 2 : for entries of the form "<id>_cp_done" corresponding to the checkpoint
//     done markers.
//
func (w *StoreEtcd) parseKey(key string) (uint64, uint8, error) {
  parts := strings.Split(key, "/")
  // Pick the last entry
  keyName := parts[len(parts)-1]

  parts = strings.Split(keyName, "_")

  if len(parts) != 2 && len(parts) != 3 {
    return 0, 0, fmt.Errorf("invalid WAL key %s", keyName)
  }

  seq, err := strconv.ParseUint(parts[0], 10, 64)
  if err != nil {
    return 0, 0, fmt.Errorf("invalid WAL key %s", keyName)
  }

  var keyType uint8
  if parts[1] == "data" {
    keyType = 0
  } else if parts[1] == "cp" && len(parts) == 2 {
    keyType = 1
  } else if parts[1] == "cp" && parts[2] == "done" {
    keyType = 2
  } else {
    return 0, 0, fmt.Errorf("invalid WAL key %s", keyName)
  }
  return seq, keyType, nil
}

// generateKey generates the key used to store an object of type "dataType"
// and LSN="lsn" in the etcd WAL.
func (w *StoreEtcd) generateKey(dataType EntryType, lsn uint64) (string,
  error) {
  if dataType == CTypeData {
    return fmt.Sprintf("%d_data", lsn), nil
  }
  if dataType == CTypeCheckpoint {
    return fmt.Sprintf("%d_cp", lsn), nil
  }
  return "", fmt.Errorf("invalid WAL entry")
}

// initializeCheckpoint() is invoked to do any book keeping before checkpoint
// records are written.
func (w *StoreEtcd) initializeCheckpoint(lsn uint64) error {

  id := lsn
  w.cpStart = id
  w.cpEnd = id
  glog.V(1).Infof("checkpoint start=end=%d", w.cpStart)
  return nil
}

// guruRefreshFailCB() is the callback invoked when a refresh on a lock fails.
func guruRefreshFailCB(namespace, key, value string,
  ttl, refreshInterval uint64) {
  // TODO: Do something else.
  glog.Warning("lock refresh fail callback invoked")
}

// guruLockBrokenCB() is the callback invoked when a lock breaks,
// possibly due to the holder getting partitioned out. But nothing needs
// to be done as no etcd locks are acquired.
func guruLockBrokenCB(name string) {
  glog.Warning("lock broken callback invoked for lock %s", name)
}

// IncNextLSN atomically increments the "cNextLSNKey". A success indicates
// continued ownership of the wal, and the right to write the "currLSN + 1"th
// lsn entry. The old and new values of "cNextLSNKey" are returned on success.
func (w *StoreEtcd) IncNextLSN(currLSN uint64) (uint64, uint64, error) {
  prevValue := guru.NewConditionMatchValue(fmt.Sprintf("%d", currLSN))
  newValue := fmt.Sprintf("%d", currLSN+1)
  _, errWrite := w.guru.CompareAndSwapTimeout(w.root, cNextLSNKey, newValue,
    prevValue, constants.EtcdTimeout)
  if errWrite != nil {
    glog.Errorf("setting key %s/%s failed: %v", w.root, cNextLSNKey, errWrite)
    return currLSN, 0, errWrite
  }
  return currLSN, currLSN + 1, nil
}

// NextLSN returns lsn at which the next entry in the wal should be written.
func (w *StoreEtcd) NextLSN() (uint64, error) {
  return w.nextLSN, nil
}
