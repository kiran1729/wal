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
// WAL implements a write ahead log for supporting journaling. Users of a WAL
// can write entries into the WAL that are persisted and can be read back
// after a crash and restart.
//
// The caller can add two types of entries - Data entries and Checkpoint
// entries. The contents of the entries are byte slices that the WAL does not
// parse or understand. It is the caller's responsibility to store appropriate
// redo or undo information in the entries to reconstruct the state after a
// crash and recovery.
//
// Checkpointing is supported by adding checkpoint entries between a
// StartCheckpoint/FinalizeCheckpoint pair of calls. Any checkpoint that
// is started but not finalized is discarded. Checkpointing reduces the
// number of entries sent to the caller on recovery. When recovery is
// initiated, all entries starting from the last completed checkpoint are
// sent to the caller. On a recovery, the WAL directory is cleaned up to
// delete all old entries before the last finalized checkpoint.
//
// WAL can use different backends for its persistent storage. All backends
// need to implement the WALStore interface below. Two examples are a WALStore
// that uses local filesystem as persistent store and another that uses
// etcd as its persistent store.
//
// Writing to the WAL is done asynchronously. All pending writes are added
// to an "opList" from which a controller go-routine drains ops and writes
// them to the backend. Once writes are committed to the backend, the
// controller issues completions via a channel to the writer.
//
// Synchronization : Multiple writers can write to the WAL. The writes are
// ordered via a logical-sequence number (lsn) and added to the opList.
// The generation of the lsn and addition to the opList is synchronized.
// Writes to the backend are single threaded (done by the controller) and
// therefore do not need to be synchronized.
//
// TODO: Have a name associated with a WAL and be able to list out
// WALs.
//
// TODO: We may need persistent locks to
// enforce that there is only a a single controller writing to a
// particular WAL.
//
// TODO: Cleanup the WAL asynchronously when FinalizeCheckpoint()
// is called.

package wal

import (
  "fmt"
  "sync"

  "github.com/golang/glog"

  "zerostack/common/util"
)

// Response is sent by the WAL in the response channel to the
// AddDataEntry/AddCheckpointEntry caller.
type Response struct {
  LSN    uint64
  Result error
}

// EntryType specifies to the caller the type of WAL entry being sent on
// recovery.
type EntryType uint8

// CTypeXXX are the types in the Response sent back to the user.
const (
  CTypeError EntryType = iota
  CTypeData
  CTypeCheckpoint
  CTypeDone
)

// Entry is the data sent to caller on a recovery. The Type field tells the
// caller if the Data slice is checkpoint bytes or data entry bytes.
type Entry struct {
  Type EntryType // type of the entry
  LSN  uint64    // Log Sequence Number (ID) of the entry
  Data []byte    // byte slice read from WAL
}

// Store is the interface that needs to be implemented by backend persistent
// storage that will be used by WAL to write to persistent medium. See the
// description in StoreFS for the definition of each of the functions.
type Store interface {
  NextLSN() (uint64, error)
  IncNextLSN(uint64) (uint64, uint64, error)
  Write(*walStoreOp) error
  ReadNext() ([]byte, EntryType, uint64, error)
  StartCheckpoint() error
  FinalizeCheckpoint() error
  StartRecovery() error
  FinishRecovery() error
  Flush() error
  Remove() error
}

type walStoreOp struct {
  lsn          uint64
  prevLSN      uint64
  dataType     EntryType
  skipChecksum bool
  data         []byte
  done         chan *Response
}

// WAL is the main object that implements the Write Ahead Log.
type WAL struct {
  store        Store
  mu           sync.Mutex         // mutex to protect shared state.
  currLSN      uint64             // log sequence number of last entry written.
  inCheckpoint bool               // if we are currently checkpointing.
  inRecovery   bool               // if we are currently doing recovery.
  opList       *util.SyncMap      // map[id]*walOp - list of ops to process.
  waitGroup    sync.WaitGroup     // used to synchronize WAL shutdown.
  opChan       chan struct{}      // channel to signal the controller to do ops.
  flushOpChan  chan chan struct{} // channel to signal the controller to Sync.
  pauseOpChan  chan struct{}      // channel to signal controller to pause.
  resumeOpChan chan struct{}      // channel to signal controller to resume.
  quitChan     chan struct{}      // channel to signal controller to quit.
}

// NewWAL creates the WAL object and returns the reference.
func NewWAL(store Store) (*WAL, error) {

  wal := &WAL{
    store:        store,
    opList:       util.NewSyncMap(),
    opChan:       make(chan struct{}),
    flushOpChan:  make(chan chan struct{}),
    pauseOpChan:  make(chan struct{}),
    resumeOpChan: make(chan struct{}),
    quitChan:     make(chan struct{}),
  }
  // TODO: Add a method Start() to start the WAL. This method
  // will also invoke an Init() method on the backing store (WalStore),
  // instead of the backing store doing an init in its constructor.
  // The existing Close() method would undo the actions performed by
  // Start().

  wal.waitGroup.Add(1)
  go wal.startController()
  return wal, nil
}

// AddDataEntry adds the data to the write queue and returns the LSN of the
// created entry and an error status. The return status only indicates if the
// entry was successfully queued. The status of the persistence of the data is
// sent in the supplied done channel.
func (w *WAL) AddDataEntry(data []byte, skipChecksum bool,
  done chan *Response) (uint64, error) {

  // TODO refactor the method so we can use a "defer Unlock()".
  w.mu.Lock()
  prevLSN, newLSN, errInc := w.store.IncNextLSN(w.currLSN)
  if errInc != nil {
    w.mu.Unlock()
    return 0, errInc
  }
  w.currLSN = newLSN
  op := &walStoreOp{dataType: CTypeData,
    data:         data,
    skipChecksum: skipChecksum,
    lsn:          w.currLSN,
    prevLSN:      prevLSN,
    done:         done,
  }
  w.opList.Add(op.lsn, op)
  w.mu.Unlock()

  glog.V(1).Infof("added data entry with LSN=%d, of size %d", w.currLSN,
    len(data))

  // signal the controller
  go func() { w.opChan <- struct{}{} }()

  return w.currLSN, nil
}

// AddCheckpointEntry adds the data to the checkpoint record and returns the
// LSN of the created entry and an error status. The return status only
// indicates if the entry was successfully queued. The status of the
// persistence of the data is sent in the supplied done channel.
func (w *WAL) AddCheckpointEntry(data []byte, skipChecksum bool,
  done chan *Response) (uint64, error) {

  // TODO refactor the method so we can use a "defer Unlock()".
  w.mu.Lock()
  prevLSN, newLSN, errInc := w.store.IncNextLSN(w.currLSN)
  if errInc != nil {
    w.mu.Unlock()
    return 0, errInc
  }
  w.currLSN = newLSN
  op := &walStoreOp{dataType: CTypeCheckpoint,
    data:         data,
    skipChecksum: skipChecksum,
    lsn:          w.currLSN,
    prevLSN:      prevLSN,
    done:         done,
  }
  w.opList.Add(op.lsn, op)
  w.mu.Unlock()

  glog.V(1).Infof("added checkpoint entry with LSN=%d, of size %d", w.currLSN,
    len(data))

  // signal the controller
  go func() { w.opChan <- struct{}{} }()
  return w.currLSN, nil
}

// StartCheckpoint starts a new checkpoint. It is temporary unless
// FinalizeCheckpoint is called. If we ever crash after StartCheckpoint but
// before FinalizeCheckpoint is called then any records associated with the
// checkpoint are discarded. If multiple goroutines call StartCheckpoint,
// only one of the goroutines will succeed and others will get an error since
// we allow only one checkpoint to be active at a time.
func (w *WAL) StartCheckpoint() error {
  w.mu.Lock()
  defer w.mu.Unlock()
  if w.inCheckpoint {
    glog.Error("starting a checkpoint while already in checkpoint")
    return fmt.Errorf("startCheckpoint while already checkpointing")
  }
  err := w.store.StartCheckpoint()
  if err != nil {
    glog.Error("starting checkpoint in store failed")
    return fmt.Errorf("starting checkpoint in store failed")
  }
  w.inCheckpoint = true
  return nil
}

// FinalizeCheckpoint finalizes the checkpoint and makes the checkpoint
// permanent. It is a synchronous call.
func (w *WAL) FinalizeCheckpoint() error {
  w.mu.Lock()
  defer w.mu.Unlock()
  if !w.inCheckpoint {
    glog.Error("finalizing a checkpoint while not checkpoint")
    return fmt.Errorf("finalizing while not checkpointing")
  }

  w.Flush()

  err := w.store.FinalizeCheckpoint()
  if err != nil {
    glog.Error("finalizing checkpoint in store failed")
    return fmt.Errorf("finalizing checkpoint in store failed")
  }
  w.inCheckpoint = false
  return nil
}

// StartRecovery starts a recovery using this WAL. The directory is parsed to
// find the last finished checkpoint and the WAL entries from that checkpoint
// onwards are sent to the caller on the recoveryCh. The send on the recoveryCh
// is done in a synchronous manner before reading the next entry from the WAL.
// The recovery handler can process them in order from the recoveryCh.
// When the entry.Type is CTypeDone then the recovery has sent the last
// known entry from the WAL. If CTypeError is received on the channel,
// caller will have to assume there is some serious error and the WAL is
// corrupted or WAL store is not available for IO. The caller should signal its
// intent to stop reading from the recovery channel using the quit channel.
func (w *WAL) StartRecovery(
  recoveryCh chan *Entry, quitCh chan struct{}) error {

  w.mu.Lock()
  // TODO: Any reason to unlock before doing the whole Recovery?
  defer w.mu.Unlock()
  if w.inRecovery {
    glog.Error("StartRecovery when already in recovery")
    return fmt.Errorf("StartRecovery when already in recovery")
  }
  w.inRecovery = true

  glog.Infof("starting recovery")

  errS := w.store.StartRecovery()
  if errS != nil {
    glog.Error("error starting recovery :: ", errS)
    return fmt.Errorf("error starting recovery :: %v", errS)
  }
  for {
    data, dataType, lsn, errRd := w.store.ReadNext()
    if lsn > w.currLSN {
      w.currLSN = lsn
    }
    if errRd != nil {
      glog.Errorf("error in reading WAL entry :: %v", errRd)
      dataType = CTypeError
    }
    select {
    case <-quitCh:
      glog.Warningf("wal reading cancelled")
      return nil
    case recoveryCh <- &Entry{Type: dataType, LSN: lsn, Data: data}:
    }
    if dataType == CTypeDone {
      glog.Infof("finished processing WAL recovery. LSN=%d", w.currLSN)
      w.store.FinishRecovery()
      break
    }
  }
  w.inRecovery = false
  // Retrieve the last lsn according to the wal backend; and use it, if
  // its greater than the last lsn read from the wal.
  endLSN, errLSN := w.store.NextLSN()
  if errLSN != nil {
    glog.Errorf("error determining the lsn stored in the backend :: %v", errLSN)
    return errLSN
  }
  if endLSN > w.currLSN {
    w.currLSN = endLSN
  }
  return nil
}

// Flush flushes all the pending disk operations and Sync() the files in a
// synchronous manner.
func (w *WAL) Flush() {
  respChan := make(chan struct{})
  // TODO: Receive an error over the channel.
  w.flushOpChan <- respChan
  <-respChan
  glog.V(1).Infof("flushed all wal entries")
}

// Remove deletes all the WAL files and returns the WAL to pristine state.
// This is useful for testing. Note that Remove should be called at the
// beginning before doing StartRecovery etc.
func (w *WAL) Remove() error {
  w.pauseOpChan <- struct{}{}
  defer func() { w.resumeOpChan <- struct{}{} }()
  return w.store.Remove()
}

// Close flushes the contents of the WAL and stops all the go-routines
// associated with it.
func (w *WAL) Close() {
  w.Flush()

  // close the "quitChan" to broadcast this to all the listeners.
  close(w.quitChan)
  w.waitGroup.Wait()
}

// startController is the goroutine that dispatches the IO operations unless
// it is paused due to recovery channel when it will not do ops from the opList.
// It helps implement an async interface to the WAL.
func (w *WAL) startController() error {
  defer w.waitGroup.Done()

  opChan := w.opChan

  for {
    select {
    case <-opChan:
      w.executeNextOp()
    case rspCh := <-w.flushOpChan:
      w.executeAllOps()
      w.store.Flush()
      rspCh <- struct{}{}
    case _ = <-w.pauseOpChan:
      opChan = nil
    case _ = <-w.resumeOpChan:
      w.store.Flush()
      opChan = w.opChan
    case _, more := <-w.quitChan:
      if !more {
        w.executeAllOps()
        w.store.Flush()
        glog.V(1).Info("stopped the wal controller")
        return nil
      }
    }
  }
}

// minLsnFunc is the lambda used to find the op with minimum op id.
func (w *WAL) minLsnFunc(val1 interface{}, val2 interface{}) bool {
  return val1.(*walStoreOp).lsn < val2.(*walStoreOp).lsn
}

// executeNextOp picks the next Op from the opList with the minimum id and
// executes the op.
func (w *WAL) executeNextOp() error {
  lsnInf, _ := w.opList.CmpFunc(w.minLsnFunc)
  if lsnInf == nil {
    glog.V(2).Infof("nothing in the ops to process")
    return nil
  }
  lsn := lsnInf.(uint64)
  opInf, ok := w.opList.Del(lsn)
  if !ok {
    // This can happen because flush flushed it already
    glog.V(2).Infof("did not find expected op in opList for lsn : %d", lsn)
    return nil
  }

  op, ok := opInf.(*walStoreOp)
  if !ok {
    glog.V(2).Infof("unexpected op in opList for lsn : lsn", lsn)
    return nil
  }

  glog.V(2).Infof("executing lsn %d", lsn)

  errW := w.store.Write(op)
  if errW != nil {
    glog.Errorf("error doing write operation :: %v", errW)
  }

  // signal the original WAL caller
  go func() {
    glog.V(2).Infof("acknowledging lsn %d", lsn)
    op.done <- &Response{op.lsn, errW}
    glog.V(2).Infof("acknowledged lsn %d", lsn)
  }()
  return errW
}

// executeAllOps picks all ops in sorted order of their id and executes them
// right away.
// TODO: Maybe we can just wait for disk controller to finish
// all ops one by one using some waitgroup instead of executing them here and
// controller trying again when it picks up diskOpCh?
func (w *WAL) executeAllOps() error {
  numOps := w.opList.Len()
  for ii := 0; ii < numOps; ii++ {
    w.executeNextOp()
  }
  return nil
}
