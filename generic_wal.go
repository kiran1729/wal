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
// This file implements a GenericWAL class that implements functionality
// which can be used by different applications to read/write WAL records and
// drive state recovery from the WAL.

package wal

import (
  "encoding/json"
  "errors"
  "fmt"
  "sort"

  "github.com/golang/glog"

  "zerostack/common/util"
)

// ErrLostOwnership indicates that wal ownership has been lost.
var ErrLostOwnership = errors.New("wal: lost ownership")

// GenericWALRecordHeader represents the WAL entries common to all applications.
type GenericWALRecordHeader struct {
  // Logical sequence number assigned to the record by write ahead log. This
  // field is not valid for checkpoint because checkpoint records do not use
  // LSN ids.
  LSN int64

  // Monotonically increasing version.
  Version int64

  // Flags that indicate the record type. Only one of them is set to true at
  // any time. The corresponding nested object holds rest of data for the
  // record.
  IsRevert   bool
  IsProgress bool

  // Revert specifies the LSN of a previous operation that has failed and
  // should not be replayed on recovery.
  Revert struct {
    LSN int64
  }

  // Progress specifies the LSN and updated wal record of a previous operation
  // which has progressed to a recovery point. During the recovery operation
  // should resume from the recovery point instead of the begining of the
  // operation.
  Progress struct {
    LSN       int64
    NewRecord *GenericWALRecord
  }
}

// GenericWALRecord represents the full WAL record including the fields
// common to all applications along with application specific data
// (within "Payload").
type GenericWALRecord struct {
  // The fields commaon across all applications.
  GenericWALRecordHeader
  // Opaque application specific data. On reads, this data is passed
  // through (without serialization) to the applications.
  Payload json.RawMessage
}

// GenericWAL is a generic class to handle WAL operations.
type GenericWAL struct {
  wal         *WAL
  recoveryMap map[int64]*GenericWALRecord
  // true iff ownership of the wal has been lost. No more writes to the
  // wal are allowed. The user of the wal has to reacquire ownership of the wal
  // and instantiate a new WAL object.
  lostOwnership bool
}

// NewGenericWAL creates a new "GenericWAL" object and returns a pointer to
// it.
func NewGenericWAL(wal *WAL) (*GenericWAL, error) {
  gWal := &GenericWAL{
    wal:         wal,
    recoveryMap: make(map[int64]*GenericWALRecord),
  }
  return gWal, nil
}

// Remove destroys all data in the wal. Corresponding wal object should not be
// used anymore.
func (gWal *GenericWAL) Remove() error {
  return gWal.wal.Remove()
}

// LogDeltaRecord writes an update record to the WAL. It returns a pointer
// to the WAL object ("GenericWALRecord") generated. "payload" represents
// application specific opaque data which is encapsulated within a
// "GenericWALRecord" object and written into the WAL.
func (gWal *GenericWAL) LogDeltaRecord(payload interface{}, version int64) (
  *GenericWALRecord, error) {

  if gWal.lostOwnership {
    return nil, ErrLostOwnership
  }

  walRecord := &GenericWALRecord{}
  walRecord.Version = version

  // serialize the "payload" into the "walRecord".
  data, errMarshal := json.Marshal(payload)
  if errMarshal != nil {
    glog.Errorf("could not serialize payload for the WAL record :: %v",
      errMarshal)
    return nil, errMarshal
  }
  walRecord.Payload = data

  // serialize the full "walRecord" before writing it into the WAL.
  data, errMarshal = json.Marshal(walRecord)
  if errMarshal != nil {
    glog.Errorf("could not serialize the full WAL record :: %v", errMarshal)
    return nil, errMarshal
  }

  lsn, errLog := gWal.writeDataRecord(data)
  if errLog != nil {
    glog.Errorf("could not log delta record to wal :: %v", errLog)
    gWal.lostOwnership = IsLostOwnership(errLog)
    return nil, errLog
  }
  walRecord.LSN = lsn
  return walRecord, nil
}

// LogRevertRecord marks a record as canceled so that it won't be replayed on
// recovery.
func (gWal *GenericWAL) LogRevertRecord(
  record *GenericWALRecord) error {

  if gWal.lostOwnership {
    return ErrLostOwnership
  }

  revert := &GenericWALRecord{Payload: []byte(`""`)}
  revert.IsRevert = true
  revert.Revert.LSN = record.LSN
  data, errMarshal := json.Marshal(revert)
  if errMarshal != nil {
    glog.Errorf("could not serialize undo/rollback record :: %v", errMarshal)
    return errMarshal
  }
  if _, err := gWal.writeDataRecord(data); err != nil {
    glog.Errorf("could not write revert record to wal :: %v", err)
    gWal.lostOwnership = IsLostOwnership(err)
    return err
  }
  return nil
}

// LogProgressRecord marks progress on a previous WAL "record". The "payload"
// represents the updated application specific payload which is encapsulated
// within the progress record.
func (gWal *GenericWAL) LogProgressRecord(record *GenericWALRecord,
  payload interface{}) error {

  if gWal.lostOwnership {
    return ErrLostOwnership
  }

  progress := &GenericWALRecord{Payload: []byte(`""`)}
  progress.IsProgress = true
  progress.Progress.LSN = record.LSN

  // serialize the payload and update the WAL record with it.
  data, errMarshal := json.Marshal(payload)
  if errMarshal != nil {
    glog.Errorf("could not serialize payload:%#v into progress record :: %v",
      payload, errMarshal)
    return errMarshal
  }

  // update the "payload".
  record.Payload = data
  progress.Progress.NewRecord = record

  data, errMarshal = json.Marshal(&progress)
  if errMarshal != nil {
    glog.Errorf("could not serialize progress record :: %v", errMarshal)
    return errMarshal
  }
  if _, err := gWal.writeDataRecord(data); err != nil {
    glog.Errorf("could not write progress record to wal :: %v", err)
    gWal.lostOwnership = IsLostOwnership(err)
    return err
  }
  return nil
}

// writeDataRecord logs a record represented by the "data" byte array
// into the wal.
func (gWal *GenericWAL) writeDataRecord(data []byte) (int64, error) {
  doneCh := make(chan *Response)

  if gWal.lostOwnership {
    return -1, ErrLostOwnership
  }

  lsn, errAdd := gWal.wal.AddDataEntry(data, false /* skipChecksum */, doneCh)
  if errAdd != nil {
    glog.Errorf("could not add wal entry :: %v", errAdd)
    gWal.lostOwnership = true
    return -1, ErrLostOwnership
  }

  response := <-doneCh
  if response == nil {
    glog.Errorf("could not persist delta record to wal")
    gWal.lostOwnership = true
    return -1, ErrLostOwnership
  }

  if response.Result != nil {
    glog.Errorf("could not persist delta record to wal :: %v", response.Result)
    gWal.lostOwnership = true
    return -1, ErrLostOwnership
  }
  return int64(lsn), nil
}

// writeCheckpointRecord logs a checkpoint record represented by the "data"
// byte array.
func (gWal *GenericWAL) writeCheckpointRecord(data []byte) (int64, error) {
  doneCh := make(chan *Response)

  if gWal.lostOwnership {
    return -1, ErrLostOwnership
  }

  lsn, errAdd := gWal.wal.AddCheckpointEntry(data, false, /* skipChecksum */
    doneCh)

  if errAdd != nil {
    glog.Errorf("could not add wal entry :: %v", errAdd)
    gWal.lostOwnership = true
    return -1, ErrLostOwnership
  }

  response := <-doneCh
  if response == nil {
    glog.Errorf("could not persist checkpoint record to wal")
    gWal.lostOwnership = true
    return -1, ErrLostOwnership
  }
  if response.Result != nil {
    glog.Errorf("could not persist checkpoint record to wal :: %v",
      response.Result)
    gWal.lostOwnership = true
    return -1, ErrLostOwnership
  }
  return int64(lsn), nil
}

// Recover reads log records from the WAL in preparation for WAL replay.
func (gWal *GenericWAL) Recover() error {
  entryCh := make(chan *Entry)
  quitCh := make(chan struct{})
  defer close(quitCh)
  go func() {
    if err := gWal.wal.StartRecovery(entryCh, quitCh); err != nil {
      glog.Errorf("could not recover state from local wal :: %v", err)
      entryCh <- nil
    }
  }()

  count := 0
  for entry := range entryCh {
    if entry == nil {
      glog.Errorf("wal recovery channel is closed unexpectedly")
      return fmt.Errorf("wal error")
    }
    count++

    switch entry.Type {
    case CTypeDone:
      glog.Infof("wal recovery is complete because last record is read")
      close(entryCh)

    case CTypeData:
      gWal.updateRecoveryMap(false /* checkpoint */, int64(entry.LSN),
        entry.Data)
      glog.V(1).Infof("recovered a delta record with lsn %v", entry.LSN)

    case CTypeCheckpoint:
      gWal.updateRecoveryMap(true /* checkpoint */, -1, entry.Data)
      glog.V(1).Infof("recovered a checkpoint record with lsn %v", entry.LSN)

    case CTypeError:
      glog.Errorf("wal recovery encountered an unrecoverable error")
      return fmt.Errorf("wal error")

    default:
      glog.Errorf("wal recovery received an unknown or invalid record")
      return fmt.Errorf("wal error")
    }
  }

  return nil
}

// updateRecoveryMap updates the recovery map with wal records that must be
// replayed.
func (gWal *GenericWAL) updateRecoveryMap(
  checkpoint bool, lsn int64, data []byte) error {

  record := &GenericWALRecord{}
  if err := json.Unmarshal(data, record); err != nil {
    glog.Errorf("could not parse wal record during recovery :: %v", err)
    return err
  }
  // We need to explicitly set the lsn field, as "lsn" is not present within
  // the "data" that is serialized on to the wal. The lsn is
  // generated "after" we finish writing "data" into the wal.
  record.LSN = lsn

  // Recovery happens as follows:
  //
  // Checkpoint records are applied immediately because they always store a
  // consistent snapshot with any live operations.
  //
  // Delta records are stored in a map so that when we see a corresponding
  // revert record, we can ignore it.
  //
  // EXTRA NOTES
  //
  // Ideally, we want to replay all delta records. But sometimes an operation
  // could have failed and returned an error. We don't want to replay such
  // operations after a crash, so we use Revert records to remove such items
  // out of the recovery record map.
  //
  // Similarly, a long running operation wants to record its progress so that
  // it can resume from the middle during recovery. They use Progress records
  // to save their progress and can skip over already completed operations
  // during recovery.
  //
  // Progress records are sometimes necessary for correctness because
  // filesystem state cannot be reconstructed through replay. For example,
  // consider this sequence of operations: etcd.Create, etcd.Remove and
  // etcd.Create. If current state of the system is at the second etcd.Create
  // and a crash here would replay etcd.Remove operation which removes the
  // files owned by second etcd.Create operation. So, progress records will
  // help avoid such cases.

  if record.IsRevert {
    delete(gWal.recoveryMap, record.Revert.LSN)
    return nil
  }

  if record.IsProgress {
    if _, ok := gWal.recoveryMap[record.Progress.LSN]; ok {
      gWal.recoveryMap[record.Progress.LSN] = record.Progress.NewRecord
    } else {
      glog.Warningf("progress record %d is ignored because its initial "+
        "record %d is not found in the recovery map", lsn, record.Progress.LSN)
    }
    return nil
  }

  // This includes both delta and checkpoint records.
  gWal.recoveryMap[lsn] = record
  return nil
}

// Replay initiates the replay of recovered WAL records. The caller should
// signal its intent to stop reading from the record channel using the quit
// channel.
func (gWal *GenericWAL) Replay(
  recordChannel chan *GenericWALRecord, quitCh chan struct{}) error {

  if len(gWal.recoveryMap) == 0 {
    recordChannel <- nil
    return nil
  }

  versionRecoveryMap := make(map[int64]*GenericWALRecord)
  for _, record := range gWal.recoveryMap {
    versionRecoveryMap[record.Version] = record
  }

  // Sort records based on the version numbers.
  var keys []int64
  for version := range versionRecoveryMap {
    keys = append(keys, version)
  }
  sort.Sort(util.Int64Slice(keys))

  // Replay the records in sorted version order.
  for _, ver := range keys {
    select {
    case <-quitCh:
      break
    case recordChannel <- versionRecoveryMap[ver]:
    }
  }
  recordChannel <- nil

  // Empty the recovery map.
  gWal.recoveryMap = make(map[int64]*GenericWALRecord)
  return nil
}

// Close closes the "gWal" object.
func (gWal *GenericWAL) Close() error {
  gWal.wal.Close()
  return nil
}

// LogCheckpoint writes a single-record checkpoint to the wal.
func (gWal *GenericWAL) LogCheckpoint(payload interface{},
  version int64) error {

  if gWal.lostOwnership {
    return ErrLostOwnership
  }

  walRecord := &GenericWALRecord{}
  walRecord.Version = version
  // serialize the payload.
  userData, errMarshal := json.Marshal(payload)
  if errMarshal != nil {
    glog.Errorf("could not serialize user payload for WAL :: %v",
      errMarshal)
    return errMarshal
  }
  walRecord.Payload = userData
  data, errMarshal := json.Marshal(walRecord)
  if errMarshal != nil {
    glog.Errorf("could not serialize wal record :: %v", errMarshal)
    return errMarshal
  }

  if err := gWal.wal.StartCheckpoint(); err != nil {
    glog.Errorf("could not start new checkpoint :: %v", err)
    return err
  }
  // TODO: WAL must have the ability to abort a checkpoint on errors,
  // otherwise, there is no way to cancel StartCheckpoint operation, so all
  // future checkpoint attempts will also fail.
  lsn, errWrite := gWal.writeCheckpointRecord(data)
  if errWrite != nil {
    glog.Errorf("could not write checkpoint record :: %v", errWrite)
    gWal.lostOwnership = IsLostOwnership(errWrite)
    return errWrite
  }
  if err := gWal.wal.FinalizeCheckpoint(); err != nil {
    glog.Errorf("could not finalize checkpoint :: %v", err)
    return err
  }
  glog.Infof("new checkpoint is recorded with lsn %d at version %d", lsn,
    version)
  walRecord.LSN = lsn
  return nil
}

// ChangeGenericWALOwnership marks a new node as the wal owner which
// automatically invalidates the current wal owner, if any.
func ChangeGenericWALOwnership(
  nodeUUID, etcdAddr, walName string, changeRecord interface{}) error {

  walStore, errStore := NewStoreEtcd(nodeUUID, etcdAddr, walName)
  if errStore != nil {
    glog.Errorf("could not create wal store object :: %v", errStore)
    return errStore
  }
  dWal, errWAL := NewWAL(walStore)
  if errWAL != nil {
    glog.Errorf("could not create distributed wal for starnet :: %v", errWAL)
    return errWAL
  }
  gWal, errGWal := NewGenericWAL(dWal)
  if errGWal != nil {
    glog.Errorf("could not create generic wal :: %v", errGWal)
    return errGWal
  }
  if err := gWal.Recover(); err != nil {
    glog.Errorf("could not recover wal to append owner change record :: %v",
      err)
    return err
  }
  // Append a dummy, owner change record into the wal so that wal operations
  // from other nodes -- whom may be thinking they are still the owner -- will
  // fail.
  if _, err := gWal.LogDeltaRecord(changeRecord, -1 /*version*/); err != nil {
    glog.Errorf("could not change wal ownership :: %v", err)
    return err
  }
  return nil
}

// IsLostOwnership returns true iff error 'err' indicates loss of
// wal ownership.
func IsLostOwnership(err error) bool {
  return err != nil && err.Error() == ErrLostOwnership.Error()
}

// IsLostOwnership returns true if one or more wal operations have faced
// ownership errors in the past.
func (gWal *GenericWAL) IsLostOwnership() bool {
  // FIXME (bvk) This is not thread-safe.
  return gWal.lostOwnership
}
