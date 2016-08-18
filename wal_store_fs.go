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
// The StoreFS implements the wal.Store interface on a filesystem. It uses a
// sequence of file numbers on disk to implement the storage. WAL entries are
// written to the currently active sequence number file till the file hits some
// size and then a new file with next sequence number is created. When
// processing the files, the files are read in order of the sequence number
// and appropriately cleaned up. Note that the sequence number will roll over
// at some point but no disk will be able hold the number of entries that will
// cause a rolled over sequence number to conflict with a still valid WAL file.
//
// When the StoreFS is started it reads the directory and finds all files
// and computes the sequence number it is expected to start from based on the
// files on the disk.
//
// When a recovery is initiated, the Store will reset the read sequence
// number to the lowest finished checkpoint record.

package wal

import (
  "bytes"
  "encoding/binary"
  "flag"
  "fmt"
  "hash/adler32"
  "io"
  "io/ioutil"
  "os"
  "sort"
  "strconv"
  "strings"
  "sync"

  "github.com/golang/glog"

  "zerostack/common/util"
)

const (
  cValidMagic   uint32 = 0xC001C0DE
  cInvalidMagic uint32 = 0xDEADBEEF
  // Sizes (256 MB max size each)
)

var (
  // TODO: evaluate the right size for data file.
  gMaxDataFileSize = flag.Int64("max_data_file_size", int64(256*util.MB),
    "maximum size of a data entries file")
)

// IMPORTANT: Update this if you update the StoreRecord below!
// This constant reflects the size of all fields other than the Data field in
// StoreRecord.
const cStoreFSRecordInfoSize int = 28

// StoreRecord is the structure of the record as stored on disk.
// - Magic: 4 bytes of cValidMagic stored at the beginning of every
//          record
// - LSN:  log sequence number of the entry in data
// - PrevLSN: log sequence number of the previous entry in WAL
// - DataSize: size of the data without the checksum
// - Checksum: adler32 checksum of data. It is 0 if checksum was not computed.
// - Data: the WAL data sent by caller
type storeFSRecord struct {
  Magic    uint32
  LSN      uint64
  PrevLSN  uint64
  DataSize uint32
  Checksum uint32
  Data     []byte
}

type walFileInfo struct {
  name     string
  file     *os.File
  offset   int64
  fileType EntryType
}

// StoreFS implements the Store interface using the filesystem. It needs
// the directory where it should store it's files as the input.
type StoreFS struct {
  root string // directory where the WAL files are stored
  // state
  mu           sync.Mutex      // mutex for state variables other than opList
  inRecovery   bool            // indicates we are in the middle of recovery
  firstSeqID   uint64          // lowest seq id found in Store root
  lastSeqID    uint64          // lowest seq id found in Store root
  readSeqID    uint64          // active seq id file to be read for recovery
  writeSeqID   uint64          // active seq id file to be written to store
  currDataFile *walFileInfo    // active data file handle being written to
  currCPFile   *walFileInfo    // active checkpoint file handle being written
  currRecFile  *walFileInfo    // active checkpoint file handle being written
  opID         util.SequenceID // opID sequence number for opList
}

// NewStoreFS creates a new StoreFS object and calls the InitRoot() and
// parseWALContents methods to do basic checks and initialize state from the
// current state on disk.
func NewStoreFS(root string) (*StoreFS, error) {
  store := &StoreFS{
    root: root,
  }
  errI := store.initRoot()
  if errI != nil {
    glog.Errorf("error initializing StoreFS root :: %v", errI)
    return nil, errI
  }
  errP := store.parseWALContents()
  if errP != nil {
    glog.Errorf("error parsing StoreFS :: %v", errP)
    return nil, errP
  }
  return store, nil
}

// parseWALContents initializes the WAL store using the directory.
// It checks if the store directory is available for reading and writing.
// If the directory looks fine, it will do some consistency checks and
// initializes the sequence numbers using the filenames in the directory.
// Any data files older than the last completed checkpoint are also deleted.
func (w *StoreFS) parseWALContents() error {
  // Read and parse the WAL directory
  files, errDir := ioutil.ReadDir(w.root)
  if errDir != nil {
    glog.Errorf("could not read the Store root : %s :: %v", w.root, errDir)
    return fmt.Errorf("could not read Store root :: %v", errDir)
  }
  fileMap := make(map[uint64]string)
  var lastCPSeqID uint64
  for _, file := range files {
    seq, isData, isTmp, errP := w.parseFilename(file.Name())
    if errP != nil {
      glog.Errorf("found non WAL file : %s :: %v", file.Name(), errP)
      continue
    }
    if !isData && isTmp {
      errRem := os.Remove(w.root + "/" + file.Name())
      if errRem != nil {
        glog.Errorf("could not remove file %s :: %v", file.Name(), errRem)
      }
      continue
    }
    if !isData && !isTmp && lastCPSeqID < seq {
      lastCPSeqID = seq
    }
    fileMap[seq] = file.Name()
  }
  // Delete all stale WAL files
  for id, name := range fileMap {
    // The file is older than last checkpoint or there is no valid checkpoint.
    if id < lastCPSeqID {
      errRem := os.Remove(w.root + "/" + name)
      if errRem != nil {
        glog.Errorf("could not remove file %s :: %v", name, errRem)
      }
      // delete the element since we deleted the file. This is safe in golang.
      delete(fileMap, id)
    }
  }
  // Now that we have cleaned up the directory. Let us check what are the min
  // and max sequence IDs in the directory.
  if len(fileMap) > 0 {
    seqIDs := make(util.Uint64Slice, len(fileMap))
    ii := 0
    for key := range fileMap {
      seqIDs[ii] = key
      ii++
    }
    sort.Sort(seqIDs)
    w.mu.Lock()
    w.firstSeqID = seqIDs[0]
    w.lastSeqID = seqIDs[len(seqIDs)-1]
    w.writeSeqID = w.lastSeqID + 1
    w.mu.Unlock()
  } else {
    w.mu.Lock()
    w.firstSeqID = 0
    w.lastSeqID = 0
    w.writeSeqID = 0
    w.mu.Unlock()
  }
  glog.Infof("finished parsing WAL root. lastCPSeqID=%d, firstID=%d, lastID=%d",
    lastCPSeqID, w.firstSeqID, w.lastSeqID)
  return nil
}

// Write does the actual disk IO for an op. It serializes the storeFSRecord
// into a buffer for writing into the file.
func (w *StoreFS) Write(op *walStoreOp) error {
  record := storeFSRecord{Magic: cValidMagic, LSN: op.lsn,
    PrevLSN: op.prevLSN}
  record.DataSize = uint32(len(op.data))
  record.Data = op.data
  if op.skipChecksum {
    record.Checksum = 0
  } else {
    record.Checksum = adler32.Checksum(op.data)
  }

  // TODO: Should we just create another []byte and copy the fields into
  // it rather than using encoding/binary?
  buf, errRW := w.writeRecord(&record)
  if errRW != nil || buf == nil {
    glog.Errorf("could not write record to buffer :: %v", errRW)
    return fmt.Errorf("could not write record to buffer :: %v", errRW)
  }
  // The bytes written = header + record.Data + cInvalidMagic
  totalSize := cStoreFSRecordInfoSize + int(record.DataSize) + 4
  wrFileInfo := w.getNextWriteFile(op.dataType)
  if wrFileInfo == nil {
    glog.Error("could not get file to write")
    return fmt.Errorf("could not get file to write")
  }
  count, errWr := wrFileInfo.file.WriteAt(buf, wrFileInfo.offset)
  if errWr != nil || count != totalSize {
    glog.Errorf("could not write to file : count %d :: %v", count, errWr)
    return fmt.Errorf("could not write to file :: %v", errWr)
  }
  // TODO: Call this Sync when writes since last call are greater than
  // a specified size instead of always.
  errS := wrFileInfo.file.Sync()
  if errS != nil {
    glog.Errorf("could not sync file :: %v", errS)
    return fmt.Errorf("could not sync file :: %v", errS)
  }
  glog.V(1).Infof("wrote %d bytes to %s file for lsn %d", totalSize,
    wrFileInfo.name, op.lsn)

  // we will increment offset without the deadbeef part
  wrFileInfo.offset += int64(cStoreFSRecordInfoSize) + int64(record.DataSize)
  return nil
}

// ReadNext is used for recovery so it is a synchronous call waiting for next
// record to be read from Store and returned.
func (w *StoreFS) ReadNext() ([]byte, EntryType, uint64, error) {
  // currRecFile should already be setup in StartRecovery
  if w.currRecFile == nil {
    glog.V(1).Infof("no more files to read")
    return nil, CTypeDone, 0, nil
  }
  var record *storeFSRecord
  hdr := make([]byte, cStoreFSRecordInfoSize)
  var data []byte
  for {
    glog.V(1).Infof("reading file %s", w.currRecFile.name)
    // read header first
    count1, errR1 := w.currRecFile.file.ReadAt(hdr, w.currRecFile.offset)
    w.currRecFile.offset += int64(cStoreFSRecordInfoSize)
    if errR1 == io.EOF {
      w.readSeqID++
      w.currRecFile = w.getNextReadFile()
      if w.currRecFile == nil {
        glog.Infof("did not find any more files to read")
        data = nil
        return nil, CTypeDone, 0, nil
      }
      continue
    }
    if errR1 != nil || count1 < cStoreFSRecordInfoSize {
      glog.Errorf("error reading WAL hdr %s :: %v", w.currRecFile.name,
        errR1)
      return nil, CTypeError, 0, fmt.Errorf("error reading file :: %v",
        errR1)
    }
    // parse the hdr into the struct
    var errP error
    record, errP = w.readRecord(hdr)
    if errP != nil {
      glog.Errorf("error parsing file data for storeFSRecord :: %v", errP)
      return nil, CTypeError, 0, fmt.Errorf("error parsing data :: %v",
        errP)
    }
    // now read the data
    data = make([]byte, record.DataSize)
    count2, errR2 := w.currRecFile.file.ReadAt(data, w.currRecFile.offset)
    w.currRecFile.offset += int64(record.DataSize)
    if errR2 != nil || count2 < int(record.DataSize) ||
      len(data) < int(record.DataSize) {
      // we should not get an error but we will try to recover from next file
      // onwards by trying to get next file ready before we return error.
      w.readSeqID++
      w.currRecFile = w.getNextReadFile()
      glog.Errorf("error in reading WAL data :: %v", errR2)
      return nil, CTypeError, 0, fmt.Errorf("error parsing record :: %v",
        errR2)
    }
    // verify checksum if non-zero
    if record.Checksum != 0 {
      expChecksum := adler32.Checksum(data)
      if expChecksum != record.Checksum {
        glog.Errorf("checksum from file %v did not match expected %v",
          record.Checksum, expChecksum)
        return nil, CTypeError, 0, fmt.Errorf("checksum error")
      }
    }
    break
  }

  glog.V(1).Infof("read record of type %d of data size %d with LSN %d",
    w.currRecFile.fileType, record.DataSize, record.LSN)

  return data, w.currRecFile.fileType, record.LSN, nil
}

// StartCheckpoint will create a new active(tmp) checkpoint file and return.
func (w *StoreFS) StartCheckpoint() error {
  return nil
}

// FinalizeCheckpoint will close the checkpoint file and move the tmp file to a
// permanent checkpoint file and return.
func (w *StoreFS) FinalizeCheckpoint() error {

  if w.currCPFile == nil {
    glog.Error("FinalizeCheckpoint without active checkpoint file")
    return fmt.Errorf("FinalizeCheckpoint without active checkpoint file")
  }
  errC := w.currCPFile.file.Close()
  if errC != nil {
    glog.Errorf("error closing checkpoint tmp file :: %v", errC)
    return fmt.Errorf("error closing checkpoint file")
  }
  finalName := strings.TrimSuffix(w.currCPFile.name, "_tmp")

  errR := os.Rename(w.root+"/"+w.currCPFile.name, w.root+"/"+finalName)
  if errR != nil {
    glog.Errorf("error closing checkpoint tmp file :: %v", errR)
    return fmt.Errorf("error closing checkpoint file :: %v", errR)
  }
  glog.V(1).Infof("Finalizing checkpoint to:%s",
    fmt.Sprintf("%s/%s", w.root, finalName))
  w.currCPFile = nil
  return nil
}

// StartRecovery will reset the read pointer to the last finished checkpoint.
// It is the caller's responsibility to make sure StartRecovery is the first
// routine called after NewStoreFS() before sending any entries to WAL.
func (w *StoreFS) StartRecovery() error {
  w.mu.Lock()
  w.inRecovery = true
  w.readSeqID = w.firstSeqID
  w.mu.Unlock()

  w.currRecFile = w.getNextReadFile()
  return nil
}

// FinishRecovery will clear the recovery state of the WALStore.
func (w *StoreFS) FinishRecovery() error {
  w.mu.Lock()
  defer w.mu.Unlock()
  w.inRecovery = false
  return nil
}

// Flush flushes all open data and checkpoint files.
// TODO: Any os calls to flush os buffers?
func (w *StoreFS) Flush() error {
  var firstError error
  if w.currDataFile != nil {
    err := w.currDataFile.file.Sync()
    if err != nil {
      firstError = err
      glog.Errorf("error flushing file %s :: %v", w.currDataFile.name, err)
    }
  }
  if w.currCPFile != nil {
    err := w.currCPFile.file.Sync()
    if err != nil {
      firstError = err
      glog.Errorf("error flushing file %s :: %v", w.currCPFile.name, err)
    }
  }
  return firstError
}

// Remove deletes the root directory and recreates it effectively wiping out
// all WAL entries.
func (w *StoreFS) Remove() error {
  errR := os.RemoveAll(w.root)
  if errR != nil {
    glog.Errorf("could not remove WAL root :: %v", errR)
    return errR
  }
  return w.initRoot()
}

////////////////////////////////////////////////////////////////////////////////
// Internal functions

// initRoot checks for the presence of the root directory and creates it if it
// is missing.
func (w *StoreFS) initRoot() error {
  rootInfo, errS := os.Stat(w.root)
  if errS == nil && rootInfo.IsDir() {
    return nil
  }
  if errS == nil && !rootInfo.IsDir() {
    glog.Errorf("found a Store root which is a file instead of dir")
    return fmt.Errorf("found a Store root which is a file instead of dir")
  }
  glog.Infof("did not find the WAL root : %s :: %v", w.root, errS)
  if os.IsNotExist(errS) {
    errM := os.MkdirAll(w.root, os.FileMode(0777))
    if errM != nil {
      glog.Errorf("cannot create Store root: %s :: %v", w.root, errM)
      return fmt.Errorf("cannot create Store root")
    }
    glog.Infof("created WAL root : %s", w.root)
    return nil
  }
  glog.Errorf("cannot find or create Store root: %s :: %v", w.root, errS)
  return fmt.Errorf("cannot find or create Store root")
}

// Filename utilities

// getDataFilename returns a data file name with the given seq number.
func (w *StoreFS) getDataFilename(seq uint64) string {
  return fmt.Sprintf("%d_data", seq)
}

// getCheckpointFilename returns a checkpoint filename with the seq number.
func (w *StoreFS) getCheckpointFilename(seq uint64, tmp bool) string {
  if tmp {
    return fmt.Sprintf("%d_cp_tmp", seq)
  }
  return fmt.Sprintf("%d_cp", seq)
}

// parseFilename breaks the given filename into parts and checks if it follows
// one of the WAL naming schemes and is a valid WAL name.
func (w *StoreFS) parseFilename(fileName string) (
  uint64, bool, bool, error) {

  var seq uint64
  var isData, isTmp bool

  parts := strings.Split(fileName, "_")

  if len(parts) != 2 && len(parts) != 3 {
    glog.Errorf("found invalid WAL file %s", fileName)
    return 0, false, false, fmt.Errorf("invalid WAL file %s", fileName)
  }

  seq, err := strconv.ParseUint(parts[0], 10, 64)
  if err != nil {
    glog.Errorf("found invalid seq id in WAL file %s", fileName)
    return 0, false, false, fmt.Errorf("invalid WAL file %s", fileName)
  }

  if parts[1] == "data" {
    isData = true
  } else if parts[1] == "cp" {
    isData = false
  } else {
    glog.Errorf("found invalid type in WAL file %s", fileName)
    return 0, false, false, fmt.Errorf("invalid WAL file %s", fileName)
  }

  if len(parts) == 3 && parts[1] == "cp" && parts[2] == "tmp" {
    isTmp = true
  } else {
    isTmp = false
  }

  return seq, isData, isTmp, nil
}

// getNextReadFile returns a walFileInfo based on the current readSeqID. This
// function is used during recovery to read files one by one.
func (w *StoreFS) getNextReadFile() *walFileInfo {
  for w.readSeqID <= w.lastSeqID {
    // check if a checkpoint file exists with the current readSeqID
    fileName := w.getCheckpointFilename(w.readSeqID, false)

    file, err := os.OpenFile(w.root+"/"+fileName, os.O_RDONLY,
      os.FileMode(0777))

    if err == nil {
      w.currRecFile = &walFileInfo{name: fileName, file: file, offset: 0,
        fileType: CTypeCheckpoint}
      return w.currRecFile
    }

    fileName = w.getDataFilename(w.readSeqID)

    file, err = os.OpenFile(w.root+"/"+fileName, os.O_RDONLY, os.FileMode(0777))

    if err == nil {
      w.currRecFile = &walFileInfo{name: fileName, file: file, offset: 0,
        fileType: CTypeData}
      return w.currRecFile
    }
    w.readSeqID++
  }
  return nil
}

// getNextWriteFile returns an walFileInfo based on the Type. It will create a
// new file with next sequence number if no file is open right now. If the new
// request is for a checkpoint, it will close the active data file. We do not
// need to close active checkpoint file when the request is for data file since
// close FinalizeCheckpoint would have done it anyway.
func (w *StoreFS) getNextWriteFile(eType EntryType) *walFileInfo {
  var fileName string
  switch eType {
  case CTypeData:
    if w.currDataFile != nil {
      if w.currDataFile.offset < *gMaxDataFileSize {
        return w.currDataFile
      }
      w.currDataFile.file.Sync()
      w.currDataFile.file.Close()
      w.currDataFile = nil
    }
    w.mu.Lock()
    w.writeSeqID++
    seq := w.writeSeqID
    w.mu.Unlock()
    fileName = w.getDataFilename(seq)
  case CTypeCheckpoint:
    // TODO: No size limit on the Checkpoint file? If we set limit
    // then checkpoint spans multiple tmp files.
    if w.currCPFile != nil {
      return w.currCPFile
    }
    // Close any data files that are active. We should not have both files
    // open at the same time so it is ok to close after the previous block.
    if w.currDataFile != nil {
      w.currDataFile.file.Sync()
      w.currDataFile.file.Close()
      w.currDataFile = nil
    }
    w.mu.Lock()
    w.writeSeqID++
    seq := w.writeSeqID
    w.mu.Unlock()
    fileName = w.getCheckpointFilename(seq, true)
  default:
    glog.Errorf("unexpected WAL entry type: %v", eType)
    return nil
  }

  file, err := os.OpenFile(w.root+"/"+fileName,
    os.O_RDWR|os.O_TRUNC|os.O_CREATE, os.FileMode(0777))

  if err != nil {
    glog.Errorf("could not open WAL file: %s :: %v", fileName, err)
    return nil
  }

  glog.V(1).Infof("opened a new WAL file %s", fileName)

  if eType == CTypeData {
    w.currDataFile = &walFileInfo{name: fileName, file: file, offset: 0}
    return w.currDataFile
  }
  w.currCPFile = &walFileInfo{name: fileName, file: file, offset: 0}
  return w.currCPFile
}

// readRecord reads individual fields out of the header bytes read from the
// file.
func (w *StoreFS) readRecord(hdr []byte) (*storeFSRecord, error) {
  record := &storeFSRecord{}
  buf := bytes.NewBuffer(hdr)
  err := binary.Read(buf, binary.LittleEndian, &record.Magic)
  if err != nil {
    glog.Errorf("could not parse WAL data for Magic :: %v", err)
    return nil, fmt.Errorf("error parsing WAL data Magic :: %v", err)
  }
  if record.Magic != cValidMagic {
    glog.Errorf("invalid Magic %X", record.Magic)
    return nil, fmt.Errorf("invalid Magic :: %v", err)
  }
  err = binary.Read(buf, binary.LittleEndian, &record.LSN)
  if err != nil {
    glog.Errorf("could not parse WAL data for LSN :: %v", err)
    return nil, fmt.Errorf("error parsing WAL data LSN :: %v", err)
  }
  err = binary.Read(buf, binary.LittleEndian, &record.PrevLSN)
  if err != nil {
    glog.Errorf("could not parse WAL data for PrevLSN :: %v", err)
    return nil, fmt.Errorf("error parsing WAL data PrevLSN :: %v", err)
  }
  err = binary.Read(buf, binary.LittleEndian, &record.DataSize)
  if err != nil {
    glog.Errorf("could not parse WAL data for DataSize :: %v", err)
    return nil, fmt.Errorf("error parsing WAL data DataSize :: %v", err)
  }
  err = binary.Read(buf, binary.LittleEndian, &record.Checksum)
  if err != nil {
    glog.Errorf("could not parse WAL data for Checksum :: %v", err)
    return nil, fmt.Errorf("error parsing WAL data Checksum :: %v", err)
  }
  return record, nil
}

// writeRecord writes the storeFSRecord to a buffer and returns the []byte
// that can be written to file.
// TODO: We could do byte copy with endianness to make it faster?
func (w *StoreFS) writeRecord(record *storeFSRecord) ([]byte, error) {
  buf := new(bytes.Buffer)
  err := binary.Write(buf, binary.LittleEndian, record.Magic)
  if err != nil {
    glog.Errorf("error encoding Magic :: %v", err)
    return nil, fmt.Errorf("error encoding Magic :: %v", err)
  }
  err = binary.Write(buf, binary.LittleEndian, record.LSN)
  if err != nil {
    glog.Errorf("error encoding LSN :: %v", err)
    return nil, fmt.Errorf("error encoding LSN :: %v", err)
  }
  err = binary.Write(buf, binary.LittleEndian, record.PrevLSN)
  if err != nil {
    glog.Errorf("error encoding PrevLSN :: %v", err)
    return nil, fmt.Errorf("error encoding PrevLSN :: %v", err)
  }
  err = binary.Write(buf, binary.LittleEndian, record.DataSize)
  if err != nil {
    glog.Errorf("error encoding DataSize :: %v", err)
    return nil, fmt.Errorf("error encoding DataSize :: %v", err)
  }
  err = binary.Write(buf, binary.LittleEndian, record.Checksum)
  if err != nil {
    glog.Errorf("error encoding Checksum :: %v", err)
    return nil, fmt.Errorf("error encoding Checksum :: %v", err)
  }
  err = binary.Write(buf, binary.LittleEndian, record.Data)
  if err != nil {
    glog.Errorf("error encoding Data :: %v", err)
    return nil, fmt.Errorf("error encoding Data :: %v", err)
  }
  invalidMagic := uint32(cInvalidMagic)
  err = binary.Write(buf, binary.LittleEndian, invalidMagic)
  if err != nil {
    glog.Errorf("error encoding invalidMagic :: %v", err)
    return nil, fmt.Errorf("error encoding invalidMagic :: %v", err)
  }

  return buf.Bytes(), nil
}

// IncNextLSN increments "currLSN" and returns.
func (w *StoreFS) IncNextLSN(currLSN uint64) (uint64, uint64, error) {
  return currLSN, currLSN + 1, nil
}

// NextLSN returns the lsn at which the next entry in the wal should be
// written.
func (w *StoreFS) NextLSN() (uint64, error) {
  // TODO: Look into the wal files to return this value. For now
  // returing "0" is fine as the code calling "EndLSN()" only calls it after
  // reading through the whole wal and has an alternate way of computing
  // this (see wal.go), which works correctly for the "StoreFS" backend.

  return 0, nil
}
