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

package wal

import (
  crand "crypto/rand"
  "fmt"
  "math/rand"
  "testing"
  "time"

  "code.google.com/p/go-uuid/uuid"
  "github.com/golang/glog"
  "github.com/stretchr/testify/assert"

  "zerostack/common/etcdmgr"
  "zerostack/common/sysutil"
  "zerostack/common/util"
)

// doRecovery runs the recovery loop on the given WAL and returns the last LSN
// that was returned in the recovery.
// The function returns the last data LSN and last CP lsn received during the
// recovery.
func doRecovery(t *testing.T, wal *WAL, lastDataLSN, lastCPLSN *uint64) {
  recChan := make(chan *Entry)
  quitChan := make(chan struct{})
  defer close(quitChan)
  go func() {
    errSt := wal.StartRecovery(recChan, quitChan)
    assert.Nil(t, errSt)
  }()
  for {
    entry := <-recChan
    if entry.Type == CTypeDone {
      glog.Info("finished recovery")
      break
    }
    if entry.Type == CTypeCheckpoint {
      *lastCPLSN = entry.LSN
    } else if entry.Type == CTypeData {
      *lastDataLSN = entry.LSN
    }
    glog.V(2).Infof("processed entry with LSN=%d and type=%v", entry.LSN,
      entry.Type)
    assert.NotNil(t, entry.Data)
  }
}

// doWALAdditionsAndCheckpoint will add some data and checkpoint records in a
// random fashion
func doWALAdditionsAndCheckpoint(t *testing.T, wal *WAL) (uint64, uint64) {
  var lastDataLSN, currCPLSN, lastCPLSN uint64

  // delete the wal with 50% probability.
  if rand.Intn(20) < 10 {
    glog.Infof("removing existing WAL")
    wal.Remove()
  }

  glog.Info("inserting entries into WAL")

  doneCh := make(chan *Response)

  totalRsp := 0

  lsnList := make(map[uint64]struct{})
  var outstandingEnts int
  for ii := 0; ii < 2; ii++ {
    // insert random number of WAL Data entries
    outstandingEnts = 0
    entryCnt := 30 + rand.Intn(15)
    for jj := 0; jj < entryCnt; jj++ {
      size := 500 + rand.Intn(100)
      data := make([]byte, size)
      _, errRd := crand.Read(data)
      assert.Nil(t, errRd)

      id, errAdd := wal.AddDataEntry(data, false, doneCh)
      assert.NoError(t, errAdd)
      lsnList[id] = struct{}{}
      lastDataLSN = id
    }

    totalRsp += entryCnt
    outstandingEnts += entryCnt
    cpDone := 0
    glog.Infof("added %d data entries", entryCnt)

    startRecovery := false

    // Now checkpoint and add random checkpoint entries
    cpCnt := 10 + rand.Intn(5)
    errSC := wal.StartCheckpoint()
    assert.Nil(t, errSC)

    for kk := 0; kk < cpCnt; kk++ {
      size := 500 + rand.Intn(100)
      data := make([]byte, size)
      _, errRd := crand.Read(data)
      assert.Nil(t, errRd)
      var errAdd error
      currCPLSN, errAdd = wal.AddCheckpointEntry(data, false, doneCh)
      assert.NoError(t, errAdd)
      lsnList[currCPLSN] = struct{}{}

      cpDone++

      // Randomly break from here with 5% probability and start recovery
      if rand.Intn(100) < 5 {
        startRecovery = true
        break
      }
    }
    totalRsp += cpDone
    outstandingEnts += cpDone
    glog.Infof("added %d checkpoint entries, total entries=%d", cpDone,
      totalRsp)
    if startRecovery {
      break
    }
    errFC := wal.FinalizeCheckpoint()
    assert.Nil(t, errFC)
    // TODO: Occasionally without the sleep, the final checkpoint
    // record is missing when the wal namespace is read during recovery.
    time.Sleep(1 * time.Second)
    // only when we finalize a checkpoint we update the last LSNs
    lastCPLSN = currCPLSN
    // set outstanding to 0, since FinalizeCheckpoint flushes the pending IOs.
    outstandingEnts = 0
  }

  // Now wait for the acks of the WAL operations.
  // Timeout if all the acks are not received within "waitDur".
  // This method is common to both the filesystem and etcd backed WALs.
  // The timeout computation below is tuned for the etcd backed WALs,
  // as they take a longer time to service IOs.
  waitDur := time.Duration(util.MaxInt(20, outstandingEnts)) * time.Second
  timeout := time.After(waitDur)
  glog.Infof("waiting %v for total=%d, outstanding=%d responses",
    waitDur, totalRsp, outstandingEnts)

  startTime := time.Now()
  // Now wait for all responses on the channel.
  for ii := 0; ii < totalRsp; ii++ {
    glog.V(2).Infof("waiting for %dth entry", ii)
    select {
    case response := <-doneCh:
      glog.V(2).Infof("Received %dth entry with lsn:%d", ii, response.LSN)
      assert.Nil(t, response.Result)
      _, found := lsnList[response.LSN]
      assert.True(t, found)
      delete(lsnList, response.LSN)
    case <-timeout:
      glog.Errorf("Timed out waiting for %dth entry", ii)
      assert.True(t, false)
    }
  }
  glog.Infof("Took %.3f seconds to get the %d outstanding responses",
    time.Since(startTime).Seconds(), outstandingEnts)

  // make sure all ops are completed.
  assert.Equal(t, len(lsnList), 0)
  glog.V(2).Infof("lastDataLSN=%d, lastCPLSN=%d", lastDataLSN, lastCPLSN)
  return lastDataLSN, lastCPLSN
}

// doWALAdditions will add some data records in a random fashion
func doWALAdditions(t *testing.T, wal *WAL) (uint64, uint64) {
  var lastDataLSN, lastCPLSN uint64

  glog.Info("inserting entries into the WAL")

  doneCh := make(chan *Response)

  totalRsp := 0

  lsnList := make(map[uint64]struct{})
  var outstandingEnts int
  for ii := 0; ii < 2; ii++ {
    // insert random number of WAL Data entries
    entryCnt := 30 + rand.Intn(15)
    for jj := 0; jj < entryCnt; jj++ {
      size := 500 + rand.Intn(100)
      data := make([]byte, size)
      _, errRd := crand.Read(data)
      assert.Nil(t, errRd)
      id, errAdd := wal.AddDataEntry(data, false, doneCh)
      assert.NoError(t, errAdd)
      lsnList[id] = struct{}{}
      lastDataLSN = id
    }

    totalRsp += entryCnt
    outstandingEnts += entryCnt
    glog.Infof("added %d data entries", entryCnt)
  }

  // Now wait for the acks of the WAL operations.
  // Timeout if all the acks are not received within "waitDur".
  // This method is common to both the filesystem and etcd backed WALs.
  // The timeout computation below is tuned for the etcd backed WALs,
  // as they take a longer time to service IOs.
  waitDur := time.Duration(util.MaxInt(20, outstandingEnts)) * time.Second
  timeout := time.After(waitDur)
  glog.Infof("waiting %v for total=%d, outstanding=%d responses",
    waitDur, totalRsp, outstandingEnts)

  startTime := time.Now()
  // Now wait for all responses on the channel.
  for ii := 0; ii < totalRsp; ii++ {
    glog.V(2).Infof("waiting for %dth entry", ii)
    select {
    case response := <-doneCh:
      glog.V(2).Infof("Received %dth entry with lsn:%d", ii, response.LSN)
      assert.Nil(t, response.Result)
      _, found := lsnList[response.LSN]
      assert.True(t, found)
      delete(lsnList, response.LSN)
    case <-timeout:
      glog.Errorf("Timed out waiting for %dth entry", ii)
      assert.True(t, false)
    }
  }
  glog.Infof("Took %.3f seconds to get the %d outstanding responses",
    time.Since(startTime).Seconds(), outstandingEnts)

  // make sure all ops are completed.
  assert.Equal(t, len(lsnList), 0)
  glog.V(2).Infof("lastDataLSN=%d, lastCPLSN=%d", lastDataLSN, lastCPLSN)
  return lastDataLSN, lastCPLSN
}

func doWALRecovery(t *testing.T, wal *WAL, lastDataLSN, lastCPLSN uint64) {
  // call the recovery routine
  var recDataLSN, recCPLSN uint64
  doRecovery(t, wal, &recDataLSN, &recCPLSN)
  assert.True(t, recCPLSN == lastCPLSN,
    "LSN is not as expected recCPLSN=%d, lastCheckpointLSN=%d",
    recCPLSN, lastCPLSN)
  // If there was no finalized commit after writing data records then
  // recDataLSN will be 0.
  assert.True(t, recDataLSN == 0 || recDataLSN == lastDataLSN,
    "LSN is not as expected recDataLSN=%d, lastDataLSN=%d",
    recDataLSN, lastDataLSN)
}

// TestWALStoreFSRandom does randomized testing a file-system backed WAL.
func TestWALStoreFSRandom(t *testing.T) {

  glog.Info("\n\nStarting TestWALStoreFSRandom\n\n")

  rand.Seed(time.Now().UnixNano())
  walFSName := fmt.Sprintf("/tmp/zerostack_wal_%d", rand.Uint32())
  walStore, errFS := NewStoreFS(walFSName)
  assert.Nil(t, errFS)
  assert.NotNil(t, walStore)

  wal, errWAL := NewWAL(walStore)
  assert.Nil(t, errWAL)
  assert.NotNil(t, wal)

  lastDataLSN, lastCPLSN := doWALAdditionsAndCheckpoint(t, wal)

  // fake crash and recovery by just creating another WAL object
  walStore2, errFS2 := NewStoreFS(walFSName)
  assert.Nil(t, errFS2)
  assert.NotNil(t, walStore2)

  wal2, errWAL := NewWAL(walStore2)
  assert.Nil(t, errWAL)
  assert.NotNil(t, wal2)
  doWALRecovery(t, wal2, lastDataLSN, lastCPLSN)
}

// TestWALStoreEtcdRandom does randomized testing of an etcd backed WAL.
func TestWALStoreEtcdRandom(t *testing.T) {
  // clean up etcd processes before and after the test
  etcdmgr.Cleanup()
  defer etcdmgr.Cleanup()

  glog.Info("\n\nStarting TestWALStoreEtcdRandom\n\n")

  rand.Seed(time.Now().UnixNano())

  freePorts, _ := sysutil.FreePortsInRange(54000, 54050, 4)
  assert.Equal(t, len(freePorts), 4)

  etcdURL := make([]string, 2)
  etcdPeerURL := make([]string, 2)

  for ii := 0; ii < 2; ii++ {
    etcdURL[ii] = fmt.Sprintf("http://127.0.0.1:%d", freePorts[ii*2+0])
    etcdPeerURL[ii] = fmt.Sprintf("http://127.0.0.1:%d", freePorts[ii*2+1])
  }

  etcds, err := etcdmgr.NewEtcdMgrs(etcdURL, etcdPeerURL)
  assert.Nil(t, err)

  for ii := 0; ii < 2; ii++ {
    err := etcds[ii].Start(t)
    assert.Nil(t, err)
  }

  for ii := 0; ii < 2; ii++ {
    err := etcdmgr.WaitForStartup(etcds[ii].ClientAddr(),
      10*time.Second,
      100*time.Millisecond)
    assert.Nil(t, err)
  }

  walStoreEtcdName := fmt.Sprintf("zerostack_wal_%d", rand.Uint32())
  walStore, errN := NewStoreEtcd(uuid.New(), etcdURL[0], walStoreEtcdName)

  assert.Nil(t, errN)
  assert.NotNil(t, walStore)

  wal, errWAL := NewWAL(walStore)
  assert.Nil(t, errWAL)
  assert.NotNil(t, wal)

  lastDataLSN, lastCPLSN := doWALAdditionsAndCheckpoint(t, wal)

  wal.Close()

  // fake crash and recovery by just creating another WAL object
  // We connect to the second etcd instance to test remote recovery.
  walStore2, errFS2 := NewStoreEtcd(uuid.New(), etcdURL[1], walStoreEtcdName)
  assert.Nil(t, errFS2)
  assert.NotNil(t, walStore2)

  wal2, errWAL := NewWAL(walStore2)
  assert.Nil(t, errWAL)
  assert.NotNil(t, wal2)

  doWALRecovery(t, wal2, lastDataLSN, lastCPLSN)
}

// TestWALStoreEtcdRandomNoCP does randomized testing of an etcd backed WAL,
// absent checkpoints.
func TestWALStoreEtcdRandomNoCP(t *testing.T) {
  // clean up etcd processes before and after the test
  etcdmgr.Cleanup()
  defer etcdmgr.Cleanup()

  glog.Info("\n\nStarting TestWALStoreEtcdRandomNoCP\n\n")

  rand.Seed(time.Now().UnixNano())

  freePorts, _ := sysutil.FreePortsInRange(54000, 54050, 4)
  assert.Equal(t, len(freePorts), 4)

  etcdURL := make([]string, 2)
  etcdPeerURL := make([]string, 2)

  for ii := 0; ii < 2; ii++ {
    etcdURL[ii] = fmt.Sprintf("http://127.0.0.1:%d", freePorts[ii*2+0])
    etcdPeerURL[ii] = fmt.Sprintf("http://127.0.0.1:%d", freePorts[ii*2+1])
  }

  etcds, err := etcdmgr.NewEtcdMgrs(etcdURL, etcdPeerURL)
  assert.Nil(t, err)

  for ii := 0; ii < 2; ii++ {
    err := etcds[ii].Start(t)
    assert.Nil(t, err)
  }

  for ii := 0; ii < 2; ii++ {
    err := etcdmgr.WaitForStartup(etcds[ii].ClientAddr(),
      10*time.Second,
      100*time.Millisecond)
    assert.Nil(t, err)
  }

  walStoreEtcdName := fmt.Sprintf("zerostack_wal_%d", rand.Uint32())
  walStore, errN := NewStoreEtcd(uuid.New(), etcdURL[0], walStoreEtcdName)

  assert.Nil(t, errN)
  assert.NotNil(t, walStore)

  wal, errWAL := NewWAL(walStore)
  assert.Nil(t, errWAL)
  assert.NotNil(t, wal)

  lastDataLSN, lastCPLSN := doWALAdditions(t, wal)

  wal.Close()

  // fake crash and recovery by just creating another WAL object
  // We connect to the second etcd instance to test remote recovery.
  walStore2, errFS2 := NewStoreEtcd(uuid.New(), etcdURL[1], walStoreEtcdName)
  assert.Nil(t, errFS2)
  assert.NotNil(t, walStore2)

  wal2, errWAL := NewWAL(walStore2)
  assert.Nil(t, errWAL)
  assert.NotNil(t, wal2)

  doWALRecovery(t, wal2, lastDataLSN, lastCPLSN)
}
