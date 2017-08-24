# wal

WAL implements a write ahead log for supporting journaling. Users of a WAL
can write entries into the WAL that are persisted and can be read back
after a crash and restart.
Please see wikipedia for the uses of a wal (https://en.wikipedia.org/wiki/Write-ahead_logging). 

This package is a basic component which lets you design a redo/undo and other things on top using the package.
There are three parts in the package:
* wal interface with a walstore backend interface
* walstore implementation using a filesystem backend
* walstore implementation using a etcd backend

The caller can add two types of entries - Data entries and Checkpoint
entries. The contents of the entries are byte slices that the WAL does not
parse or understand. It is the caller's responsibility to store appropriate
redo or undo information in the entries to reconstruct the state after a
crash and recovery.

Checkpointing is supported by adding checkpoint entries between a
StartCheckpoint/FinalizeCheckpoint pair of calls. Any checkpoint that
is started but not finalized is discarded. Checkpointing reduces the
number of entries sent to the caller on recovery. When recovery is
initiated, all entries starting from the last completed checkpoint are
sent to the caller. On a recovery, the WAL directory is cleaned up to
delete all old entries before the last finalized checkpoint.

WAL can use different backends for its persistent storage. All backends
need to implement the WALStore interface below. Two examples are a WALStore
that uses local filesystem as persistent store and another that uses
etcd as its persistent store.

Using etcd as a key-value store provides a mechanism to construct a distributed wal. However, typical
wals are single writer through their life. Distributed wal has only one writer at a time but multiple
possible writers over time. The distributed wal code has some limitations on how it should be used to
resolve the ownership in case of multiple writers trying to claim ownership. 

Writing to the WAL is done asynchronously. All pending writes are added
to an "opList" from which a controller go-routine drains ops and writes
them to the backend. Once writes are committed to the backend, the
controller issues completions via a channel to the writer.
