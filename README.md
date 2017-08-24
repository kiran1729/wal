# wal

wal package implements a Write Ahead Log. Please see wikipedia for the uses of a wal (https://en.wikipedia.org/wiki/Write-ahead_logging). 

This package is a basic component which lets you design a redo/undo and other things on top using the package.
There are three parts in the package:
* wal interface with a walstore backend interface
* walstore implementation using a filesystem backend
* walstore implementation using a etcd backend

Using etcd as a key-value store provides a mechanism to construct a distributed wal. However, typical
wals are single writer through their life. Distributed wal has only one writer at a time but multiple
possible writers over time. The distributed wal code has some limitations on how it should be used to
resolve the ownership in case of multiple writers trying to claim ownership. 

