# Name
Migrate - read Tarantool 1.5 snapshots and xlogs inside modern Tarantool.

This module aims to ease migration from Tarantool 1.5 to modern versions of Tarantool.

## Status

Under early development

## Version

This document describes migrate 0.0.1

## Installation
* luarocks >= 2.4.2
* tarantool >= 1.10

```bash
luarocks --tree .rocks --lua-version 5.1 --server http://moonlibs.github.io/rocks \
	install https://raw.githubusercontent.com/orchaton/migrate/master/migrate-scm-1.rockspec
```

## Synopsis

This library provides luafun interface to read binary snapshots and xlogs (yet).

```lua
-- Read single snapshot
require "migrate".pairs "/path/to/00000000001342537118.snap":length()

-- Read directory with snapshots and xlogs
require "migrate".paris "/path/to/snap_and_xlog_dir":length()
require "migrate".pairs { dir = "/path/to/snap_and_xlog_dir" }:length()

-- Read snapshots and xlogs from different directories
require "migrate".pairs { dir = { snap = "/path/to/snaps", xlog = "/path/to/xlogs" } }:length()

-- Read only xlogs (will read all xlogs from LSN=0. May raise if no xlog found)
require "migrate".pairs { dir = { xlog = "/path/to/xlogs" } }:length()

-- Read xlogs from specified LSN (all xlogs gaps will be checked)
require "migrate".pairs { dir = { xlog = "/path/to/xlogs" }, confirmed_lsn = 10 }:length()

-- Read xlogs from specified LSN (with no LSN Gaps check)
require "migrate".pairs { dir = { xlog = "/path/to/xlogs" }, confirmed_lsn = 10, checklsn = false }:length()
```

## TODO
* Persisting of LSN
* Bootstrap using transactions
* Replication protocol (1.5) support
