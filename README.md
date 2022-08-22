# Name
Migrate - read Tarantool 1.5 snapshots and xlogs inside modern Tarantool.

This module aims to ease migration from Tarantool 1.5 to modern versions of Tarantool.

## Status
![Linter check](https://github.com/ochaton/migrate/actions/workflows/lint.yml/badge.svg)
![Release status](https://github.com/ochaton/migrate/actions/workflows/push-rockspec.yml/badge.svg)

Tested in production

## Version

This document describes migrate 0.0.1

## Installation
* luarocks >= 2.4.2
* tarantool >= 1.10

```bash
luarocks --tree .rocks --lua-version 5.1 --server http://moonlibs.github.io/rocks \
	install https://raw.githubusercontent.com/ochaton/migrate/master/migrate-scm-1.rockspec

# Using tarantool
tarantoolctl rocks --server=https://moonlibs.github.io/rocks install https://github.com/ochaton/migrate/releases/download/0.1.0/migrate-0.1.0-1.src.rock
```

This library also published at https://moonlibs.github.io/rocks

Starting from Tarantool 2.10.0 you may add url https://moonlibs.github.io/rocks to you `.rocks/config-5.1.lua` and install library like this:

```bash
tarantoolctl rocks install migrate
```

Configuration of rocks servers should be following:
```
$ cat .rocks/config-5.1.lua
rocks_servers = {
        "http://moonlibs.github.io/rocks",        -- moonlibs libs
        "http://rocks.tarantool.org/",            -- tarantool libs
        "http://luarocks.org/repositories/rocks", -- luarocks.org libs
}
```

## Synopsis

This library provides luafun interface to read binary snapshots, xlogs and replication.

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

-- Read xlogs from specified LSN (without LSN Gaps check)
require "migrate".pairs { dir = { xlog = "/path/to/xlogs" }, confirmed_lsn = 10, checklsn = false }:length()

-- Read transactions from replication (without LSN checks)
require "migrate".replica{ host = "tarantool.host.i", port = 3301, confirmed_lsn = 0 }:take(100):each(require'log'.info)

-- Execute full pipeline with bootstraping from snaps, xlogs and replication
require "migrate".pairs {
	dir = {
		xlog = "/path/to/xlogs",
		snap = "/path/to/snaps",
	},
	-- persist creates space and persists lsn of each transactions
	-- in space box.space._migration
	-- enabling persist forces each tuple to be processed in separate transaction
	persist = true, -- (default false)
	-- txn allows to configure how many xlogs will share same transaction
	-- txn takes effect only with persist
	txn = 1, -- (default 1)
	replication = {
		host = "legacy-master.addr.i",
		port = 3301, -- primary port,
		timeout = 1.5,
		reconnect = 1/3, -- default in seconds
		debug = false,
	},
}
-- Iterator will gracefully finished when all snaps and xlogs were processed.
-- If replication is enabled iterator will finish when it confirms read_only master's lsn.
```

## Example
Naive example

```lua
-- default log_level=5 (info), log_level=6 (verbose) gives you more information about migration process
box.cfg{log_level = 6}

require "migrate".pairs {
	dir = "/path/to/snaps_and_xlogs",
	persist = true,
	txn = 1000,
	debug = false, -- enable this if you want more information about migration process
	replication = {
		host = "legacy-master.addr.i",
		port = 3301, -- primary port, replica port will be discovered automatically
		timeout = 3, -- write timeout in seconds
		reconnect = 1/3, -- default in seconds
		debug = false, -- enable this if you want more verbose information about replication
	}
}:each(function(t)
	local h = t.HEADER
	local space_no = t.BODY.space_id
	local tuple = t.BODY.tuple
	local key = t.BODY.key
	local op = t.HEADER.type -- UPDATE, INSERT, DELETE or REPLACE

	local tuple = migrate_tuple(space_no, tuple)
	if key then
		key = migrate_primary(space_no, key)
	end

	local space = box.space[512+space_no]
	if op == 'INSERT' then
		space:insert(tuple)
	elseif op == 'DELETE' then
		space:delete(key)
	elseif op == 'UPDATE' then
		space:update(key, tuple)
	elseif op == 'REPLACE' then
		space:replace(tuple)
	end
end)
```

## TODO
* Tests and doc
