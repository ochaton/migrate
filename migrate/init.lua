local ffi = require 'ffi.reloadable'
local errno = require 'errno'
local fio = require 'fio'
local fun = require 'fun'
local fiber = require 'fiber'
local clock = require 'clock'
local log = require 'log'
local saferbuf = require 'bin.saferbuf'

local parse_xlog_row = require 'migrate.parser'.parse_xlog_row
local parse_snap_row = require 'migrate.parser'.parse_snap_row

ffi.typedef('off64_t', [[typedef int64_t off64_t;]])

ffi.fundef('mmap', [[void *mmap(void *addr, size_t length, int prot, int flags,int fd, off64_t offset);]])
ffi.fundef('munmap', [[int munmap(void *addr, size_t length);]])
ffi.fundef('madvise', [[int madvise(void *addr, size_t length, int advice);]])

local ffi_cast = ffi.cast
local C = ffi.C

local PROT_READ  = 1
local MAP_SHARED = 1
local MADV_SEQUENTIAL  = 2

local magic = 0xba0babed
local hsize = ffi.sizeof "struct header_v11"
local M = {}

local function file_iterator(self)
	local buf = self.buf
	if not buf then
		return nil
	end
	if buf:avail() <= 4 then
		self:close()
		return nil
	end

	assert(buf:u32() == magic, "malformed row magic")

	assert(buf:have(hsize), "unexpected end of file")
		local h = ffi_cast('struct header_v11 *', buf.p.c)
		assert(h.header_crc32c == C.crc32_calc(0, buf.p.c+4, hsize-4), "header crc32 missmatch")
	buf:skip(hsize)

	assert(buf:have(h.len), "unexpected end of file")
	assert(h.data_crc32c == C.crc32_calc(0, buf.p.c, h.len), "data crc32c missmatch")

	assert(buf:u16() == self.row_type, "unexpected row_type received")
	buf:skip(8) -- drop cookie

	local space, tuple, op, extra = self.parser(buf)
	local key
	if op == 'UPDATE' or op == 'DELETE' then
		key, tuple = tuple, extra
	end
	return h.lsn, {
		HEADER = {
			lsn  = h.lsn,
			type = op,
			timestamp = h.tm,
			source = self.source,
			file = self.path,
		},
		BODY = {
			space_id = space,
			tuple = tuple,
			key = key,
		},
	}
end

local function file_iterator_close(self)
	local orig = getmetatable(self).__index
	debug.setmetatable(self, nil)

	log.verbose("Closing file_iterator %s", orig.path)

	local base = orig.base
	orig.base = nil
	orig.buf = nil

	if base then
		log.verbose("Unmapping base for %s", orig.path)
		if -1 == C.munmap(base, orig.size) then
			error(("Failed to unmap addr for %s: %s"):format(orig.path, errno.strerror(ffi.errno())), 2)
		end
	end
end

function M.file(path)
	local file do
		local err
		file, err = fio.open(path, {"O_RDONLY"})
		if err then
			error(path..": "..tostring(err))
		end
	end

	log.verbose("File %s openned", path)

	local self = { file = file, close = file_iterator_close }
	local header, row_type, parser, source
	if path:match("%.xlog$") then
		header = "XLOG\n0.11\n\n"
		row_type = C.XLOG
		parser = parse_xlog_row
		source = 'xlog'
	elseif path:match("%.snap$") then
		header = "SNAP\n0.11\n\n"
		row_type = C.SNAP
		parser = parse_snap_row
		source = 'snap'
	else
		error(("Unexpected file extenstion: %s"):format(path))
	end

	self.header   = header
	self.row_type = row_type
	self.source   = source
	self.parser   = parser
	self.file     = file
	self.path     = path
	self.size     = tonumber64(file:stat().size)

	local base = C.mmap(box.NULL, self.size, PROT_READ, MAP_SHARED, file.fh, 0)
	if base == -1 then
		error(errno.strerror(ffi.errno()))
	end
	if -1 == C.madvise(base, self.size, MADV_SEQUENTIAL) then
		error(errno.strerror(ffi.errno()))
	end
	self.base = base
	self.buf = saferbuf.new(base, self.size)
	self.close = file_iterator_close

	assert(file:close())
	self.file = nil

	local public = newproxy()
	debug.setmetatable(public, {
		__index = self,
		__gc = file_iterator_close,
	})

	assert(self.buf:str(#self.header) == self.header, "binary header missmatch")
	return fun.iter(file_iterator, public)
end

local function once(func, ...)
	local args = {n = select('#', ...), ...}
	return fun.range(1):map(function() func(unpack(args, 1, args.n)) return end)
end

local function proxy(func)
	return function(...)
		return func(...) or ...
	end
end

local function checklsn(self)
	return function(trans)
		local h = trans.HEADER
		if h.lsn ~= self.confirmed_lsn+1 then
			error(("XlogGapError: Missing transactions between %s and %s while reading %s:%s"):format(
				self.confirmed_lsn, h.lsn, h.source, h.file
			))
		end
		self:confirm(h.lsn)
		return trans
	end
end

local function confirmer(self)
	return function(t)
		self:confirm(t.HEADER.lsn)
		return t
	end
end

local function xlog2lsn(path)
	return (assert(tonumber64(fio.basename(path):sub(1, -#".xlog"-1)), "malformed xlog name"))
end

function M.pairs(opts)
	if type(opts) == 'string' then
		if fio.path.is_dir(opts) then
			local dir = opts
			opts = {
				dir = {
					snap = dir,
					xlog = dir,
				},
				persist = false,
			}
		else
			return M.file(opts)
		end
	end
	if type(opts) ~= 'table' then
		error("Usage: migrate.pairs(opts|path)", 2)
	end

	local self = {
		confirmed_lsn = opts.confirmed_lsn or 0,
		persist = opts.persist,
		txn = opts.txn or 1,
		checklsn = true,
		debug = opts.debug,

		on_replica_connected = opts.on_replica_connected or function() end,

		in_txn = 0,
	}

	if self.confirmed_lsn < 0 then
		error("confirmed_lsn must be >= 0", 2)
	end

	if opts.checklsn == false then
		self.checklsn = false
	end

	if self.persist then
		self.schema = box.space._schema

		local promote_lsn, confirmed_lsn do
			local t
			t = self.schema:get('migrate_promote_lsn')
			promote_lsn = t and t[2] or 0

			t = self.schema:get('migrate_confirmed_lsn')
			confirmed_lsn = t and t[2] or 0
		end

		if confirmed_lsn < promote_lsn then
			error("Cannot start migration. Previous migration was aborted during snapshot recovery. "
				.."You need to truncate all spaces which were affected during previous recovery and "
				.."rollback `migrate_confirmed_lsn` and `migrate_promote_lsn` in `box.space._schema`",
				2)
		end

		self.confirmed_lsn = confirmed_lsn

		function self.commit()
			-- confirm_lsn only for last transaction in batch
			if self.debug then
				log.verbose("_schema:put { migrate_confirmed_lsn, %q }", self.confirmed_lsn)
			end
			self.schema:put({ 'migrate_confirmed_lsn', self.confirmed_lsn })
			box.commit()
			self.in_txn = 0
		end
		function self.begin()
			self.in_txn = 0
			box.begin()
		end

		function self.recovery_snap_row()
			if not self.schema:get('migrate_promote_lsn') then
				log.warn("promote_lsn(%s)", self.snap_lsn)
				self.schema:put({ 'migrate_promote_lsn', self.snap_lsn })
			end
			if self.in_txn == self.txn then
				self.commit()
				self.begin()
				self.in_txn = 0
			end
			self.in_txn = self.in_txn + 1
		end

		function self.confirm(this, next_lsn)
			this.confirmed_lsn = next_lsn
			if this.in_txn == this.txn then
				self.commit()
				self.begin()
				this.in_txn = 0
			end
			this.in_txn = this.in_txn + 1
			if this.debug then
				log.verbose("confirm(%s)", next_lsn)
			end
		end
	else
		self.commit = function() end
		self.begin = function() end
		self.recovery_snap_row = function() end

		function self.confirm(this, next_lsn)
			this.confirmed_lsn = next_lsn
			if this.debug then
				log.verbose("confirm(%s)", next_lsn)
			end
			return this.confirmed_lsn
		end
	end

	local iterator = fun.iter{} -- empty iterator

	if opts.dir then
		if type(opts.dir) == 'string' then
			opts.dir = { snap = opts.dir, xlog = opts.dir }
		elseif type(opts.dir) ~= 'table' then
			error("Usage: migrate.pairs{ dir = { xlog = 'path/to/xlogs', snap = 'path/to/snaps' } }", 2)
		end
		opts.dir.xlog = opts.dir.xlog or opts.dir.snap

		if opts.dir.snap and self.confirmed_lsn == 0 then
			if not fio.path.is_dir(opts.dir.snap) then
				error(opts.dir.snap .. ": is not a directory", 2)
			end

			local snaps = fun.grep("^[0-9]+%.snap$", fio.listdir(opts.dir.snap)):totable()
			table.sort(snaps)

			self.snap_lsn = 0
			if #snaps == 0 then
				log.warn("Snaps not found at: %s", opts.dir.snap)
			else
				local snap = snaps[#snaps]
				self.snap_lsn = tonumber64(snap:sub(1, -#".snap"-1))

				snap = fio.pathjoin(opts.dir.snap, snap)
				iterator = iterator
					:chain(once(function() self.snap_recovery_started = clock.time() end))
					:chain(once(self.begin))
					:chain(M.file(snap):map(proxy(self.recovery_snap_row)))
					:chain(
						once(function()
							self:confirm(self.snap_lsn)
							self:commit()
							log.info("Snapshot %s was recovered in %.2fs", snap, clock.time()-self.snap_recovery_started)
						end)
					)
			end
		else
			self.snap_lsn = self.confirmed_lsn
		end

		if opts.dir.xlog then
			if not fio.path.is_dir(opts.dir.xlog) then
				error(opts.dir.xlog .. ": is not a directory", 2)
			end

			local xlogs = fun.grep("^[0-9]+%.xlog$", fio.listdir(opts.dir.xlog)):totable()
			table.sort(xlogs)

			local pos = 1
			for i = #xlogs, 1, -1 do
				local xlog_lsn = xlog2lsn(xlogs[i])
				if xlog_lsn < self.snap_lsn then
					pos = i
					break
				end
			end

			for j = pos, #xlogs do
				local xlog = fio.pathjoin(opts.dir.xlog, xlogs[j])

				local xlog_iterator do
					xlog_iterator = M.file(xlog)

					local xlog_lsn = xlog2lsn(xlogs[j])
					if xlog_lsn <= self.snap_lsn then
						xlog_iterator = xlog_iterator:drop_while(function(t)
							return t.HEADER.lsn < self.snap_lsn
						end)
					end

					if self.checklsn then
						-- checklsn also confirms lsn if wal order is ok
						log.verbose("Setting checklsn for %s", xlog)
						xlog_iterator = xlog_iterator:map(checklsn(self))
					else
						xlog_iterator = xlog_iterator:map(confirmer(self))
					end
				end

				-- Chain xlog iterator into global iterator:
				iterator = iterator
					:chain(once(function() self.xlog_recovery_started = clock.time() end))
					:chain(xlog_iterator)
					:chain(
						once(function()
							self.recovery_snap_row()
							self.commit()
							log.info("Xlog %s was recovered in %.2fs", xlog, clock.time() - self.xlog_recovery_started)
						end)
					)
			end
		end
	end

	if opts.replication then
		if type(opts.replication) == 'string' then
			local host, port = opts.replication:match("^(.+):([^:]+)$")
			self.replication = {
				host = host,
				port = tonumber(port),
				timeout = 1,
				debug = self.debug,
			}
		elseif type(opts.replication) == 'table' then
			self.replication = opts.replication
			assert(self.replication.host, "replication.host is required")
			assert(self.replication.port, "replication.port is required (primary)")
		else
			error("Malformed field replication", 2)
		end

		iterator = fun.chain(
			iterator,

			once(function()
				self.replication.confirmed_lsn = self.confirmed_lsn
				self.replica_iterator, self.replica = M.replica(self.replication)

				self:on_replica_connected(self.replica)

				if self.checklsn then
					self.replica_iterator = self.replica_iterator:map(checklsn(self))
				else
					self.replica_iterator = self.replica_iterator:map(confirmer(self))
				end
			end),

			fun.ones():map(function() return self.replica_iterator:nth(1) end):take_while(fun.op.truth),

			once(function()
				self.replica:close()
			end)
		)
	end

	-- We need to close openned transaction
	if self.persist then
		iterator = iterator:chain(once(self.commit))
	end

	iterator = iterator:grep(fun.op.truth)

	self.iterator = iterator

	---@TODO: automigrate based on DDL
	-- if opts.automigrate then
	-- 	assert(opts.ddl, "ddl is required for automigrate")
	-- 	self.iterator:each(require 'migrate.automigrate'(opts.ddl))
	-- 	return true
	-- end

	return iterator
end

local function replica_iterator(self)
	local t
	while not t and not self.replica:consistent() do
		t = self.channel:get(0.01)
	end
	if t == nil then
		return nil
	end

	local h, space, tuple, op, extra = unpack(t)
	local key
	if op == 'UPDATE' or op == 'DELETE' then
		key, tuple = tuple, extra
	end
	return h.lsn, {
		HEADER = {
			lsn  = h.lsn,
			type = op,
			timestamp = h.tm,
			source = 'replica',
			file   = self.replica:desc(),
		},
		BODY = {
			space_id = space,
			tuple = tuple,
			key = key,
		},
	}
end

function M.replica(opts)
	if type(opts) ~= 'table' then
		error("Usage: migrate.replica({host = 'host', port = 'port', ...})", 2)
	end

	local channel = fiber.channel()

	function opts.on_tuple(...)
		assert(channel:put({...}))
	end

	local self = { channel = channel, replica = require 'migrate.replica'(opts.host, opts.port, opts) }
	return fun.iter(replica_iterator, self), self.replica
end

return M
