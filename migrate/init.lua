local ffi = require 'ffi.reloadable'
local errno = require 'errno'
local fio = require 'fio'
local fun = require 'fun'
local fiber = require 'fiber'
local log = require 'log'
local saferbuf = require 'bin.saferbuf'

local parse_xlog_row = require 'migrate.parser'.parse_xlog_row
local parse_snap_row = require 'migrate.parser'.parse_snap_row

ffi.typedef('off64_t', [[typedef int64_t off64_t;]])

ffi.fundef('mmap', [[void *mmap(void *addr, size_t length, int prot, int flags,int fd, off64_t offset);]])
ffi.fundef('munmap', [[int munmap(void *addr, size_t length);]])
ffi.fundef('madvise', [[int madvise(void *addr, size_t length, int advice);]])

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
		local h = ffi.cast('struct header_v11 *', buf.p.c)
		assert(h.header_crc32c == ffi.C.crc32_calc(0, buf.p.c+4, hsize-4), "header crc32 missmatch")
	buf:skip(hsize)

	assert(buf:have(h.len), "unexpected end of file")
	assert(h.data_crc32c == ffi.C.crc32_calc(0, buf.p.c, h.len), "data crc32c missmatch")

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

local function file_iterator_close(obj)
	local self = obj.orig
	self.buf = nil
	if self.base then
		ffi.gc(self.base, nil)
		if -1 == ffi.C.munmap(self.base, self.size) then
			error(("Failed to unmap addr for %s: %s"):format(self.path, errno.strerror(ffi.errno())), 2)
		end
		self.base = nil
	end
	log.verbose("Closing %s", self.path)
	fiber.new(self.file.close, self.file)
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

	local self = { file = file }
	local header, row_type, parser, source
	if path:match("%.xlog$") then
		header = "XLOG\n0.11\n\n"
		row_type = ffi.C.XLOG
		parser = parse_xlog_row
		source = 'xlog'
	elseif path:match("%.snap$") then
		header = "SNAP\n0.11\n\n"
		row_type = ffi.C.SNAP
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

	local size = self.size
	local base = ffi.C.mmap(box.NULL, self.size, PROT_READ, MAP_SHARED, file.fh, 0)
	ffi.gc(base, function(b) ffi.C.munmap(b, size) end)

	if base == -1 then
		error(errno.strerror(ffi.errno()))
	end
	if -1 == ffi.C.madvise(base, self.size, MADV_SEQUENTIAL) then
		error(errno.strerror(ffi.errno()))
	end
	self.base = base
	self.buf = saferbuf.new(base, self.size)

	function self.close(obj)
		obj.buf = nil
		if obj.base then
			ffi.gc(obj.base, nil)
			if -1 == ffi.C.munmap(obj.base, obj.size) then
				error(("Failed to unmap addr for %s: %s"):format(path, errno.strerror(ffi.errno())), 2)
			end
			obj.base = nil
		end
		log.verbose("Closing %s", path)
		fiber.new(obj.file.close, obj.file)
	end

	self.__guard = newproxy()
	debug.setmetatable(self.__guard, {
		__index = { orig = { base = self.base, size = self.size, path = self.path, file = self.file, buf = self.buf } },
		__gc = file_iterator_close,
	})

	assert(self.buf:str(#self.header) == self.header, "binary header missmatch")
	return fun.iter(file_iterator, self)
end

local methods = {}
methods.__index = methods

function methods:commit()
	if self.persist then
		self.in_txn = 0
		box.commit()
	end
end

function methods:confirm(next_lsn)
	if self.persist then
		if self.in_txn == self.txn then
			box.commit()
			box.begin()
			self.in_txn = 0
		end
		self.in_txn = self.in_txn + 1
		self.confirmed_lsn = self.schema:put({ 'confirmed_lsn', next_lsn }).value
	else
		self.confirmed_lsn = next_lsn
	end
	log.verbose("Confirm lsn: %s", self.confirmed_lsn)
	return self.confirmed_lsn
end

local function finalizer(self, msg)
	return fun.iter({1}):map(function()
		self:commit()
		fiber.yield()
		log.info(msg)
	end):grep(fun.op.truth)
end

function M.pairs(opts)
	if type(opts) == 'string' then
		if fio.path.is_dir(opts) then
			opts = {
				dir = {
					snap = opts,
					xlog = opts,
				},
				confirmed_lsn = 0,
				persist = false,
			}
		else
			return M.file(opts)
		end
	end
	if type(opts) ~= 'table' then
		error("Usage: migrate.pairs(opts|path)", 2)
	end

	local self = setmetatable({
		confirmed_lsn = opts.confirmed_lsn or 0,
		persist = opts.persist,
		txn = opts.txn or 1,
		checklsn = true,

		in_txn = 0,
	}, methods)

	if opts.checklsn == false then
		self.checklsn = false
	end

	if self.persist then
		box.schema.space.create('_migration', {
			format = {
				{ name = 'key',   type = 'string' },
				{ name = 'value', type = 'unsigned' },
			},
			if_not_exists = true,
		}):create_index('pri', {
			parts = { 'key' },
			if_not_exists = true,
		})
		self.schema = box.space._migration
		local promote_lsn = (self.schema:get('promote_lsn') or {}).value
		local confirmed_lsn = (self.schema:get('confirmed_lsn') or {}).value

		if confirmed_lsn < promote_lsn then
			error("Cannot start migration. Previous migration was aborted during snapshot recovery. "
				.."You need to truncate all spaces which were affected during previous recovery and "
				.."box.space._migration", 2)
		end

		self.confirmed_lsn = confirmed_lsn
	end

	local iterator = fun.iter{} -- empty iterator

	if opts.dir then
		if type(opts.dir) == 'string' then
			opts.dir = { snap = opts.dir, xlog = opts.dir }
		end
		opts.dir.xlog = opts.dir.xlog or opts.dir.snap

		self.fileorder = {}

		if opts.dir.snap and self.confirmed_lsn == 0 then
			local snaps = fun.grep("^[0-9]+%.snap$", fio.listdir(opts.dir.snap)):totable()
			table.sort(snaps)

			self.snap_lsn = 0
			if #snaps == 0 then
				log.warn("Snaps not found at: %s", opts.dir.snap)
			else
				local snap = snaps[#snaps]
				self.snap_lsn = tonumber64(snap:sub(1, -#".snap"-1))

				snap = fio.pathjoin(opts.dir.snap, snap)
				iterator = fun.chain(
					iterator,
					M.file(snap),
					fun.iter{1}:map(function() self:confirm(self.snap_lsn) end):grep(fun.op.truth),
					finalizer(self, ("Snapshot %s was recovered"):format(snap))
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
				local xlog_lsn = tonumber64(xlogs[i]:sub(1, -#".xlog"-1))
				if xlog_lsn < self.snap_lsn then
					pos = i
					break
				end
			end

			for j = pos, #xlogs do
				local xlog = fio.pathjoin(opts.dir.xlog, xlogs[j])
				iterator = fun.chain(
					iterator,
					M.file(xlog),
					finalizer(self, ("Xlog %s was recovered"):format(xlog))
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
			fun.iter{1}:each(function()
				self.replication.confirmed_lsn = self.confirmed_lsn
				self.replica_iterator = M.replica(self.replication)
				self.replica = assert(self.replica_iterator.param.replica, "no replica")
				assert(self.replica:wait_con(10), "replica not connected")
			end),
			fun.ones():take_while(function()
				return not self.replica:consistent()
			end),
			fun.iter{1}:each(function()
				self.replica:close()
			end)
		)
	end

	if self.checklsn then
		iterator = iterator:map(function(trans)
			local h = trans.HEADER
			if h.source == 'xlog' and h.lsn > self.confirmed_lsn then
				if h.lsn ~= self.confirmed_lsn+1 then
					error(("XlogGapError: Missing transactions between %s and %s while reading %s"):format(
						self.confirmed_lsn, h.lsn, self.file
					))
				end
				self:confirm(h.lsn)
			end
			return trans
		end)
	else
		iterator = iterator:map(function(t)
			self:confirm(t.HEADER.lsn)
			return t
		end)
	end

	if self.persist then
		-- We need to close openned transaction
		iterator = fun.chain(iterator, fun.iter({1}):map(box.commit):grep(fun.op.truth))
	end

	self.iterator = iterator
	return iterator
end

local function replica_iterator(self)
	local t = self.channel:get()
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
		error("Usage: migrate.replica({[host:port]]})", 2)
	end

	local channel = fiber.channel()

	function opts.on_tuple(...)
		channel:put({...})
	end

	local self = { channel = channel, replica = require 'migrate.replica'(opts.host, opts.port, opts) }
	return fun.iter(replica_iterator, self)
end

return M
