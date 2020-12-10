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
	return 0, {
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
	ffi.gc(base, function(b) self.base = nil ffi.C.munmap(b, size) end)

	if base == -1 then
		error(errno.strerror(ffi.errno()))
	end
	if -1 == ffi.C.madvise(base, self.size, MADV_SEQUENTIAL) then
		error(errno.strerror(ffi.errno()))
	end
	self.base = base
	self.buf = saferbuf.new(base, self.size)

	function self:close() -- luacheck: ignore
		self.buf = nil
		if self.base then
			ffi.gc(self.base, nil)
			if -1 == ffi.C.munmap(self.base, self.size) then
				error(("Failed to unmap addr for %s: %s"):format(path, errno.strerror(ffi.errno())), 2)
			end
			self.base = nil
		end
		log.verbose("Closing %s", path)
		fiber.new(self.file.close, self.file)
	end

	self.__guard = newproxy()
	debug.setmetatable(self.__guard, { __gc = function() self:close() end })

	assert(self.buf:str(#self.header) == self.header, "binary header missmatch")
	return fun.iter(file_iterator, self)
end

local function dir_iterator(self, state)
	if not state.reading then
		local fid = state.fid
		local file = self.fileorder[fid+1]
		if not file then
			return nil
		end
		self.file = file
		state.fid = fid+1
		state.reading = M.file(file)
		state.source = state.reading.param.source
	end

	local fstate, value = state.reading.gen(state.reading.param, state.reading.state)
	if fstate then
		state.reading.state = fstate
		return state, value
	else
		state.reading = nil
		if state.source == 'snap' then
			self.confirmed_lsn = self.snap_lsn
		end
		return dir_iterator(self, state)
	end
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

	local self = { confirmed_lsn = opts.confirmed_lsn or 0, checklsn = true }

	if opts.dir then
		if type(opts.dir) == 'string' then
			opts.dir = { snap = opts.dir, xlog = opts.dir }
		end
		opts.dir.xlog = opts.dir.xlog or opts.dir.snap

		self.fileorder = {}

		if opts.dir.snap then
			local snaps = fun.grep("^[0-9]+%.snap$", fio.listdir(opts.dir.snap)):totable()
			table.sort(snaps)

			self.snap_lsn = 0
			if #snaps == 0 then
				log.warn("Snaps not found at: %s", opts.dir.snap)
			else
				local snap = snaps[#snaps]
				table.insert(self.fileorder, fio.pathjoin(opts.dir.snap, snap))
				self.snap_lsn = tonumber64(snap:sub(1, -#".snap"-1))
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
				local lsn = tonumber64(xlogs[i]:sub(1, -#".xlog"-1))
				if lsn < self.snap_lsn then
					pos = i
					break
				end
			end
			for j = pos, #xlogs do
				table.insert(self.fileorder, fio.pathjoin(opts.dir.xlog, xlogs[j]))
			end
		end
	end

	if opts.checklsn == false then
		self.checklsn = false
	end

	local iterator = fun.iter(dir_iterator, self, { fid = 0 })
	if self.checklsn then
		iterator = iterator:map(function(trans)
			local h = trans.HEADER
			if h.source == 'xlog' and h.lsn > self.confirmed_lsn then
				if h.lsn ~= self.confirmed_lsn+1 then
					error(("XlogGapError: Missing transactions between %s and %s while reading %s"):format(
						self.confirmed_lsn, h.lsn, self.file
					))
				end
				self.confirmed_lsn = h.lsn
			end
			return trans
		end)
	end

	return iterator
end

return M