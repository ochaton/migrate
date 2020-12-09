local ffi = require 'ffi.reloadable'
local errno = require 'errno'
local fio = require 'fio'
local fun = require 'fun'
local log = require 'log'
local saferbuf = require 'bin.saferbuf'

local parse_xlog_row = require 'migrate.parser'.parse_xlog_row
local parse_snap_row = require 'migrate.parser'.parse_snap_row

ffi.typedef('off64_t', [[typedef int64_t off64_t;]])
ffi.fundef('mmap', [[
	void *mmap(void *addr, size_t length, int prot, int flags,int fd, off64_t offset);
]])
ffi.fundef('munmap', [[
	int munmap(void *addr, size_t length);
]])

local PROT_READ  = 1
local MAP_SHARED = 1
local magic = 0xba0babed

local hsize = ffi.sizeof "struct header_v11"

local M = {}

local function row_iterator(self)
	if self.buf:avail() == 0 then
		self:close()
		return nil
	end

	local buf = self.buf
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
	return self, {
		HEADER = {
			lsn  = h.lsn,
			type = op,
			timestamp = h.tm,
		},
		BODY = {
			space_id = space,
			tuple = tuple,
			key = key,
		},
	}
end

function M.pairs(path)
	local file do
		local err
		file, err = fio.open(path, {"O_RDONLY"})
		if err then
			error(tostring(err))
		end
	end

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
	self.size     = tonumber64(file:stat().size)

	local base = ffi.C.mmap(box.NULL, self.size, PROT_READ, MAP_SHARED, file.fh, 0)
	if base == -1 then
		error(errno.strerror(ffi.errno()))
	end
	self.base = ffi.gc(base, function(addr)
		if -1 == ffi.C.munmap(addr, self.size) then
			log.error("Failed to unmap addr for %s: %s", path, errno.strerror(ffi.errno()))
		end
	end)
	self.buf = saferbuf.new(base, self.size)

	function self:close() -- luacheck: ignore
		ffi.gc(self.base, nil)
		self.buf = nil
		if -1 == ffi.C.munmap(self.base, self.size) then
			error(("Failed to unmap addr for %s: %s"):format(path, errno.strerror(ffi.errno())), 2)
		end
		assert(self.file:close())
	end

	assert(self.buf:str(#self.header) == self.header, "binary header missmatch")

	return fun.iter(row_iterator, self)
end

return M