local connection = require 'connection'
local tnt15 = require 'connection.legacy'

local M = require'obj'.class({}, 'migrate.replica', connection)
local ffi = require 'ffi'
local saferbuf = require 'bin.saferbuf'
local fiber = require 'fiber'
local errno = require 'errno'
local json = require 'json'

local DISCONECT = 1
local REQUESTED = 2
local STREAMING = 3
local DESTROYED = 4

local parse_xlog_row = require 'migrate.parser'.parse_xlog_row
local hsize = ffi.sizeof "struct header_v11"

function M:_init(host, primary_port, opt)
	self.on_tuple = assert(opt.on_tuple, "on_tuple callback is required")
	self.confirmed_lsn = assert(opt.confirmed_lsn, "migrate.replica: confirmed_lsn is required")
	self.buf = saferbuf.new(self.rbuf, 0)

	self.primary = tnt15(host, primary_port)
	self.primary:wait_con(10)

	local box_cfg do
		box_cfg = self.primary:call('box.dostring', "return box.cjson.encode(box.cfg)")
		box_cfg = json.decode(box_cfg)
	end

	self.upstream = { host = host, port = tonumber(box_cfg.replication_port) }
	self.replica_state = DISCONECT
	self:super(M, '_init')(self.upstream.host, self.upstream.port, opt)

	self.pull_primary = fiber.channel()

	self.fiber = fiber.create(function()
		while not self.pull_primary:is_closed() do
			local box_info = self.primary:call('box.dostring', "return box.cjson.encode(box.info())")
			box_info = json.decode(box_info)
			self.remote_lsn = box_info.lsn
			self.remote_rw  = box_info.status == 'primary'
			self.pull_primary:get(0.1)
		end
	end)
end

function M:on_connected()
	local request_lsn = self.confirmed_lsn+1
	self:log('I', "Connection established. Requesting LSN=%", request_lsn)
	self:push_write(ffi.new('uint64_t[1]', request_lsn), 8)
	self.replica_state = REQUESTED
end

function M:on_disconnect(e)
	if self.replica_state == DESTROYED then
		self.pull_primary:close()
		return
	end

	self.replica_state = DISCONECT
	self:super(M, 'on_disconnect')(e)
end

function M:callback(...)
	local ok, err = pcall(self.on_tuple, ...)
	if not ok then
		self:log('E', "on_tuple callback failed: %s", err)
		self:on_connect_reset(errno.ECONNABORTED)
		return false
	end

	return true
end

function M:on_read()
	local buf = self.buf
	buf.len = self.avail

	if self.replica_state == REQUESTED then
		local proto_version = buf:u32()
		assert(proto_version == 11, "Protocol version missmatch")
		self.replica_state = STREAMING
	end

	local confirmed_lsn = self.confirmed_lsn

	while buf:avail() > hsize do
		local save = buf.p.c
		local h = ffi.cast('struct header_v11 *', buf.p.c)

		assert(h.header_crc32c == ffi.C.crc32_calc(0, buf.p.c+4, hsize-4), "header crc32 missmatch")
		buf:skip(hsize)

		if buf:avail() < h.len then
			buf.p.c = save
			break
		end

		local row_type = buf:u16()
		buf:skip(8) -- drop cookie (do we actually must do this?)

		assert(row_type == ffi.C.XLOG, "malformed row_type")

		self.last_action = fiber.time()
		self.lag = self.last_action - h.tm

		if not self:callback(h, parse_xlog_row(buf)) then
			break
		end
		confirmed_lsn = h.lsn
	end

	self.avail = buf:avail()
	self.confirmed_lsn = confirmed_lsn
end

function M:close()
	self.replica_state = DESTROYED
	self.pull_primary:close()
	self:super(M, 'close')()
end

M.destroy = M.close

return M
