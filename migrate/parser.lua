#!/usr/bin/env tarantool
local ffi = require 'ffi.reloadable'
local table_new = require 'table.new'
local ffi_cast = ffi.cast
local band = bit.band

ffi.typedef('struct header_v11', [[
	struct header_v11 {
		uint32_t header_crc32c;
		int64_t lsn;
		double tm;
		uint32_t len;
		uint32_t data_crc32c;
	} __attribute__((packed));
]])

ffi.typedef('struct box_snap_row', [[
	struct box_snap_row {
		uint32_t space;
		uint32_t tuple_size;
		uint32_t data_size;
		char data[];
	} __attribute__((packed));
]])

ffi.typedef('enum log_format', [[
	enum log_format { XLOG = 65534, SNAP = 65535 };
]])

ffi.fundef('crc32_calc', [[
	uint32_t crc32_calc(uint32_t crc, const unsigned char *buf, unsigned int len);
]])

local function parse_snap_row(rb)
	local row = ffi_cast('struct box_snap_row *', rb.p.c)
	rb:skip(4+4+4) -- skip header
	local save = rb.p.c

	local tuple = table_new(row.tuple_size, 0)
	for i = 1, row.tuple_size do
		tuple[i] = rb:str(rb:ber())
	end

	assert(rb.p.c - save == row.data_size, "malformed tuple")
	return row.space, tuple, 'INSERT'
end

local function parse_tuple(rb)
	local fc = rb:u32()

	local tuple = table_new(fc, 0)
	for i = 1, fc do
		tuple[i] = rb:str(rb:ber())
	end

	return tuple
end

local update_op_map = {
	[0] = '=',
	[1] = '+',
	[2] = '&',
	[3] = '^',
	[4] = '|',
	[6] = 'x',
	[7] = '>'
}

local REPLACE = 13
local UPDATE  = 19
local DELETE  = 21
local function parse_xlog_row(rb)
	local op = rb:u16()
	if not (op == REPLACE or op == UPDATE or op == DELETE) then
		error("Malformed XLOG operation")
	end

	local space, flags = rb:i32(), rb:i32()
	if op == REPLACE then
		return space, parse_tuple(rb), band(flags, 0x02) == 0x02 and 'INSERT' or 'REPLACE'
	elseif op == DELETE then
		return space, parse_tuple(rb), 'DELETE'
	elseif op == UPDATE then
		local tuple = parse_tuple(rb)
		local op_count = rb:i32()

		local ops = table_new(op_count, 0)
		for i = 1, op_count do
			local field_no = rb:i32()
			local op_num = rb:i8()
			local update_op = update_op_map[op_num]
			if not update_op then
				error("parser does not support update operation: "..op_num)
			end
			local new_val = rb:str(rb:ber())
			ops[i] = { update_op, field_no, new_val }
		end

		return space, tuple, 'UPDATE', ops
	end
end

return {
	parse_xlog_row = parse_xlog_row,
	parse_snap_row = parse_snap_row,
}