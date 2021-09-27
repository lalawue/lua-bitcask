--
-- Copyright (c) 2021 lalawue
--
-- This library is free software; you can redistribute it and/or modify it
-- under the terms of the MIT license. See LICENSE for details.
--

local CRC32 = require("bitcask.crc32")
local Struct = require("struct")
local FileSystem = require("lfs")

local fType = type
local fOpen = io.open
local fString = tostring
local fNumber = tonumber
local fPairs = pairs
local iPairs = ipairs
local fNext = next
local fRep = string.rep
local sFormat = string.format
local tRemove = table.remove
local tInsert = table.insert
local oTime = os.time


-- Internal Interface
--

-- bucket operation

--[[
    create bucket dir and insert into _buckets
]]
local function _bucketCreate(self, name)
    self._buckets[name] = {
        act_fid = 0,
        max_fid = 0,
        free_fids = {},
        kinfo = {}
    }
    FileSystem.mkdir(self._config.dir .. "/" .. name)
end

--[[
    with leading '0'
]]
local function _indexString(fid)
    return fRep("0", 10 - fid:len()) .. fid
end

--[[
    to path like 'dir/bucket/0000000000.dat'
]]
local function _fidPath(self, fid, bucket_name)
    fid = _indexString(fString(fid))
    bucket_name = bucket_name or self._bucket_name
    return sFormat("%s/%s/%s.dat", self._config.dir, bucket_name, fid)
end

--[[
    get next empty fid slot
]]
local function _nextEmptyFid(bucket_info)
    if #bucket_info.free_fids > 0 then
        bucket_info.act_fid = bucket_info.free_fids[#bucket_info.free_fids]
        tRemove(bucket_info.free_fids)
    else
        bucket_info.act_fid = bucket_info.max_fid + 1
        bucket_info.max_fid = bucket_info.act_fid
    end
    return bucket_info.act_fid
end

--[[
    return active file id, file offset
]]
local function _activeFid(self, bucket_name)
    bucket_name = bucket_name or self._bucket_name
    local bucket_info = self._buckets[bucket_name]
    if not bucket_info then
        return 0, 0
    end
    local act_fid = bucket_info.act_fid
    local offset = 0
    while true do
        local attr = FileSystem.attributes(_fidPath(self, act_fid, bucket_name))
        if attr then
            if attr.size >= self._config.file_size then
                if act_fid ~= bucket_info.max_fid then
                    act_fid = bucket_info.max_fid
                else
                    act_fid = _nextEmptyFid(bucket_info)
                end
            else
                offset = attr.size
                bucket_info.act_fid = act_fid
                break
            end
        else
            break
        end
    end
    return act_fid, offset
end

-- key/value operation

local _bfmt = "<IIIIII" --
local _rsize = Struct.pack(_bfmt, 0,0,0,0,0,0):len()

--[[
    content can be string or record_t
]]
local function _newRecord(content)
    local record = {}
    if content then
        if fType(content) == "string" then
            record.time, record.fid, record.offset, record.ksize, record.vsize, record.crc32
                = Struct.unpack(_bfmt, content)
        else
            record.time = content.time
            record.fid = content.fid
            record.offset = content.offset
            record.ksize = content.ksize
            record.vsize = content.vsize
            record.crc32 = content.crc32
        end
    end
    return record
end

--[[
    read one record, move file pointer to next record
]]
local function _readRecord(fp, read_value)
    local content = fp:read(_rsize)
    if content == nil then
        return nil
    end
    local record = _newRecord(content)
    local key = fp:read(record.ksize)
    local value = nil
    if read_value then
        value = fp:read(record.vsize)
    elseif record.vsize > 0 then
        fp:seek("cur", record.vsize)
    end
    return record, key, value
end

--[[
    append record to file path
]]
local function _writeRecord(path, record, key, value)
    local fp = fOpen(path, "ab+")
    if not fp then
        return false
    end
    fp:write(Struct.pack(_bfmt, record.time, record.fid, record.offset,
                                record.ksize, record.vsize, record.crc32))
    fp:write(key)
    if value then
        fp:write(value)
    end
    fp:close()
    return true
end

--[[
    read bucket info, last gc time
]]
local function _readBucketInfo(self, bucket_name)
    local fp = fOpen(sFormat("%s/%s.info", self._config.dir, bucket_name), "rb")
    if fp then
        local content = fp:read("*a")
        fp:close()
        return fNumber(content)
    end
    return 0
end

--[[
    write bucket info, current gc time
]]
local function _writeBucketInfo(self, bucket_name)
    local fp = fOpen(sFormat("%s/%s.info", self._config.dir, bucket_name), "wb")
    if fp then
        fp:write(fString(oTime()))
        fp:close()
    end
end

--[[
    load db bucket dir to memory structure _buckets
]]
local function _loadBucketsInfo(self)
    local path = self._config.dir
    for dname in FileSystem.dir(path) do
        local dpath = path .. "/" .. dname
        local dattr = FileSystem.attributes(dpath)
        if dattr and dattr.mode == "directory" and dname:sub(1, 1) ~= "." then
            local max_fid = 0
            for fname in FileSystem.dir(dpath) do
                local fpath = dpath .. "/" .. fname
                local fattr = FileSystem.attributes(fpath)
                if fattr and fattr.mode == "file" then
                    local fid = fNumber(fname:sub(1, fname:len() - 4))
                    if fid > max_fid then
                        max_fid = fid
                    end
                end
            end
            -- current bucket active fid
            _bucketCreate(self, dname)
            self._buckets[dname].max_fid = max_fid
        end
    end
    if fNext(self._buckets) == nil or not FileSystem.attributes(path .. "/" .. self._bucket_name) then
        _bucketCreate(self, self._bucket_name)
    end
end

--[[
    load every bucket's key/value
]]
local function _loadKeysInfo(self)
    for _, bucket_info in fPairs(self._buckets) do
        local max_fid = bucket_info.max_fid
        local kinfo = bucket_info.kinfo
        for fid = 0, max_fid, 1 do
            local fp = fOpen(_fidPath(self, fid), "rb")
            while fp do
                local record, key = _readRecord(fp, false)
                if record then
                    if record.vsize > 0 then
                        kinfo[key] = record
                    else
                        kinfo[key] = nil
                    end
                else
                    fp:close()
                    break
                end
            end
            if not fp and fid < bucket_info.max_fid then
                tInsert(bucket_info.free_fids, fid)
            end
        end
        bucket_info.act_fid = _activeFid(self)
    end
end

-- Public Interface
--

local _M = {}
_M.__index = _M

--[[
    list all bucket names
]]
function _M:allBuckets()
    local tbl = {}
    for name, _ in fPairs(self._buckets) do
        tbl[#tbl + 1] = name
    end
    return tbl
end

--[[
    change to bucket, if bucket not exist, create it
]]
function _M:changeBucket(name)
    if fType(name) ~= "string" or name:len() <= 0 then
        return false
    end
    if self._buckets[name] == nil then
        _bucketCreate(self, name)
    end
    self._bucket_name = name
    return true
end

--[[
    list all keys in bucket_name
]]
function _M:allKeys(bucket_name)
    local bucket_info = self._buckets[bucket_name or self._bucket_name]
    if bucket_info == nil then
        return {}
    end
    local tbl = {}
    for name, _ in fPairs(bucket_info.kinfo) do
        tbl[#tbl + 1] = name
    end
    return tbl
end

--[[
    get value info of key
]]
function _M:info(key)
    if fType(key) ~= "string" or key:len() <= 0 then
        return false
    end
    local kinfo = self._buckets[self._bucket_name].kinfo
    if kinfo == nil then
        return false
    end
    local info = kinfo[key]
    if info == nil then
        return false
    end
    return {
        time = info.time,
        size = info.vsize,
        crc32 = info.crc32
    }
end

--[[
    set key value to active bucket
]]
function _M:set(key, value)
    if fType(key) ~= "string" or fType(value) ~= "string" or key:len() <= 0 or value:len() <= 0 then
        return false
    end
    local kinfo = self._buckets[self._bucket_name].kinfo
    local record = kinfo[key]
    if record ~= nil then
        -- check original value
        local fp = fOpen(_fidPath(self, record.fid), "rb")
        if fp then
            fp:seek("set", record.offset + _rsize + record.ksize)
            local nvalue = fp:read(record.vsize)
            fp:close()
            if nvalue == value then
                -- same value
                return true
            end
        end
        -- remove origin record
        record.vsize = 0 -- means to be delete
        local fid = _activeFid(self) -- append to active fid
        if not _writeRecord(_fidPath(self, fid), record, key, nil) then
            return false
        end
    end
    -- create new record
    record = _newRecord()
    record.time = oTime()
    record.ksize = key:len()
    record.vsize = value:len()
    record.fid, record.offset = _activeFid(self)
    record.crc32 = CRC32.update(key .. value)
    kinfo[key] = record
    -- write to dat file
    if not _writeRecord(_fidPath(self, record.fid), record, key, value) then
        return false
    end
    return true
end

function _M:get(key)
    if fType(key) ~= "string" or key:len() <= 0 then
        return nil
    end
    local kinfo = self._buckets[self._bucket_name].kinfo
    local record = kinfo[key]
    if record == nil then
        return nil
    end
    local fp = fOpen(_fidPath(self, record.fid), "rb")
    if not fp then
        return nil
    end
    fp:seek("set", record.offset)
    local _, nkey, nvalue = _readRecord(fp, true)
    fp:close()
    if nkey == key and record.crc32 == CRC32.update(nkey .. nvalue) then
        return nvalue
    else
        return nil
    end
end

function _M:remove(key)
    if fType(key) ~= "string" or key:len() <= 0 then
        return false
    end
    local kinfo = self._buckets[self._bucket_name].kinfo
    local record = kinfo[key]
    if record == nil then
        return false
    end
    kinfo[key] = nil
    record.vsize = 0 -- means to be delete
    local fid = _activeFid(self) -- append to active fid
    if not _writeRecord(_fidPath(self, fid), record, key, nil) then
        return false
    end
    return true
end

-- garbage collection

--[[
    remove deleted record in buckets dat files
]]
function _M:gc(bucket_name)
    bucket_name = bucket_name or self._bucket_name
    local bucket_info = self._buckets[bucket_name]
    if bucket_info == nil then
        return false
    end
    -- get last gc time
    local last_time = _readBucketInfo(self, bucket_name)
    -- collect rm record entries, include old entry and rm entry
    local rm_tbl = {}
    for fid = 0, bucket_info.max_fid, 1 do
        local fid_path = _fidPath(self, fid, bucket_name)
        local fattr = FileSystem.attributes(fid_path)
        -- skip file no modification since last gc, for only collect remove record append
        if fattr and fattr.modification >= last_time then
            local fp = fOpen(fid_path, "rb")
            while fp do
                local rm_record = _readRecord(fp, false)
                if not rm_record then
                    fp:close()
                    break
                elseif rm_record.vsize == 0 then
                    -- insert origin record
                    local sfid = fString(rm_record.fid)
                    if not rm_tbl[sfid] then
                        rm_tbl[sfid] = {}
                    end
                    tInsert(rm_tbl[sfid], _newRecord(rm_record))
                    -- insert rm record in realy place
                    sfid = fString(fid)
                    if not rm_tbl[sfid] then
                        rm_tbl[sfid] = {}
                    end
                    rm_record.fid = fid -- rm record realy fid
                    rm_record.offset = fp:seek("cur") - _rsize - rm_record.ksize - rm_record.vsize
                    tInsert(rm_tbl[sfid], rm_record)
                end
            end -- while
        end -- fattr
    end
    -- if no delete entry
    if fNext(rm_tbl) == nil then
        return true
    end
    -- check in rm_tbl with realy fid, offset
    local function _inTbl(rm_tbl, fid, offset)
        for i, r in iPairs(rm_tbl) do
            if r.fid == fid and r.offset == offset then
                tRemove(rm_tbl, i)
                return true
            end
        end
        return false
    end
    local kinfo = bucket_info.kinfo
    -- merge origin fid, first increase act_fid
    _nextEmptyFid(bucket_info)
    for sfid, tbl in fPairs(rm_tbl) do
        local in_fid = fNumber(sfid)
        local in_path = _fidPath(self, in_fid, bucket_name)
        local in_fp = fOpen(in_path, "rb")
        local has_skip = false
        while in_fp do
            local in_offset = in_fp:seek("cur")
            local in_record, in_key, in_value = _readRecord(in_fp, true)
            if not in_record then
                break
            elseif _inTbl(tbl, in_fid, in_offset) then
                -- has deleted entry, remove original fid file
                has_skip = true
            elseif in_record.vsize > 0 then
                -- update kinfo
                in_record.fid, in_record.offset = _activeFid(self, bucket_name)
                kinfo[in_key] = in_record
                -- write to file
                local out_path = _fidPath(self, in_record.fid, bucket_name)
                _writeRecord(out_path, in_record, in_key, in_value)
            end
        end
        in_fp:close()
        if has_skip then
            os.remove(in_path)
        end
        tInsert(bucket_info.free_fids, in_fid)
    end
    -- record this gc time
    _writeBucketInfo(self, bucket_name)
    return true
end

--[[
    config should be {
        dir = "/path/to/store/data",
        file_size = "data file size",   -- default 64M
    }
]]
local function _opendb(config)
    if not config or fType(config.dir) ~= "string" then
        return nil
    end
    FileSystem.mkdir(config.dir)
    local ins = setmetatable({}, _M)
    ins._config = {}
    ins._config.dir = config.dir
    ins._config.file_size = config.file_size or (64 * 1024 * 1024) -- 64M default
    ins._bucket_name = config.bucket or "0"
    ins._buckets = {}
    --[[
        ins structure as
        {
            _config = {
                dir,                    -- db dir
                bucket,                 -- active bucket name when opened
                file_size               -- keep key/value in one entry in priority
            },
            _bucket_name = '0',         -- current active bucket name
            _buckets = {
                [name] = {
                    act_fid,            -- active file id in bucket
                    max_fid,            -- max file id in bucket
                    free_fids = {       -- free fid slot after delete entries
                    }
                    kinfo = {           -- record_t map with key index
                        [key] = record_t
                    }
                }
            },
        }
    ]]
    _loadBucketsInfo(ins)
    _loadKeysInfo(ins)
    return ins
end

return {
    opendb = _opendb
}
