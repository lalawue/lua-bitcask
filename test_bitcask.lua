--
--

local Bitcask = require("bitcask")
local FileSystem = require("lfs")

local config = {
    dir = "/tmp/bitcask",
    file_size = 512 -- 512 byte
}

local db = Bitcask.opendb(config)

-- test bucket
db:changeBucket("hello")
db:set("a", "b")

local attr = FileSystem.attributes("/tmp/bitcask/hello")
if not attr or attr.mode ~= "directory" then
    print("failed to create bucket 'hello'")
    os.exit(0)
end

attr = FileSystem.attributes("/tmp/bitcask/hello/0000000000.dat")
if not attr or attr.mode ~= "file" then
    print("failed to set in bucket 'hello'")
    os.exit(0)
end

-- change to default bucket with set/get/delete/gc
--
db:changeBucket("0")

if db:get("a") then
    print("Invalid bucket namespace")
else
    print("PASS Bucket")
end

local count = 256
local value = "abcdefghijklmnopqrstuvwxyz"

for i = 1, count, 1 do
    local name = tostring(i)
    db:set(name, value .. name)
end

local has_invalid = false
for i = 1, count, 1 do
    local name = tostring(i)
    if db:get(name) ~= (value .. name) then
        print("Invalid get ", i)
        has_invalid = true
        break
    end
end

if not has_invalid then
    print("PASS Set/Get")
end

for i = 1, count, 2 do
    db:remove(tostring(i))
end

has_invalid = false
for i = 1, count, 2 do
    if db:get(tostring(i)) then
        print("Failed to delete ", i)
        has_invalid = true
        break
    end
end

if not has_invalid then
    print("PASS Delete")
end

db:gc("0")
db = nil

-- open new database to test after gc
collectgarbage()
local ndb = Bitcask.opendb(config)

has_invalid = false
for i = 1, count, 1 do
    local name = tostring(i)
    if i % 2 == 1 then
        if ndb:get(name) then
            has_invalid = true
            print("GC failed to delete ", i)
            break
        end
    else
        if ndb:get(name) ~= (value .. name) then
            has_invalid = true
            print("GC failed to get ", i)
            break
        end
    end
end

if not has_invalid then
    print("PASS GC")
end
