-- Validate every type/argument and encode the entire result BEFORE the first mutation.
-- MSET is one Redis command: hot snapshot and COMMITTED evidence cannot be partially applied by script errors.
local function stringOrNone(k)
    local t = redis.call('TYPE', k).ok
    return t == 'string' or t == 'none'
end
if not stringOrNone(KEYS[1]) or not stringOrNone(KEYS[2]) or not stringOrNone(KEYS[3]) then return 0 end
local raw = redis.call('GET', KEYS[2])
if not raw then return 0 end
local ok, s = pcall(cjson.decode, raw)
if not ok or type(s) ~= 'table' or s.schemaVersion ~= 1 or s.generation ~= ARGV[2]
    or s.conversationId ~= ARGV[15] or tostring(s.epoch) ~= ARGV[3] or type(s.events) ~= 'table'
    or type(s.head) ~= 'number' or type(s.archivedThrough) ~= 'number' or s.head < s.archivedThrough then return 0 end
local expected = tonumber(ARGV[4])
local now = tonumber(ARGV[10])
local ttl = tonumber(ARGV[9])
if not expected or not now or not ttl or ttl <= 0 or expected >= 9999999999999 then return 0 end
local entry = s.events[ARGV[5]]
if entry then
    if entry.hash ~= ARGV[6] or entry.sequence ~= expected + 1 then return 0 end
    if entry.state == 'COMMITTED' and s.head >= entry.sequence then return 1 end
    if entry.state == 'ABORTED' then return -1 end
    return 0
end
local count = 0
local pending = 0
for id, e in pairs(s.events) do
    if type(e) ~= 'table' or type(e.at) ~= 'number' or type(e.sequence) ~= 'number' then return 0 end
    if (e.archived == true or e.state == 'ABORTED') and now - e.at >= tonumber(ARGV[14]) then
        s.events[id] = nil
    else
        count = count + 1
        if e.state == 'COMMITTED' and e.archived ~= true then pending = pending + 1 end
    end
end
if count >= tonumber(ARGV[12]) then return 0 end
local state = 'COMMITTED'
if redis.call('GET', KEYS[1]) ~= ARGV[1] or s.head ~= expected or pending >= tonumber(ARGV[11]) then state = 'ABORTED' end
s.events[ARGV[5]] = {state=state, hash=ARGV[6], sequence=expected+1, at=now, archived=false}
if state == 'COMMITTED' then
    s.events[ARGV[5]].body = ARGV[7]
    s.head = expected + 1
end
local encoded = cjson.encode(s)
if string.len(encoded) > tonumber(ARGV[13]) then return 0 end
if state == 'ABORTED' then
    redis.call('SET', KEYS[2], encoded)
    return -1
end
redis.call('MSET', KEYS[2], encoded, KEYS[3], ARGV[8])
-- A failure after MSET still has complete commit proof. An omitted expiry only retains the cache longer.
redis.call('PEXPIRE', KEYS[3], ttl)
return 1
