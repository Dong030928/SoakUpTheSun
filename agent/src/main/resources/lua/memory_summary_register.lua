local function stringOrNone(k)
    local t = redis.call('TYPE', k).ok
    return t == 'none' or t == 'string'
end
for _, k in ipairs(KEYS) do if not stringOrNone(k) then return 0 end end
local raw = redis.call('GET', KEYS[3])
local proof = {events={}}
if raw then
    local ok
    ok, proof = pcall(cjson.decode, raw)
    if not ok or proof.generation ~= ARGV[6] or type(proof.events) ~= 'table' then return 0 end
end
local old = proof.events[ARGV[4]]
if old then
    if old.hash == ARGV[4] then return 1 end
    return 0
end
if redis.call('GET', KEYS[1]) ~= ARGV[1] or redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
local now = tonumber(ARGV[7])
local count = 0
for id, entry in pairs(proof.events) do
    if entry.doneAt and now - entry.doneAt >= tonumber(ARGV[8]) then proof.events[id] = nil
    else count = count + 1 end
end
if count >= tonumber(ARGV[9]) then return 0 end
proof.generation = ARGV[6]
proof.events[ARGV[4]] = {hash=ARGV[4], body=ARGV[5], at=now, lastRedrive=now}
local encoded = cjson.encode(proof)
if string.len(encoded) > tonumber(ARGV[10]) then return 0 end
-- The immutable event and its accepted task intent become visible together, only after half-message ACK.
redis.call('MSET', KEYS[2], ARGV[3], KEYS[3], encoded)
return 1
