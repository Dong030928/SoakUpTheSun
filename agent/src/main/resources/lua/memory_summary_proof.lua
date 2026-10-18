local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, proof = pcall(cjson.decode, raw)
if not ok or proof.generation ~= ARGV[1] or type(proof.events) ~= 'table' then return 0 end
local entry = proof.events[ARGV[2]]
if not entry or entry.hash ~= ARGV[2] then return 0 end
local now = tonumber(ARGV[3])
if ARGV[4] == 'ACK' then
    if not entry.doneAt then entry.doneAt = now end
    entry.body = nil
elseif ARGV[4] == 'CLAIM' then
    if entry.doneAt or now - entry.lastRedrive < tonumber(ARGV[5]) then return 0 end
    entry.lastRedrive = now
else return 0 end
redis.call('SET', KEYS[1], cjson.encode(proof))
return 1
