-- Bounded re-drive lease, shared by all workers. A worker crash only delays the next attempt.
if redis.call('TYPE', KEYS[1]).ok ~= 'string' then return 0 end
local ok, s = pcall(cjson.decode, redis.call('GET', KEYS[1]))
if not ok or type(s) ~= 'table' or s.generation ~= ARGV[1] or type(s.events) ~= 'table' then return 0 end
local e = s.events[ARGV[2]]
local now = tonumber(ARGV[4])
if not e or e.hash ~= ARGV[3] or e.state ~= 'COMMITTED' or e.archived == true
    or now - math.max(e.at, e.lastRedrive or 0) < tonumber(ARGV[5]) then return 0 end
e.lastRedrive = now
redis.call('SET', KEYS[1], cjson.encode(s))
return 1
