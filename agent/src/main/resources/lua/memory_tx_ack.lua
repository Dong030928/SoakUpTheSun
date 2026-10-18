if redis.call('TYPE', KEYS[1]).ok ~= 'string' then return 0 end
local ok, s = pcall(cjson.decode, redis.call('GET', KEYS[1]))
if not ok or type(s) ~= 'table' or s.schemaVersion ~= 1 or s.generation ~= ARGV[1]
    or tostring(s.epoch) ~= ARGV[2] or type(s.events) ~= 'table' then return 0 end
local e = s.events[ARGV[3]]
local through = tonumber(ARGV[5])
if not e or e.hash ~= ARGV[4] or e.state ~= 'COMMITTED' or not through or through > s.head then return 0 end
e.archived = true
-- Retention starts at confirmation, never at original acceptance; repeated receipts do not postpone expiry.
if e.body then e.at = tonumber(ARGV[6]) end
e.body = nil
s.archivedThrough = math.max(s.archivedThrough, through)
local encoded = cjson.encode(s)
redis.call('SET', KEYS[1], encoded)
return 1
