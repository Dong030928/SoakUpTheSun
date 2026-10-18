-- Offline, drained, retention-checked release. A concurrent receipt/checkpoint change prevents removal.
if redis.call('TYPE', KEYS[1]).ok ~= 'string' or redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('TYPE', KEYS[2]).ok ~= 'string' or redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[2])
return 1
