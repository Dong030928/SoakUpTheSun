-- A single-key atomic update works on standalone Redis and Redis Cluster.
local previous = redis.call('GET', KEYS[1])
if previous then
    local ok, record = pcall(cjson.decode, previous)
    if ok and type(record) == 'table' and type(record.timestamp) == 'number' then
        if record.timestamp > tonumber(ARGV[2]) then return 0 end
    end
end
redis.call('PSETEX', KEYS[1], ARGV[3], ARGV[1])
return 1
