-- Recovery is serialized with chat and never overwrites any existing hot record.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
return 1
