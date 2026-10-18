-- All keys are in one conversation slot. The global scheduling ZSet is deliberately outside this script.
for _, k in ipairs(KEYS) do
    local t = redis.call('TYPE', k).ok
    if t ~= 'none' and t ~= 'string' then return 0 end
end
if (redis.call('GET', KEYS[1]) or '') ~= ARGV[1] then return 0 end
if ARGV[3] ~= '' and redis.call('GET', KEYS[2]) ~= ARGV[3] then return 0 end
if ARGV[4] ~= '' then
    local raw = redis.call('GET', KEYS[3])
    if not raw then return 0 end
    local ok, tx = pcall(cjson.decode, raw)
    if not ok or tx.generation ~= ARGV[4] or tostring(tx.epoch) ~= ARGV[5]
        or tx.archivedThrough < tonumber(ARGV[6]) then return 0 end
end
if ARGV[7] ~= '' then redis.call('MSET', KEYS[1], ARGV[2], KEYS[4], ARGV[7])
else redis.call('SET', KEYS[1], ARGV[2]) end
return 1
