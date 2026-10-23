local op, token, ttl = ARGV[1], ARGV[2], tonumber(ARGV[3])
local current = redis.call('GET', KEYS[1])
if op == 'release' then
    if current == token then return redis.call('DEL', KEYS[1]) end
    return 0
end
if op == 'renew' and current ~= token then return 0 end
if op == 'acquire' and current and current ~= token then return 0 end
redis.call('SET', KEYS[1], token, 'PX', ttl)
local clock = redis.call('TIME')
return tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000) + ttl
