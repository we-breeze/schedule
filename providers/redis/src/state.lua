-- All keys belong to one logical Redis shard. No operation removes work before
-- retaining either its durable record or a recoverable owned reservation.
local op = ARGV[1]
local p = cjson.decode(ARGV[2])
-- Lua errors do not roll back prior writes. Validate namespace key types before
-- mutation so corrupted/reused namespaces fail without partially enqueueing.
local expected = {'hash', 'zset', 'zset', 'zset', 'hash', 'hash', 'zset', 'hash', 'zset'}
for i, key in ipairs(KEYS) do
    local kind = redis.call('TYPE', key).ok
    if kind ~= 'none' and kind ~= expected[i] then
        return cjson.encode({error='invalid namespace key type at index ' .. i})
    end
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local function present(v) return v ~= nil and v ~= cjson.null end
local function read(id)
    local raw = redis.call('HGET', KEYS[1], id)
    if raw then return cjson.decode(raw) end
end
local function save(r) redis.call('HSET', KEYS[1], r.job.id, cjson.encode(r)) end
local function terminal(r)
    return r.status == 'Succeeded' or r.status == 'Failed' or r.status == 'Cancelled'
end
local function finish_terminal(r)
    r.finished_at_ms = now
    redis.call('ZADD', KEYS[4], now, r.job.id)
    redis.call('ZREM', KEYS[2], r.job.id)
    redis.call('ZREM', KEYS[9], r.job.id)
    if present(r.job.overlap_key) and redis.call('HGET', KEYS[5], r.job.overlap_key) == r.job.id then
        redis.call('HDEL', KEYS[5], r.job.overlap_key)
    end
end
local function queue(r)
    redis.call('ZADD', KEYS[2], r.job.run_at_ms, r.job.id)
    if present(r.job.expires_at_ms) then
        redis.call('ZADD', KEYS[9], r.job.expires_at_ms, r.job.id)
    end
end
local function prune(retention)
    local excess = redis.call('ZCARD', KEYS[4]) - retention
    if excess <= 0 then return end
    local ids = redis.call('ZRANGE', KEYS[4], 0, -1)
    for _, id in ipairs(ids) do
        if excess <= 0 then break end
        if not redis.call('HGET', KEYS[8], id) then
            redis.call('HDEL', KEYS[1], id)
            redis.call('ZREM', KEYS[4], id)
            excess = excess - 1
        end
    end
end
local function insert(job)
    if redis.call('HEXISTS', KEYS[1], job.id) == 1 then return 'existing' end
    if present(job.overlap_key) then
        local old_id = redis.call('HGET', KEYS[5], job.overlap_key)
        if old_id then
            local old = read(old_id)
            if old and not terminal(old) then return 'overlap' end
        end
    end
    if redis.call('HLEN', KEYS[1]) - redis.call('ZCARD', KEYS[4]) >= p.capacity then
        return 'capacity'
    end
    local r = {job=job, status='Queued', failures=0, deliveries=0,
        owner_token=cjson.null, lease_until_ms=cjson.null, finished_at_ms=cjson.null,
        result=cjson.null, last_error=cjson.null}
    save(r)
    queue(r)
    if present(job.overlap_key) then redis.call('HSET', KEYS[5], job.overlap_key, job.id) end
    return 'inserted'
end
local function owns(r)
    return r and r.status == 'Running' and r.owner_token == p.token and r.lease_until_ms > now
end
if op == 'get' then
    return redis.call('HGET', KEYS[1], p.id) or 'null'
elseif op == 'submit' then
    local status = insert(p.job)
    if status == 'capacity' then return cjson.encode({error='capacity'}) end
    return cjson.encode(status)
elseif op == 'claim' then
    local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, p.capacity)
    for _, id in ipairs(expired) do
        local r = read(id)
        if r and r.status == 'Running' and r.lease_until_ms <= now then
            r.status = 'Queued'
            r.owner_token = cjson.null
            r.lease_until_ms = cjson.null
            save(r)
            queue(r)
        end
        redis.call('ZREM', KEYS[3], id)
    end
    -- Expiry applies even when a job's due time is still in the future or its
    -- kind is not consumed by this worker. Terminal records release capacity.
    local deadlines = redis.call('ZRANGEBYSCORE', KEYS[9], '-inf', now, 'LIMIT', 0, p.capacity)
    for _, id in ipairs(deadlines) do
        local r = read(id)
        if r and r.status == 'Queued' then
            r.status = 'Cancelled'
            finish_terminal(r)
            save(r)
        end
        redis.call('ZREM', KEYS[9], id)
    end
    prune(p.retention)
    local allowed = {}
    for _, kind in ipairs(p.kinds) do allowed[kind] = true end
    local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, p.capacity)
    local claims = {}
    for _, id in ipairs(ids) do
        if #claims >= p.limit then break end
        local r = read(id)
        if r and r.status == 'Queued' and allowed[r.job.kind] then
            r.status = 'Running'
            r.owner_token = p.owner .. ':' .. p.nonce .. ':' .. id
            r.lease_until_ms = now + p.lease_ms
            r.deliveries = r.deliveries + 1
            save(r)
            redis.call('ZREM', KEYS[2], id)
            redis.call('ZREM', KEYS[9], id)
            redis.call('ZADD', KEYS[3], r.lease_until_ms, id)
            table.insert(claims, {record=r, owner_token=r.owner_token})
        end
    end
    if #claims == 0 then return '[]' end
    return cjson.encode(claims)
elseif op == 'renew' then
    local r = read(p.id)
    if not owns(r) then return 'false' end
    r.lease_until_ms = now + p.lease_ms
    save(r)
    redis.call('ZADD', KEYS[3], r.lease_until_ms, p.id)
    return 'true'
elseif op == 'finish' then
    local r = read(p.id)
    if not owns(r) then return 'false' end
    local c = p.completion
    if c == 'Cancelled' then r.status = 'Cancelled'
    elseif c.Success ~= nil then r.status = 'Succeeded'; r.result = c.Success
    elseif c.ContinueAt ~= nil then
        r.status = 'Queued'
        r.job.run_at_ms = c.ContinueAt.run_at_ms
        if present(c.ContinueAt.payload) then r.job.payload = c.ContinueAt.payload end
    elseif c.Failure ~= nil then
        r.failures = r.failures + 1
        r.last_error = c.Failure.error
        if present(c.Failure.retry_at_ms) and r.failures < r.job.max_failures then
            r.status = 'Queued'; r.job.run_at_ms = c.Failure.retry_at_ms
        else r.status = 'Failed' end
    else return cjson.encode({error='invalid completion'}) end
    r.owner_token = cjson.null
    r.lease_until_ms = cjson.null
    redis.call('ZREM', KEYS[3], p.id)
    if terminal(r) then
        finish_terminal(r)
    else queue(r) end
    save(r)
    prune(p.retention)
    return 'true'
elseif op == 'register' then
    local raw = redis.call('HGET', KEYS[6], p.schedule.id)
    local rev = 0
    if raw then
        local old = cjson.decode(raw)
        if old.signature == p.signature then return 'true' end
        rev = old.revision + 1
        if present(old.last_job_id) then redis.call('HDEL', KEYS[8], old.last_job_id) end
    elseif redis.call('HLEN', KEYS[6]) >= p.capacity then return cjson.encode({error='capacity'}) end
    local r = {schedule=p.schedule, signature=p.signature, revision=rev,
        next_run_at_ms=p.schedule.first_run_at_ms, last_job_id=cjson.null}
    redis.call('HSET', KEYS[6], p.schedule.id, cjson.encode(r))
    redis.call('ZADD', KEYS[7], r.next_run_at_ms, p.schedule.id)
    return 'true'
elseif op == 'due' then
    local allowed = nil
    if present(p.kinds) then
        allowed = {}
        for _, kind in ipairs(p.kinds) do allowed[kind] = true end
    end
    local ids = redis.call('ZRANGEBYSCORE', KEYS[7], '-inf', now, 'LIMIT', 0, p.capacity)
    local records = {}
    for _, id in ipairs(ids) do
        if #records >= p.limit then break end
        local r = cjson.decode(redis.call('HGET', KEYS[6], id))
        if not allowed or allowed[r.schedule.job.kind] then table.insert(records, r) end
    end
    if #records == 0 then return '[]' end
    return cjson.encode(records)
elseif op == 'advance' then
    local raw = redis.call('HGET', KEYS[6], p.id)
    if not raw then return 'false' end
    local r = cjson.decode(raw)
    if r.revision ~= p.revision then return 'false' end
    if present(p.job) then
        local status = insert(p.job)
        if status == 'capacity' then return cjson.encode({error='capacity'}) end
        if status ~= 'overlap' then
            if present(r.last_job_id) then redis.call('HDEL', KEYS[8], r.last_job_id) end
            r.last_job_id = p.job.id
            if present(p.next) then redis.call('HSET', KEYS[8], p.job.id, p.id) end
        end
    end
    r.revision = r.revision + 1
    r.next_run_at_ms = p.next
    redis.call('HSET', KEYS[6], p.id, cjson.encode(r))
    if present(p.next) then redis.call('ZADD', KEYS[7], p.next, p.id)
    else redis.call('ZREM', KEYS[7], p.id) end
    prune(p.retention)
    return 'true'
end
return cjson.encode({error='unknown operation'})
