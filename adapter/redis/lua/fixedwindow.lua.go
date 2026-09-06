package lua

// fixedWindowSrc mirrors domain.FixedWindowLimiter.Apply. Windows are whole
// multiples of the window measured from the Unix epoch, which is what
// domain.truncateFromEpoch computes.
//
// KEYS[1] state hash
// ARGV: limit, window_us, cost, ttl_ms
// returns: allowed, remaining, retry_after_us, reset_at_us
const fixedWindowSrc = `
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost   = tonumber(ARGV[3])
local ttl    = tonumber(ARGV[4])

if window <= 0 then
  return redis.error_reply('rate_limiter: Window must be positive')
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local start = math.floor(now / window) * window
local reset = start + window

local h = redis.call('HMGET', KEYS[1], 'start', 'count')
if (h[1] == false) ~= (h[2] == false) then
  return redis.error_reply('rate_limiter: partial fixed window state')
end

local count = 0
if h[1] ~= false and tonumber(h[1]) == start then
  count = tonumber(h[2])
end

local allowed, retry, remaining = 1, 0, 0
if count + cost > limit then
  allowed = 0
  remaining = math.max(0, limit - count)
  retry = reset - now
else
  count = count + cost
  remaining = limit - count
end

redis.call('HSET', KEYS[1], 'start', start, 'count', count)
redis.call('PEXPIRE', KEYS[1], ttl)

return {allowed, remaining, retry, reset}
`
