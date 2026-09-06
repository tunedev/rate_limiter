package lua

// tokenBucketSrc mirrors domain.TokenBucketLimiter.Apply. Timestamps are in
// microseconds: Lua numbers are doubles, exact only below 2^53, and a Unix
// nanosecond timestamp is past that. The refill rate arrives in nanoseconds,
// which is the unit domain.Params.Rate works in and is small enough to stay
// exact; accrual scales the microsecond elapsed time up to meet it.
//
// KEYS[1] state hash
// ARGV: limit, burst, window_us, rate_ns, cost, ttl_ms
// returns: allowed, remaining, retry_after_us, reset_at_us
const tokenBucketSrc = `
local limit   = tonumber(ARGV[1])
local burst   = tonumber(ARGV[2])
local window  = tonumber(ARGV[3])
local rate_ns = tonumber(ARGV[4])
local cost    = tonumber(ARGV[5])
local ttl     = tonumber(ARGV[6])

local capacity = limit + burst
if rate_ns <= 0 then
  return redis.error_reply('rate_limiter: Limit over Window leaves no time between permits')
end

-- micros converts a whole number of permits at rate_ns into microseconds.
local function micros(permits)
  return math.floor(permits * rate_ns / 1000)
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local h = redis.call('HMGET', KEYS[1], 'tokens', 'at')
if (h[1] == false) ~= (h[2] == false) then
  return redis.error_reply('rate_limiter: partial token bucket state')
end

local tokens, at
if h[1] == false then
  tokens, at = capacity, now
else
  tokens, at = tonumber(h[1]), tonumber(h[2])
end

local elapsed = now - at
if elapsed < 0 then
  elapsed = 0
elseif elapsed > window then
  elapsed = window
end

local accrued = math.floor(elapsed * 1000 / rate_ns)
if accrued > 0 then
  tokens = math.min(capacity, tokens + accrued)
  at = at + micros(accrued)
end
if tokens >= capacity then
  at = now
end

local allowed, retry = 1, 0
if tokens < cost then
  allowed = 0
  retry = micros(cost - tokens)
else
  tokens = tokens - cost
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'at', at)
redis.call('PEXPIRE', KEYS[1], ttl)

return {allowed, tokens, retry, now + micros(capacity - tokens)}
`
