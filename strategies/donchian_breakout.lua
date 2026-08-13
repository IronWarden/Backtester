-- donchian_breakout.lua
-- Turtle-style channel breakout: buy when the close makes a new
-- `entry_period`-day high, exit when it makes a new `exit_period`-day low.
-- Enters on strength and rides the move until the channel breaks, so it
-- takes many small losses in exchange for a few large winners.
--
-- Params:
--   entry_period  lookback for the breakout high  (default 55)
--   exit_period   lookback for the stop low       (default 20)
--   buyType       "equalWeights" (default) | "greedy"

local entry_period = params.entry_period or 55
local exit_period  = params.exit_period or 20
local buyType      = params.buyType or "equalWeights"

-- Highest close over the `n` days ENDING THE DAY BEFORE `day`, so today's
-- close is compared against the prior channel rather than against itself.
local function highest(t, day, n)
    local hi = 0
    for i = day - n, day - 1 do
        local c = close_at(t, i)
        if c > hi then hi = c end
    end
    return hi
end

local function lowest(t, day, n)
    local lo = math.huge
    for i = day - n, day - 1 do
        local c = close_at(t, i)
        -- close_at returns 0 outside the series; ignore those.
        if c > 0 and c < lo then lo = c end
    end
    return lo
end

function step(day)
    local warmup = math.max(entry_period, exit_period)
    if day < warmup then return end
    for i = 1, #tickers do
        local t   = tickers[i]
        local px  = price(t, day)
        local pos = position(t)
        if px > 0 then
            if pos == nil then
                local hi = highest(t, day, entry_period)
                if hi > 0 and px > hi then
                    buy_max(t, px, buyType, day)
                end
            else
                local lo = lowest(t, day, exit_period)
                if lo < math.huge and px < lo then
                    sell_all(t, px, day)
                end
            end
        end
    end
end
