-- sma_cross.lua
-- Classic dual-SMA crossover. When the short SMA crosses above the long
-- SMA we open a position; on the inverse cross we flatten.
--
-- All averaging is delegated to Go (`sma`), this script just compares
-- yesterday's and today's values and emits orders.
--
-- Params:
--   short    short-window length      (default 10)
--   long     long-window length       (default 50)
--   buyType  "equalWeights" (default) | "greedy"

local short   = params.short   or 10
local long    = params.long    or 50
local buyType = params.buyType or "equalWeights"

local prevShort = {}
local prevLong  = {}

function step(day)
    if day < long then return end
    for i = 1, #tickers do
        local t = tickers[i]
        local s = sma(t, day, short)
        local l = sma(t, day, long)
        local ps = prevShort[t] or 0
        local pl = prevLong[t]  or 0
        if ps ~= 0 and pl ~= 0 then
            local p = price(t, day)
            if s > l and ps <= pl then
                buy_max(t, p, buyType, day)
            elseif s < l and ps >= pl then
                sell_all(t, p, day)
            end
        end
        prevShort[t] = s
        prevLong[t]  = l
    end
end
