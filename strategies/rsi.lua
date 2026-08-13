-- rsi.lua
-- Mean-reversion: buy a ticker when its RSI dips below `buy_thresh` and
-- there is no open position; flatten when RSI climbs back above
-- `sell_thresh`. Indicator math runs in Go.
--
-- Params:
--   period       lookback for RSI         (default 14)
--   buy_thresh   oversold threshold       (default 30)
--   sell_thresh  overbought threshold     (default 70)
--   buyType      "equalWeights" (default) | "greedy"

local period      = params.period      or 14
local buy_thresh  = params.buy_thresh  or 30
local sell_thresh = params.sell_thresh or 70
local buyType     = params.buyType     or "equalWeights"

function step(day)
    if day <= period then return end
    for i = 1, #tickers do
        local t = tickers[i]
        local r = rsi(t, day, period)
        local p = price(t, day)
        local pos = position(t)
        if r < buy_thresh and pos == nil then
            buy_max(t, p, buyType, day)
        elseif r > sell_thresh and pos ~= nil then
            sell_all(t, p, day)
        end
    end
end
