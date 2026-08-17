-- trend_following.lua
-- Absolute (time-series) momentum, decided per ticker: hold it while its
-- close is above its own moving average, sit in cash while it is below.
-- A well-known way to cut deep drawdowns, paid for with whipsaw losses in
-- choppy markets.
--
-- Params:
--   period      moving-average lookback in days   (default 200)
--   buffer_pct  percent the close must clear the average by before acting
--               (default 0; ~1.0 trades less and whipsaws less around the
--               line, at the cost of entering and exiting later)
--   buyType     "equalWeights" (default) | "greedy"
--
-- @works: prices trend persistently above or below a long average
-- @fails: chop around the average, where the buffer is crossed repeatedly
--   and each crossing is a trade
-- @sweep: period = [50, 100, 200], buffer_pct = [0.0, 0.01, 0.03]
-- @baseline: buy_and_hold.lua, and sma_cross.lua for whether the second
--   average was worth it

local period  = params.period or 200
local buffer  = (params.buffer_pct or 0) / 100.0
local buyType = params.buyType or "equalWeights"

function step(day)
    if day < period then return end
    for i = 1, #tickers do
        local t   = tickers[i]
        local px  = price(t, day)
        local ma  = sma(t, day, period)
        local pos = position(t)
        if px > 0 and ma > 0 then
            if pos == nil and px > ma * (1 + buffer) then
                buy_max(t, px, buyType, day)
            elseif pos ~= nil and px < ma * (1 - buffer) then
                sell_all(t, px, day)
            end
        end
    end
end
