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
