-- bollinger_reversion.lua
-- Mean reversion on Bollinger Bands: buy when the close drops below the
-- lower band (moving average minus k standard deviations) and exit when it
-- climbs back to the average. The opposite bet to trend_following — it
-- profits in range-bound markets and bleeds in sustained downtrends.
--
-- The moving average comes from the Go sma() helper; the standard
-- deviation is computed here in Lua over the same window.
--
-- Params:
--   period   lookback for the average and the deviation  (default 20)
--   k        band width in standard deviations           (default 2.0)
--   buyType  "equalWeights" (default) | "greedy"
--
-- @works: the market oscillates around a stable average, so a band touch is
--   a stretch rather than a new trend
-- @fails: a sustained downtrend, where it buys each new low as the band
--   keeps moving down with the price
-- @sweep: period = [10, 20, 50], k = [1.5, 2.0, 2.5]
-- @baseline: buy_and_hold.lua, and rsi.lua as the other mean-reversion rule

local period  = params.period or 20
local k       = params.k or 2.0
local buyType = params.buyType or "equalWeights"

-- Population standard deviation of the closes over [day-period, day),
-- matching the window sma() averages.
local function stdev(t, day, mean)
    local sum = 0.0
    for i = day - period, day - 1 do
        local d = price(t, i) - mean
        sum = sum + d * d
    end
    return math.sqrt(sum / period)
end

function step(day)
    if day < period then return end
    for i = 1, #tickers do
        local t   = tickers[i]
        local px  = price(t, day)
        local ma  = sma(t, day, period)
        local pos = position(t)
        if px > 0 and ma > 0 then
            local sd = stdev(t, day, ma)
            if pos == nil and sd > 0 and px < ma - k * sd then
                buy_max(t, px, buyType, day)
            elseif pos ~= nil and px >= ma then
                sell_all(t, px, day)
            end
        end
    end
end
