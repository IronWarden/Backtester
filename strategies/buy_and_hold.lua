-- buy_and_hold.lua
-- Open an equal-weight position in every ticker on day 0 and hold it
-- to the end of the window.
--
-- Params:
--   buyType  "equalWeights" (default) | "greedy"
--
-- @works: the asset rises over your window — the default answer, and the
--   one most strategies fail to beat
-- @fails: you cannot sit through the drawdown it takes on the way; it has
--   no exit and never goes to cash
-- @sweep: nothing numeric to sweep; vary the UNIVERSE instead, which is
--   what actually decides this one
-- @baseline: itself: this IS the baseline every other strategy is measured
--   against, and the Benchmark for the market comparison

local buyType = params.buyType or "equalWeights"

function step(day)
    if day ~= 0 then return end
    for i = 1, #tickers do
        local t = tickers[i]
        buy_max(t, price(t, day), buyType, day)
    end
end
