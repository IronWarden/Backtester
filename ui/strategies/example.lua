-- buy_and_hold.lua
-- Open an equal-weight position in every ticker on day 0 and hold it
-- to the end of the window.
--
-- Params:
--   buyType  "equalWeights" (default) | "greedy"

local buyType = params.buyType or "equalWeights"

function step(day)
    if day ~= 0 then return end
    for i = 1, #tickers do
        local t = tickers[i]
        buy_max(t, price(t, day), buyType, day)
    end
end