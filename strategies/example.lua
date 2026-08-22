-- example.lua
-- The starter script. The app's Lua tab opens this file, and it is yours to
-- edit: it ships as the simplest complete strategy — open an equal-weight
-- position in every ticker on day 0 and hold it to the end of the window —
-- so there is always something runnable on screen to change one line of.
--
-- Params:
--   buyType  "equalWeights" (default) | "greedy"
--
-- @works: whenever buy_and_hold.lua does, since that is what this is — the
--   assets rise over your window
-- @fails: as a strategy, whenever buy_and_hold.lua fails; as a starting
--   point, never — edit it freely, buy_and_hold.lua is the copy to preserve
-- @sweep: nothing numeric to sweep; vary the UNIVERSE instead, which is what
--   actually decides this one
-- @baseline: buy_and_hold.lua, which is this file plus a library header

local buyType = params.buyType or "equalWeights"

function step(day)
    if day ~= 0 then return end
    for i = 1, #tickers do
        local t = tickers[i]
        buy_max(t, price(t, day), buyType, day)
    end
end
