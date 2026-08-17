-- buy_and_hold_weighted.lua
-- Allocates the initial capital across tickers by weight on day 0, then
-- holds to the end of the window. Weights are relative (they need not sum
-- to 1) — each ticker gets weight/sum(weights) of the starting cash.
--
-- Params:
--   weights = { TICKER = number, ... }   -- missing tickers default to 0
--
-- @works: you have a real view on relative sizing and want to hold it
--   without rebalancing
-- @fails: one name runs away and quietly becomes the whole book, so the
--   weights you chose stop being the weights you hold
-- @sweep: weights are a table, not a number, so sweep by writing several
--   [[portfolio]] blocks; rebalance.lua is the sweepable version
-- @baseline: buy_and_hold.lua on the same tickers — if equal weight wins,
--   the view was not worth the concentration

local weights = params.weights or {}

function step(day)
    if day ~= 0 then return end

    local total = 0.0
    for i = 1, #tickers do
        total = total + (weights[tickers[i]] or 0)
    end
    if total <= 0 then return end

    -- Snapshot starting cash once so each allocation is a fixed fraction of
    -- the initial capital rather than the dwindling balance.
    local capital = cash()
    for i = 1, #tickers do
        local t = tickers[i]
        local w = weights[t] or 0
        if w > 0 then
            local px = price(t, day)
            if px and px > 0 then
                -- Fractional shares: invest the whole weighted slice of capital
                -- rather than flooring to whole shares (which, for high-priced
                -- assets, could round down to zero and leave the slice in cash).
                local shares = (capital * (w / total)) / px
                if shares > 0 then
                    buy(t, shares, px, day)
                end
            end
        end
    end
end
