-- rebalance.lua
-- Hold a fixed target allocation and rebalance back to it on a cadence,
-- trimming what has run up and topping up what has lagged. Run it against
-- buyAndHold:equalWeights over the same tickers to see whether rebalancing
-- helped or hurt over your window.
--
-- Params:
--   rebalance_days  trading days between rebalances (default 63, ~quarterly)
--   weights         optional { TICKER = number } target weights; they are
--                   relative and need not sum to 1. Omit for equal weight.

local every   = params.rebalance_days or 63
local weights = params.weights

local function equity(day)
    local total = cash()
    for i = 1, #tickers do
        local pos = position(tickers[i])
        if pos then
            total = total + pos.amount * price(tickers[i], day)
        end
    end
    return total
end

-- Sells first, then buys, so proceeds fund the purchases.
local function rebalance_to(day, targets)
    local eq = equity(day)
    for i = 1, #tickers do
        local t, px = tickers[i], price(tickers[i], day)
        if px > 0 then
            local pos = position(t)
            local held = pos and pos.amount or 0
            local want = (eq * (targets[t] or 0)) / px
            if held - want > 1e-9 then
                sell(t, held - want, px, day)
            end
        end
    end
    for i = 1, #tickers do
        local t, px = tickers[i], price(tickers[i], day)
        if px > 0 then
            local pos = position(t)
            local held = pos and pos.amount or 0
            local want = (eq * (targets[t] or 0)) / px
            local short = want - held
            local affordable = cash() / px
            if short > affordable then short = affordable end
            if short > 1e-9 then
                buy(t, short, px, day)
            end
        end
    end
end

function step(day)
    if day % every ~= 0 then return end

    local targets = {}
    if weights then
        local total = 0.0
        for i = 1, #tickers do
            total = total + (weights[tickers[i]] or 0)
        end
        if total <= 0 then return end
        for i = 1, #tickers do
            targets[tickers[i]] = (weights[tickers[i]] or 0) / total
        end
    else
        for i = 1, #tickers do
            targets[tickers[i]] = 1.0 / #tickers
        end
    end
    rebalance_to(day, targets)
end
