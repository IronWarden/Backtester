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
--
-- Uses the built-in target_weights(day, targets), which sells before it buys so
-- the proceeds fund the purchases. That used to be forty hand-written lines in
-- this file and in momentum_rotation.lua; both now call the same primitive.

local every   = params.rebalance_days or 63
local weights = params.weights

function step(day)
    if day % every ~= 0 then return end

    local targets = {}
    if weights then
        -- Weights are relative, so normalise them to fractions of equity.
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
    target_weights(day, targets)
end
