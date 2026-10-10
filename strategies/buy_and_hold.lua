-- buy_and_hold.lua
-- Buy every ticker on day 0 and hold it to the end of the window, either
-- equally or at the weights you give. Scheduled deposits are invested at
-- that day's close, split the same way.
--
-- Params:
--   weights  { TICKER = number, ... }  (optional) relative weights; they
--            need not sum to 1, and a ticker left out gets nothing.
--            Without it every ticker gets an equal share.
--   buyType  "equalWeights" (default) | "greedy" -- only read without
--            weights; greedy pours everything into the tickers in order
--
-- @works: the asset rises over your window — the default answer, and the
--   one most strategies fail to beat
-- @fails: you cannot sit through the drawdown it takes on the way; it has
--   no exit and never goes to cash. With weights, one name can run away and
--   quietly become the whole book
-- @sweep: nothing numeric to sweep; vary the UNIVERSE instead, which is
--   what actually decides this one. rebalance.lua is the version that holds
--   its weights
-- @baseline: itself: this IS the baseline every other strategy is measured
--   against, and the Benchmark for the market comparison

local weights = params.weights
local buyType = params.buyType or "equalWeights"

-- Each ticker's share of new money, fixed for the whole run.
local shares = {}
if weights then
    local total = 0.0
    for i = 1, #tickers do
        total = total + (weights[tickers[i]] or 0)
    end
    for i = 1, #tickers do
        local t = tickers[i]
        shares[t] = total > 0 and (weights[t] or 0) / total or 0
    end
end

-- invest spends `amount` across the tickers. Every leg is sized against
-- `amount` as it stood before any of them filled: reading the live balance
-- would shrink each leg in turn and strand part of the money in cash.
-- buy_max sizes against the real fill price, so slippage cannot push the
-- last leg over the balance.
local function invest(amount, day)
    if weights then
        for i = 1, #tickers do
            local t = tickers[i]
            if shares[t] > 0 then
                buy_max(t, price(t, day), "greedy", day, amount * shares[t])
            end
        end
    elseif buyType == "greedy" then
        -- Pours what is left of `amount` into each ticker in order, and
        -- never reaches into cash that `amount` did not provide.
        local left = amount
        for i = 1, #tickers do
            local t = tickers[i]
            local before = cash()
            buy_max(t, price(t, day), "greedy", day, left)
            left = left - (before - cash())
        end
    else
        for i = 1, #tickers do
            local t = tickers[i]
            buy_max(t, price(t, day), "equalWeights", day, amount)
        end
    end
end

function step(day)
    if day == 0 then
        invest(cash(), day)
        return
    end
    -- A [portfolio.Cashflow] deposit. Only the deposit is spent, never other
    -- idle cash; withdrawals are funded by the engine before this runs.
    local flow = cashflow()
    if flow > 0 then
        invest(flow, day)
    end
end
