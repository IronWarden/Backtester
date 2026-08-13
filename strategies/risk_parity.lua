-- risk_parity.lua
-- Inverse-volatility weighting: every `rebalance_days`, size each ticker by
-- 1/volatility so quieter assets get more capital and each contributes a
-- similar share of portfolio risk. Most useful when blending assets whose
-- volatilities differ a lot — stocks with bonds, or $SP500 with $CASH.
--
-- This is the simple diagonal form of risk parity: it ignores correlations
-- and equalizes standalone risk contributions.
--
-- CAVEAT: weight is proportional to 1/volatility with no cap, so a very
-- quiet asset dominates. Pairing $SP500 with $CASH puts almost everything
-- in cash and produces a near-flat equity curve — that is the method
-- working as defined, not a bug. Use assets with comparable volatility
-- (e.g. equity indices with bond funds), or add a weight cap, if you want
-- a meaningful allocation to the riskier leg.
--
-- Params:
--   lookback        days used to measure volatility  (default 60)
--   rebalance_days  trading days between rebalances  (default 21, ~monthly)
--   min_vol         volatility floor, so a flat series cannot take an
--                   unbounded weight                 (default 0.0001)

local lookback = params.lookback or 60
local every    = params.rebalance_days or 21
local min_vol  = params.min_vol or 0.0001

-- Sample standard deviation of daily returns over the trailing window.
local function volatility(t, day)
    local rets, n, mean = {}, 0, 0.0
    for i = day - lookback + 1, day do
        local prev, cur = price(t, i - 1), price(t, i)
        if prev > 0 and cur > 0 then
            n = n + 1
            rets[n] = cur / prev - 1
            mean = mean + rets[n]
        end
    end
    if n < 2 then return 0 end
    mean = mean / n
    local sum = 0.0
    for i = 1, n do
        local d = rets[i] - mean
        sum = sum + d * d
    end
    return math.sqrt(sum / (n - 1))
end

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
    if day < lookback + 1 then return end
    if day % every ~= 0 then return end

    local inv, total = {}, 0.0
    for i = 1, #tickers do
        local t = tickers[i]
        if price(t, day) > 0 then
            local v = volatility(t, day)
            if v < min_vol then v = min_vol end
            inv[t] = 1.0 / v
            total = total + inv[t]
        end
    end
    if total <= 0 then return end

    local targets = {}
    for t, w in pairs(inv) do
        targets[t] = w / total
    end
    rebalance_to(day, targets)
end
