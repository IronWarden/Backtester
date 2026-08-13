-- momentum_rotation.lua
-- Cross-sectional momentum: every `rebalance_days`, rank the tickers by
-- their trailing return and hold the top `top_n` in equal weight, selling
-- everything else. Names with negative momentum are skipped, so a
-- market-wide downturn moves the book to cash.
--
-- Params:
--   lookback        trading days of trailing return  (default 126, ~6 months)
--   skip_days       days skipped before measuring    (default 0; set 21 for
--                   the academic "12-1" momentum that ignores last month)
--   top_n           how many tickers to hold         (default 2)
--   rebalance_days  trading days between rebalances  (default 21, ~monthly)

local lookback = params.lookback or 126
local skip     = params.skip_days or 0
local top_n    = params.top_n or 2
local every    = params.rebalance_days or 21

-- Total account value: cash plus the marked-to-market positions.
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

-- Move the book to `targets` (ticker -> fraction of equity). Sells run in a
-- first pass so their proceeds are available to the buys in the second.
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
    if day < lookback + skip then return end
    if day % every ~= 0 then return end

    local ranked = {}
    for i = 1, #tickers do
        local t = tickers[i]
        local recent = price(t, day - skip)
        local past   = price(t, day - skip - lookback)
        if recent > 0 and past > 0 then
            ranked[#ranked + 1] = { ticker = t, ret = recent / past - 1 }
        end
    end
    table.sort(ranked, function(a, b) return a.ret > b.ret end)

    -- Only the winners with positive momentum get capital; if none
    -- qualify every target is 0 and rebalance_to moves everything to cash.
    local n = math.min(top_n, #ranked)
    local winners = 0
    for i = 1, n do
        if ranked[i].ret > 0 then winners = winners + 1 end
    end

    local targets = {}
    if winners > 0 then
        for i = 1, n do
            if ranked[i].ret > 0 then
                targets[ranked[i].ticker] = 1.0 / winners
            end
        end
    end
    rebalance_to(day, targets)
end
