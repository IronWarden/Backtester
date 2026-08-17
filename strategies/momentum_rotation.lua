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
--
-- Built on two primitives: rank(day, fn) scores every ticker and returns them
-- strongest first, and target_weights(day, targets) moves the book to those
-- fractions of equity, selling before buying. Both used to be hand-written here.

local lookback = params.lookback or 126
local skip     = params.skip_days or 0
local top_n    = params.top_n or 2
local every    = params.rebalance_days or 21

function step(day)
    if day < lookback + skip then return end
    if day % every ~= 0 then return end

    -- Trailing return measured to `skip` days ago, so the most recent stretch
    -- can be excluded (12-1 momentum). A ticker without enough history scores
    -- nil and is left out of the ranking rather than ranked last.
    local ranked = rank(day, function(t)
        local recent = price(t, day - skip)
        local past   = price(t, day - skip - lookback)
        if recent > 0 and past > 0 then
            return recent / past - 1
        end
        return nil
    end)

    -- Only the winners with positive momentum get capital; if none qualify
    -- every target is 0 and target_weights moves everything to cash.
    local n = math.min(top_n, #ranked)
    local winners = 0
    for i = 1, n do
        if ranked[i].value > 0 then winners = winners + 1 end
    end

    local targets = {}
    if winners > 0 then
        for i = 1, n do
            if ranked[i].value > 0 then
                targets[ranked[i].ticker] = 1.0 / winners
            end
        end
    end
    target_weights(day, targets)
end
