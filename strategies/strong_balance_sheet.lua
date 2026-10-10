-- strong_balance_sheet.lua
-- Defensive: every `rebalance_days`, hold the `top_n` companies funded most by
-- their own equity rather than by debt — highest equity / total assets — among
-- those whose operations generated cash last year. Reads SEC filings, 2010 on.
--
-- Params:
--   top_n           how many companies to hold        (default 10)
--   rebalance_days  trading days between re-rankings  (default 63, ~quarterly)
--   min_names       fewest scoreable companies worth ranking; below it the
--                   book is left alone                (default 5)
--
-- Uses the newest balance sheet, quarterly included, so a company that takes on
-- debt drops out within a quarter of filing it. The cash-flow test is the full
-- fiscal year: a company with a solid balance sheet that is burning cash is not
-- defensive, it is running down its cushion.
--
-- @works: credit stress — selloffs that punish borrowers (2015-16 energy
--   debt, the 2022 rate shock) hit lightly-indebted companies least
-- @fails: long bull markets with cheap money, where leverage is rewarded and
--   these lag; and banks, insurers and utilities run on borrowed money by
--   design, so they never qualify — partly a sector bet
-- @sweep: top_n = [5, 10, 15], rebalance_days = [63, 126, 252]
-- @baseline: buy_and_hold.lua on the same tickers

fundamentals = {
    -- The reported figures this script reads. The engine loads them before
    -- the run starts, so fundamental() can return only these. Keep it a
    -- plain table: it is read, not run.
    source  = "sec",   -- SEC filings, 2010 on
    metrics = {
        "Stockholders Equity",
        "Total Assets",
        "Operating Cash Flow",
    },
}

local top_n     = params.top_n or 10
local every     = params.rebalance_days or 63
local min_names = params.min_names or 5

local function strength(t, day)
    local eq     = fundamental(t, "Stockholders Equity", day)
    local assets = fundamental(t, "Total Assets", day)
    local cfo    = fundamental(t, "Operating Cash Flow", day, "annual")
    if eq == nil or assets == nil or cfo == nil then return nil end
    if assets <= 0 or eq <= 0 or cfo <= 0 then return nil end
    return eq / assets
end

function step(day)
    if day % every ~= 0 then return end
    local ranked = rank(day, strength)
    if #ranked < min_names then return end
    local n = math.min(top_n, #ranked)
    local weights = {}
    for i = 1, n do weights[ranked[i].ticker] = 1.0 / n end
    target_weights(day, weights)
end
