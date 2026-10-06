-- gross_profitability.lua
-- Gross profitability (Novy-Marx): every `rebalance_days`, hold the `top_n`
-- companies with the most gross profit per dollar of assets, in equal weight.
-- Reads SEC filings, so it works from 2010 on.
--
-- Params:
--   top_n           how many companies to hold        (default 10)
--   rebalance_days  trading days between re-rankings  (default 63, ~quarterly)
--   min_names       fewest scoreable companies worth ranking; below it the
--                   book is left alone                (default 5)
--
-- Many filers never tag a gross profit line, so it is rebuilt as revenue minus
-- cost of revenue when missing. Banks and insurers have neither and are left
-- out — on a list that holds them, this is partly a sector bet.
--
-- @works: gross profit is the cleanest measure of how much a business earns
--   from what it sells, before accounting choices lower down the income
--   statement; Novy-Marx (2013) found it predicts returns about as strongly
--   as book-to-market
-- @fails: a high gross margin spent on overhead — it ignores SG&A and R&D, so
--   a company burning cash below the gross line still ranks well; and asset-
--   light companies win by construction, whatever their prospects
-- @sweep: top_n = [5, 10, 15], rebalance_days = [63, 126, 252]
-- @baseline: buy_and_hold.lua on the same tickers

fundamentals = {
    -- The reported figures this script reads. The engine loads them before
    -- the run starts, so fundamental() can return only these. Keep it a
    -- plain table: it is read, not run.
    source  = "sec",   -- SEC filings, 2010 on
    metrics = {
        "Gross Profit",
        "Total Revenue",
        "Cost Of Revenue",
        "Total Assets",
    },
}

local top_n     = params.top_n or 10
local every     = params.rebalance_days or 63
local min_names = params.min_names or 5

local function gross_profit(t, day)
    local gp = fundamental(t, "Gross Profit", day, "annual")
    if gp ~= nil then return gp end
    local rev  = fundamental(t, "Total Revenue", day, "annual")
    local cogs = fundamental(t, "Cost Of Revenue", day, "annual")
    if rev == nil or cogs == nil then return nil end
    return rev - cogs
end

local function profitability(t, day)
    local gp = gross_profit(t, day)
    local assets = fundamental(t, "Total Assets", day, "annual")
    if gp == nil or assets == nil or assets <= 0 then return nil end
    return gp / assets
end

function step(day)
    if day % every ~= 0 then return end
    local ranked = rank(day, profitability)
    if #ranked < min_names then return end
    local n = math.min(top_n, #ranked)
    local weights = {}
    for i = 1, n do weights[ranked[i].ticker] = 1.0 / n end
    target_weights(day, weights)
end
