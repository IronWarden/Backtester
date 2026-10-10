-- low_asset_growth.lua
-- Conservative investment: every `rebalance_days`, hold the `top_n` companies
-- whose total assets grew LEAST over their last fiscal year, in equal weight.
-- Reads SEC filings, so it works from 2011 on.
--
-- Params:
--   top_n           how many companies to hold        (default 10)
--   rebalance_days  trading days between re-rankings  (default 63, ~quarterly)
--   min_names       fewest scoreable companies worth ranking; below it the
--                   book is left alone                (default 5)
--
-- Growth compares the last two fiscal-year-end balance sheets, each as first
-- filed, so a company's growth does not change until its next 10-K.
--
-- @works: companies that grow their balance sheets fastest — acquisitions,
--   capex binges, issuing stock — go on to underperform (Cooper, Gulen and
--   Schill 2008); it is the investment factor in the Fama-French five-factor
--   model
-- @fails: growth that pays off — it avoids every company compounding assets
--   because its investments work, and a shrinking balance sheet can be
--   write-downs and distress rather than discipline
-- @sweep: top_n = [5, 10, 15], rebalance_days = [63, 126, 252]
-- @baseline: buy_and_hold.lua on the same tickers

fundamentals = {
    -- The reported figures this script reads. The engine loads them before
    -- the run starts, so fundamental() can return only these. Keep it a
    -- plain table: it is read, not run.
    source  = "sec",   -- SEC filings, 2010 on
    metrics = {
        "Total Assets",
    },
}

local top_n     = params.top_n or 10
local every     = params.rebalance_days or 63
local min_names = params.min_names or 5

-- Negative growth ranks highest, so rank() (strongest first) puts the most
-- conservative company first.
local function conservatism(t, day)
    local now  = fundamental(t, "Total Assets", day, "annual")
    local prev = fundamental(t, "Total Assets", day, "annual", 1)
    if now == nil or prev == nil or prev <= 0 then return nil end
    return -(now / prev - 1)
end

function step(day)
    if day % every ~= 0 then return end
    local ranked = rank(day, conservatism)
    if #ranked < min_names then return end
    local n = math.min(top_n, #ranked)
    local weights = {}
    for i = 1, n do weights[ranked[i].ticker] = 1.0 / n end
    target_weights(day, weights)
end
