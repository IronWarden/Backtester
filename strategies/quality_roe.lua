-- quality_roe.lua
-- Quality: every `rebalance_days`, hold the `top_n` companies earning the most
-- on their shareholders' money — latest full-year net income over average book
-- equity — in equal weight. Reads SEC filings, so it works from 2010 on.
--
-- Params:
--   top_n           how many companies to hold        (default 10)
--   rebalance_days  trading days between re-rankings  (default 63, ~quarterly)
--   min_names       fewest scoreable companies worth ranking; below it the
--                   book is left alone                (default 5)
--
-- Every figure is the one first filed with the SEC, read from the day after
-- the filing — never a later restatement, never before it was public. A company
-- with no figure yet is left out rather than scored as zero, and so is one with
-- negative equity: buybacks have pushed some (McDonald's since 2016) below zero,
-- where ROE stops meaning anything.
--
-- @works: highly profitable businesses keep earning high returns for years and
--   the market underprices that persistence — the quality factor
-- @fails: ROE is high because equity is tiny: leverage and heavy buybacks
--   shrink the denominator, so it drifts toward indebted companies; and in
--   junk rallies (2009, 2020-21) the least profitable names lead
-- @sweep: top_n = [5, 10, 15], rebalance_days = [63, 126, 252]
-- @baseline: buy_and_hold.lua on the same tickers — quality has to beat
--   simply owning all of them

fundamentals = {
    -- The reported figures this script reads. The engine loads them before
    -- the run starts, so fundamental() can return only these. Keep it a
    -- plain table: it is read, not run.
    source  = "sec",   -- SEC filings, 2010 on
    metrics = {
        "Net Income",
        "Stockholders Equity",
    },
}

local top_n     = params.top_n or 10
local every     = params.rebalance_days or 63
local min_names = params.min_names or 5

-- Net income over the average of this year's and last year's closing equity;
-- just this year's when there is no prior year yet. nil excludes the company.
local function roe(t, day)
    local ni = fundamental(t, "Net Income", day, "annual")
    local eq = fundamental(t, "Stockholders Equity", day, "annual")
    if ni == nil or eq == nil or eq <= 0 then return nil end
    local prev = fundamental(t, "Stockholders Equity", day, "annual", 1)
    if prev ~= nil and prev > 0 then eq = (eq + prev) / 2 end
    return ni / eq
end

function step(day)
    if day % every ~= 0 then return end
    local ranked = rank(day, roe)
    -- Ranking three names is not a screen. Keep what is held until enough
    -- companies have reported to compare.
    if #ranked < min_names then return end
    local n = math.min(top_n, #ranked)
    local weights = {}
    for i = 1, n do weights[ranked[i].ticker] = 1.0 / n end
    target_weights(day, weights)
end
