-- piotroski_fscore.lua
-- Piotroski F-score: every `rebalance_days`, score each company 0-8 on whether
-- its latest annual report shows it getting healthier, and hold up to
-- `max_names` scoring at least `min_score`, in equal weight. Cash if none do.
--
-- Params:
--   min_score       points needed to be held, of 8     (default 7)
--   max_names       most companies held at once        (default 10)
--   rebalance_days  trading days between re-scorings   (default 63, ~quarterly)
--
-- One point each, comparing the latest fiscal year with the one before:
--   profitable           net income / opening assets > 0
--   cash-generative      operating cash flow > 0
--   improving            return on assets rose
--   earnings are cash    operating cash flow > net income
--   deleveraging         long-term debt / assets did not rise
--   more liquid          current ratio rose
--   better margins       gross margin rose
--   more productive      revenue / opening assets rose
--
-- Piotroski's ninth test, "issued no new shares", is left out: SEC share
-- counts are as reported, so every stock split would read as a huge share
-- issue. A test whose figures are missing scores 0 — a company has to show its
-- work. Needs three annual balance sheets, so a company scores from its third
-- XBRL 10-K (2011-12 for most).
--
-- @works: cheap or neglected companies whose fundamentals are quietly
--   improving — Piotroski (2000) built it to separate winners from losers
--   among value stocks, where it does most of its work
-- @fails: on a list of large, healthy companies most score 5-7 and it sorts
--   noise; and the signals are year-over-year changes, so a company that is
--   excellent but stable earns fewer points than a mediocre one recovering
-- @sweep: min_score = [6, 7, 8], max_names = [5, 10], rebalance_days = [63, 252]
-- @baseline: buy_and_hold.lua on the same tickers

fundamentals = {
    -- The reported figures this script reads. The engine loads them before
    -- the run starts, so fundamental() can return only these. Keep it a
    -- plain table: it is read, not run.
    source  = "sec",   -- SEC filings, 2010 on
    metrics = {
        "Net Income",
        "Operating Cash Flow",
        "Total Assets",
        "Long Term Debt",
        "Current Assets",
        "Current Liabilities",
        "Gross Profit",
        "Total Revenue",
        "Cost Of Revenue",
    },
}

local min_score = params.min_score or 7
local max_names = params.max_names or 10
local every     = params.rebalance_days or 63

-- Latest fiscal year (back = 0) or an earlier one.
local function annual(t, metric, day, back)
    return fundamental(t, metric, day, "annual", back)
end

local function gross_margin(t, day, back)
    local rev = annual(t, "Total Revenue", day, back)
    if rev == nil or rev <= 0 then return nil end
    local gp = annual(t, "Gross Profit", day, back)
    if gp == nil then
        local cogs = annual(t, "Cost Of Revenue", day, back)
        if cogs == nil then return nil end
        gp = rev - cogs
    end
    return gp / rev
end

local function ratio(a, b)
    if a == nil or b == nil or b <= 0 then return nil end
    return a / b
end

-- 1 when the comparison holds, 0 when it fails or cannot be made.
local function point(ok) if ok then return 1 end return 0 end

local function fscore(t, day)
    local ni0  = annual(t, "Net Income", day, 0)
    local ni1  = annual(t, "Net Income", day, 1)
    local ta0  = annual(t, "Total Assets", day, 0)
    local ta1  = annual(t, "Total Assets", day, 1)
    local ta2  = annual(t, "Total Assets", day, 2)
    local cfo0 = annual(t, "Operating Cash Flow", day, 0)
    -- Without this year's earnings and opening assets there is nothing to
    -- score, and the company is left out rather than given 0.
    local roa0 = ratio(ni0, ta1)
    if roa0 == nil then return nil end
    local roa1 = ratio(ni1, ta2)

    local lev0 = ratio(annual(t, "Long Term Debt", day, 0), ta0)
    local lev1 = ratio(annual(t, "Long Term Debt", day, 1), ta1)
    local cr0  = ratio(annual(t, "Current Assets", day, 0),
                       annual(t, "Current Liabilities", day, 0))
    local cr1  = ratio(annual(t, "Current Assets", day, 1),
                       annual(t, "Current Liabilities", day, 1))
    local gm0  = gross_margin(t, day, 0)
    local gm1  = gross_margin(t, day, 1)
    local at0  = ratio(annual(t, "Total Revenue", day, 0), ta1)
    local at1  = ratio(annual(t, "Total Revenue", day, 1), ta2)

    return point(roa0 > 0)
         + point(cfo0 ~= nil and cfo0 > 0)
         + point(roa1 ~= nil and roa0 > roa1)
         + point(cfo0 ~= nil and cfo0 > ni0)
         + point(lev0 ~= nil and lev1 ~= nil and lev0 <= lev1)
         + point(cr0 ~= nil and cr1 ~= nil and cr0 > cr1)
         + point(gm0 ~= nil and gm1 ~= nil and gm0 > gm1)
         + point(at0 ~= nil and at1 ~= nil and at0 > at1)
end

function step(day)
    if day % every ~= 0 then return end
    local ranked = rank(day, fscore)
    -- Before anything has been filed there is no score at all; that is not a
    -- verdict to sell on.
    if #ranked == 0 then return end
    local hold = {}
    for i = 1, #ranked do
        if ranked[i].value < min_score or #hold >= max_names then break end
        hold[#hold + 1] = ranked[i].ticker
    end
    local weights = {}
    for _, t in ipairs(hold) do weights[t] = 1.0 / #hold end
    target_weights(day, weights)
end
