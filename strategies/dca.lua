-- dca.lua
-- Dollar-cost averaging: invest a fixed dollar amount on a fixed cadence,
-- split equally across the tickers, and never sell. The natural baseline
-- for "should I have invested it all at once?" comparisons against
-- buyAndHold:equalWeights.
--
-- NOTE: the engine models no external cash inflows, so contributions come
-- out of the starting balance — this spreads the initial capital into the
-- market over time rather than adding new money. To be roughly fully
-- invested by the end, set `amount` to BuyingPower divided by the number
-- of planned buys (≈ trading days in the window / every_days).
--
-- Params:
--   amount      dollars to deploy each cadence  (default 1000)
--   every_days  trading days between buys       (default 21, ~monthly)

local amount = params.amount or 1000
local every  = params.every_days or 21

function step(day)
    if day % every ~= 0 then return end

    local budget = math.min(amount, cash())
    if budget <= 0 then return end

    local slice = budget / #tickers
    for i = 1, #tickers do
        local t, px = tickers[i], price(tickers[i], day)
        if px > 0 then
            local shares = slice / px
            if shares > 0 then
                buy(t, shares, px, day)
            end
        end
    end
end
