package backtest

// Somewhere to start.
//
// The Lua tab opens on a blank scratch file, which is no help at all to
// someone who wants to write their own idea, because it shows no shape and
// never says there are several. The library has twelve
// scripts to crib from, but reading a finished strategy to learn its skeleton
// means separating the shape from the idea, which is exactly the thing a
// beginner cannot do yet.
//
// These are shapes with the idea removed. Each one runs as written -- a
// skeleton that does not run teaches a person that they broke it -- and each
// has one TODO where the idea goes.
//
// They live in Go rather than in strategies/ because that directory is the
// gallery: everything in it is listed as a strategy to run, and requires the
// four header tags. A skeleton is neither.

// LuaSkeleton is one starting shape for a strategy.
type LuaSkeleton struct {
	// Name is the label on the button; Summary is the sentence under it,
	// saying what kind of idea this shape is for.
	Name    string `json:"name"`
	Summary string `json:"summary"`
	// Source is the whole file, ready to save and run.
	Source string `json:"source"`
}

// LuaSkeletons returns the starting shapes, in the order they should be
// offered: the one most strategies are, then the two that are not.
func LuaSkeletons() []LuaSkeleton {
	return append([]LuaSkeleton(nil), luaSkeletons...)
}

var luaSkeletons = []LuaSkeleton{
	{
		Name:    "A signal, per ticker",
		Summary: "Decide about each holding on its own — a moving average, an RSI level, a breakout. Most strategies are this shape.",
		Source: `-- A signal, per ticker.
--
-- step(day) runs once per trading day, day counting from 0. This shape looks
-- at each holding on its own and decides whether to be in it.

function step(day)
  for i = 1, #tickers do
    local t = tickers[i]
    local px = price(t, day)
    if px > 0 then
      -- TODO: your signal. This one is "above its own 50-day average".
      local avg = sma(t, day, 50)
      local want_in = avg > 0 and px > avg

      -- plot() draws a number under the results, on the same days as the
      -- equity curve. It is the fastest way to see what the signal is doing.
      if i == 1 then
        plot("price", px)
        plot("average", avg)
      end

      local held = position(t)
      if want_in and not held then
        buy_max(t, px, "equalWeights", day)
      elseif held and not want_in then
        sell_all(t, px, day)
      end
    end
  end
end
`,
	},
	{
		Name:    "Rebalance on a schedule",
		Summary: "Hold everything and put the weights back where you want them every so often. No signal, no timing — the shape a lot of real portfolios are.",
		Source: `-- Rebalance on a schedule.
--
-- No signal and no timing: hold the whole list, and every so often put the
-- weights back where they should be. target_weights does the arithmetic --
-- it sells first so the proceeds fund the buys.

function step(day)
  -- TODO: how often, in trading days. 21 is about a month, 63 a quarter.
  local every = params.rebalance_days or 21

  if day % every ~= 0 then return end

  local weights = {}
  for i = 1, #tickers do
    -- TODO: your weights. Equal is the honest default; anything else is a
    -- claim that you know which of these deserves more.
    weights[tickers[i]] = 1.0 / #tickers
  end
  target_weights(day, weights)

  plot("holdings", #tickers)
end
`,
	},
	{
		Name:    "Rank and hold the top few",
		Summary: "Score every ticker against the others and hold the best ones — momentum, value, low volatility. The shape that needs a whole universe rather than a handful.",
		Source: `-- Rank and hold the top few.
--
-- rank(day, fn) scores every ticker with your function and returns them
-- sorted strongest first. A ticker your function returns nil for is left out
-- rather than ranked last, which is what you want for one with no history yet.

function step(day)
  local every = params.rebalance_days or 21
  local top_n = params.top_n or 3

  if day % every ~= 0 then return end

  local ranked = rank(day, function(t, d)
    -- TODO: your score. Higher is better. This one is 3-month momentum.
    local r, ok = roc(t, d, 63)
    if not ok then return nil end   -- not enough history: leave it out
    return r
  end)

  local weights = {}
  for i = 1, math.min(top_n, #ranked) do
    weights[ranked[i].ticker] = 1.0 / top_n
  end
  -- A ticker absent from the table is a target of zero, so everything that
  -- fell out of the top N is sold by this one call.
  target_weights(day, weights)

  if #ranked > 0 then
    plot("best score", ranked[1].value)
  end
end
`,
	},
}
