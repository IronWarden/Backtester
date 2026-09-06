package backtest

// The Lua API, described well enough for an editor to teach it.
//
// A strategy script has 34 globals available and no way to discover them. The
// app's Lua tab is a plain text editor with syntax highlighting: nothing
// completes, nothing explains, and the only way to learn that the function is
// `sma(t, day, period)` and not `sma(t, period)` is to read
// strategies/README.md in another window. So the surface is described here
// once, in Go, and served to the editor -- typing becomes the way you find
// out what exists.
//
// This table is the ONLY hand-maintained copy. TestLuaAPICoversEveryGlobal
// scans the package for `L.SetGlobal("name", ...)` and fails if the two sets
// differ in either direction, so a global added to the engine cannot ship
// without its documentation and a documented name cannot outlive its global.
// That check is the whole reason this is a table rather than prose: the
// assistant's prompt drifted from the engine for several releases once
// already, and prose cannot be diffed against code.
//
// Signatures are the README's, verbatim in meaning: an editor that completes
// the wrong argument order is worse than one that completes nothing.

// LuaGlobal is one name a strategy script can use.
type LuaGlobal struct {
	// Name is the global itself; Signature is how it is called, with argument
	// names ("sma(ticker, day, period)"), or just the name for a value.
	Name      string `json:"name"`
	Signature string `json:"signature"`
	// Kind is "function" or "value", which is what decides whether an editor
	// completes it with parentheses.
	Kind string `json:"kind"`
	// Returns is what comes back, in a few words. Empty for the values and for
	// the functions that return nothing.
	Returns string `json:"returns"`
	// Summary is one sentence. It is what shows on hover, so it says what the
	// thing is FOR, and names the trap where there is one -- the fundamentals
	// returning nil rather than 0 is the difference between a working strategy
	// and one that trades on a figure it never had.
	Summary string `json:"summary"`
	// Group is the heading it belongs under, matching strategies/README.md so
	// the two read the same way.
	Group string `json:"group"`
}

// Groups, in the order a script needs them.
const (
	LuaGroupContext      = "Context"
	LuaGroupPrices       = "Prices"
	LuaGroupIndicators   = "Indicators"
	LuaGroupFundamentals = "Fundamentals"
	LuaGroupAccount      = "Account and orders"
)

// LuaAPI returns the whole surface, grouped and in a sensible reading order.
// A fresh slice each call: it crosses into the UI, and a shared backing array
// is the kind of thing that gets sorted in place by a caller one day.
func LuaAPI() []LuaGlobal {
	return append([]LuaGlobal(nil), luaAPI...)
}

var luaAPI = []LuaGlobal{
	// Context ---------------------------------------------------------------
	{
		Name: "tickers", Signature: "tickers", Kind: "value",
		Group:   LuaGroupContext,
		Summary: "1-indexed table of the portfolio's ticker strings, in config order.",
	},
	{
		Name: "params", Signature: "params", Kind: "value",
		Group:   LuaGroupContext,
		Summary: "Table of the [portfolio.Params] values from the config. A key the script never reads is reported as a warning, because in a sweep it means every trial ran identical settings.",
	},
	{
		Name: "num_days", Signature: "num_days()", Kind: "function",
		Group: LuaGroupContext, Returns: "number",
		Summary: "Trading days in this run. `day` is 0-based, so the last day is num_days() - 1.",
	},
	{
		Name: "log", Signature: "log(...)", Kind: "function",
		Group:   LuaGroupContext,
		Summary: "Say something the run console will show. Any number of values, joined with spaces like print.",
	},
	{
		Name: "plot", Signature: "plot(name, value)", Kind: "function",
		Group:   LuaGroupContext,
		Summary: "Record a number for today, drawn on the results panel against the same days as the equity curve — the way to SEE what the strategy is reacting to. One value per name per day (the last call wins), at most 8 names.",
	},

	// Prices ----------------------------------------------------------------
	{
		Name: "price", Signature: "price(ticker, day)", Kind: "function",
		Group: LuaGroupPrices, Returns: "number",
		Summary: "Closing price on that day, or 0 when the day is out of range. Same as close_at.",
	},
	{
		Name: "close_at", Signature: "close_at(ticker, day)", Kind: "function",
		Group: LuaGroupPrices, Returns: "number",
		Summary: "Closing price on that day, or 0 when the day is out of range.",
	},
	{
		Name: "open_at", Signature: "open_at(ticker, day)", Kind: "function",
		Group: LuaGroupPrices, Returns: "number",
		Summary: "That day's opening price, or 0 when the day is out of range.",
	},
	{
		Name: "high_at", Signature: "high_at(ticker, day)", Kind: "function",
		Group: LuaGroupPrices, Returns: "number",
		Summary: "That day's high, or 0 when the day is out of range.",
	},
	{
		Name: "low_at", Signature: "low_at(ticker, day)", Kind: "function",
		Group: LuaGroupPrices, Returns: "number",
		Summary: "That day's low, or 0 when the day is out of range.",
	},
	{
		Name: "volume_at", Signature: "volume_at(ticker, day)", Kind: "function",
		Group: LuaGroupPrices, Returns: "number",
		Summary: "That day's volume, or 0 when the day is out of range.",
	},
	{
		Name: "date_at", Signature: "date_at(ticker, day)", Kind: "function",
		Group: LuaGroupPrices, Returns: `"YYYY-MM-DD"`,
		Summary: `The calendar date of that trading day, or "" when the day is out of range.`,
	},

	// Indicators ------------------------------------------------------------
	{
		Name: "sma", Signature: "sma(ticker, day, period)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number",
		Summary: "Mean close over [day-period, day). Returns 0 without enough history, which is a value rather than an answer — check it before opening a position on a warm-up artifact.",
	},
	{
		Name: "rsi", Signature: "rsi(ticker, day, period)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number",
		Summary: "Wilder-style RSI. Returns 50 without enough history and 100 when there were no losses.",
	},
	{
		Name: "roc", Signature: "roc(ticker, day, n)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number, ok",
		Summary: "Trailing return over the n days ending at day — the building block of momentum and trend. The second return says whether there was enough history; ignoring it treats 'unknown' as 0.",
	},
	{
		Name: "stdev", Signature: "stdev(ticker, day, n)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number, ok",
		Summary: "Standard deviation of the n daily returns ending at day. NOT annualized — multiply by math.sqrt(252).",
	},
	{
		Name: "zscore", Signature: "zscore(ticker, day, n)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number, ok",
		Summary: "How far today's close sits from its n-day mean, in standard deviations of the close. Strongly negative is stretched to the downside.",
	},
	{
		Name: "atr", Signature: "atr(ticker, day, n)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number, ok",
		Summary: "Average true range in price units, for volatility-scaled sizing and stops that widen with the instrument.",
	},
	{
		Name: "high_n", Signature: "high_n(ticker, day, n)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number, ok",
		Summary: "Highest High over the n days ending at day, inclusive. A breakout channel without the loop.",
	},
	{
		Name: "low_n", Signature: "low_n(ticker, day, n)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number, ok",
		Summary: "Lowest Low over the n days ending at day, inclusive.",
	},
	{
		Name: "corr", Signature: "corr(tickerA, tickerB, day, n)", Kind: "function",
		Group: LuaGroupIndicators, Returns: "number, ok",
		Summary: "Pearson correlation of two tickers' daily returns over n days — for pairs, and for checking a diversified book is not one bet in five costumes.",
	},

	// Fundamentals ----------------------------------------------------------
	{
		Name: "fundamental", Signature: `fundamental(ticker, metric, day)`, Kind: "function",
		Group: LuaGroupFundamentals, Returns: "number or nil",
		Summary: `The newest figure PUBLISHED on or before that day, or nil. Returns nil rather than 0 on purpose: a company can genuinely report zero, so a 0 would be indistinguishable from "not published yet". Declare the metric in Fundamentals = [...] first or nothing is fetched.`,
	},
	{
		Name: "market_cap", Signature: "market_cap(ticker, day)", Kind: "function",
		Group: LuaGroupFundamentals, Returns: "number or nil",
		Summary: "Shares outstanding × close, or nil when the share count is missing or the close is not positive.",
	},

	// Account and orders ----------------------------------------------------
	{
		Name: "cash", Signature: "cash()", Kind: "function",
		Group: LuaGroupAccount, Returns: "number",
		Summary: "Uninvested buying power.",
	},
	{
		Name: "equity", Signature: "equity(day)", Kind: "function",
		Group: LuaGroupAccount, Returns: "number",
		Summary: "Cash plus every position marked at that day's close.",
	},
	{
		Name: "position", Signature: "position(ticker)", Kind: "function",
		Group: LuaGroupAccount, Returns: "{amount, avg_price, current_price} or nil",
		Summary: "The open position, or nil when flat. A negative amount is a short.",
	},
	{
		Name: "weight_of", Signature: "weight_of(ticker, day)", Kind: "function",
		Group: LuaGroupAccount, Returns: "number",
		Summary: "The fraction of equity this position currently represents, for checking drift.",
	},
	{
		Name: "buy", Signature: "buy(ticker, shares, price, day)", Kind: "function",
		Group:   LuaGroupAccount,
		Summary: "Buy an exact share count. Fractional shares are allowed.",
	},
	{
		Name: "buy_max", Signature: `buy_max(ticker, price, buyType, day)`, Kind: "function",
		Group:   LuaGroupAccount,
		Summary: `Size the order automatically. buyType is "equalWeights" (split cash across the portfolio's tickers) or "greedy" (pour all remaining cash in).`,
	},
	{
		Name: "sell", Signature: "sell(ticker, shares, price, day)", Kind: "function",
		Group:   LuaGroupAccount,
		Summary: "Sell an exact share count. With AllowShort, a sell past the holding closes the long and shorts the remainder; without it, such an order is refused.",
	},
	{
		Name: "sell_all", Signature: "sell_all(ticker, price, day)", Kind: "function",
		Group:   LuaGroupAccount,
		Summary: "Close the whole position whichever way it points — a short is closed by buying the shares back.",
	},
	{
		Name: "short", Signature: "short(ticker, shares, price, day)", Kind: "function",
		Group:   LuaGroupAccount,
		Summary: "Be short exactly `shares` after this call: a TARGET, not a delta, so it is correct without reading the current position first. short(t, 0, ...) goes flat. Needs AllowShort.",
	},
	{
		Name: "cover", Signature: "cover(ticker, price, day)", Kind: "function",
		Group:   LuaGroupAccount,
		Summary: "Buy the whole short back. A no-op on a long.",
	},
	{
		Name: "target_weights", Signature: "target_weights(day, weights)", Kind: "function",
		Group:   LuaGroupAccount,
		Summary: "Move the whole book to those fractions of equity: sells run first so their proceeds fund the buys. A ticker absent from the table is a target of ZERO, so target_weights(day, {}) goes to cash. A negative fraction is a short leg.",
	},
	{
		Name: "rank", Signature: "rank(day, fn)", Kind: "function",
		Group: LuaGroupAccount, Returns: "{ {ticker=, value=}, ... }",
		Summary: "Score every ticker with fn(ticker, day) and return them sorted strongest first. A ticker whose fn returns nil is left out rather than ranked last.",
	},
}
