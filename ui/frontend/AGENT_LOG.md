
## T83 — Put the hypothesis in front of the run, in both modes
- **Status:** done
- **Commit:** (below)
- **What:** A "Before you run" card in simple mode: a plain-English claim plus
  a metric/operator/number criterion, rendered into the same TOML the engine
  registers. `App.CriterionMetrics` binds `backtest.CriterionMetricNames` so
  the picker cannot offer a criterion `ParseCriterion` rejects. The `ID` is a
  hash of the question, so editing the claim or threshold registers a new
  question instead of silently re-judging the old one. An empty claim still
  runs unjudged. Advanced mode's raw box is untouched, and "Edit as TOML"
  carries the question over.
- **Notes:** My first hash implementation was wrong and my own check caught it:
  I took `toString(36).slice(0, 5)`, but base36 puts the fastest-changing
  digits LAST, so `sharpe >= 1.2` and `sharpe >= 1.3` produced the same id —
  two different questions merging into one in the research log. Fixed to use
  the whole hash, and the property is asserted both ways (same question keeps
  its id, changed criterion gets a new one).
  Verified against the engine rather than in the abstract: the generated
  documents were captured from the compiled module and fed to
  `ParseHypothesis` in a new `ui/hypothesis_test.go` (including a claim
  containing quotes and a negative threshold), and a real run with one of them
  produced a verdict — `hypothesis H-11eyu7q — rejected, sharpe >= 1.2 —
  measured 0.8415`. That test pins the format the engine accepts; the
  generator's own behaviour still needs T87.
