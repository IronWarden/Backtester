# The desktop app

The Wails (Go + React) front end to the backtesting engine. What it does is in
[docs/app.md](../docs/app.md); building it is in
[docs/installation.md](../docs/installation.md).

```bash
wails build -tags webkit2_41     # from ui/; the webkit tag is required on Linux
wails dev -tags webkit2_41       # live reload while developing
```

`wails dev` also serves the app at http://localhost:34115, so it can be driven
from a browser with the Go bindings working.
