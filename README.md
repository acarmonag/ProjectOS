# ProjectOS — Real-Time Market Data Streaming

A broker-based system that streams OHLCV (candlestick) data for multiple currency pairs and commodities and visualises them live in a 3×3 chart grid with SMA overlays.

---

## Architecture

```
MarketConsumer(s)  ──►  Broker  ──►  Client
  (producers)        (TCP hub)    (subscriber + charts)
```

- **`broker.py`** — Central TCP broker. Accepts connections from producers and subscribers, caches every tick to CSV, and fans data out to all connected clients via per-client queues.
- **`market.py`** — Producer. Replays an OHLCV CSV file row by row (one tick per second) and streams each row to the broker.
- **`client.py`** — Subscriber. Renders a live 3×3 candlestick grid (up to 9 markets) with SMA(5) and SMA(13) overlays.
- **`config.py`** — Single source of truth for all shared constants (host, port, buffer size, chart settings, etc.).

The broker spawns all MarketConsumer subprocesses automatically — you only need to start the broker and the client.

---

## Requirements

- Python 3.9+
- `numpy`
- `matplotlib`
- `mplfinance`

---

## Setup

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

---

## Running

**Terminal 1 — start the broker:**
```bash
venv/bin/python3 broker.py
```
Select a timeframe period when prompted (e.g. `H1`). The broker spawns one MarketConsumer subprocess per matching CSV file automatically.

**Terminal 2 — start the client:**
```bash
venv/bin/python3 client.py
```
A matplotlib window opens and populates with candlestick charts as ticks arrive.

> The broker holds MarketConsumers in a ready state until the client connects, so start the client shortly after the broker.

---

## Data

CSV files live in `MonedasCSV/` and follow the naming convention `<MARKET>_<PERIOD>.csv` (e.g. `BTCUSD_H1.csv`).

Available periods: `M1`, `M5`, `M15`, `M30`, `H1`, `H4`, `D1`

Each file contains OHLCV rows with timestamps in `YYYY-MM-DD HH:MM` format.

---

## Cache

The broker writes three files to `caches/` at runtime. Stale files from a previous run are removed automatically on startup.

| File | Contents |
|---|---|
| `all_data.csv` | Every tick from every market, appended in arrival order |
| `<MARKET>.csv` | Ticks for a single market |
| `date_sorted.csv` | All ticks sorted by timestamp, rewritten on each tick |

---

## Configuration

All tuneable values live in `config.py` — one change applies everywhere.

| Constant | Default | Description |
|---|---|---|
| `HOST` / `PORT` | `localhost:54321` | Broker bind address |
| `BUFFER_SIZE` | `4096` | TCP receive buffer (bytes) |
| `TICK_INTERVAL` | `1.0` s | Delay between ticks (market.py) |
| `ALL_DATA_MAXLEN` | `100 000` | Max ticks kept in memory (broker) |
| `CLIENT_QUEUE_MAXSIZE` | `500` | Per-client send-queue depth before drops |
| `MAX_MARKETS` | `9` | Max charts in the grid |
| `MAX_CANDLES` | `20` | Rolling candle window per chart |
| `SMA_SHORT` / `SMA_LONG` | `5` / `13` | SMA periods |
| `RECONNECT_DELAY` | `5.0` s | Retry interval if broker drops |

---

## Changelog

### v3 — Performance pass

- **`config.py`** — all constants extracted to a single shared module; no more per-file duplication.
- **Numpy circular buffer** — client stores each market's data in a pre-allocated `(MAX_CANDLES, 7)` numpy array. New ticks overwrite the oldest slot (O(1), zero allocation). `pandas` is no longer needed in the client.
- **`np.convolve` SMA** — replaces `pandas .rolling().mean()`; pure numpy, no object overhead.
- **`canvas.draw_idle()`** — coalesces all per-market redraws within a `plt.pause()` cycle into one render pass, eliminating redundant full-figure redraws.
- **`bisect.insort`** — broker maintains `all_data` in sorted order via O(n) insertion instead of O(n log n) `.sort()` on every tick.
- **Lock scope reduced** — broker lock held only for fast in-memory pointer ops; all file I/O runs outside the lock so concurrent producers don't block each other.
- **Per-client send queue** — each client gets a bounded `queue.Queue`; the handler thread drains it independently. Slow clients drop ticks instead of stalling the entire pipeline.
- **`subprocess.Popen`** — market-consumer processes are fire-and-forget; no thread is wasted blocking on `subprocess.run`.
- **Partial-read buffering** in broker — ticks split across two `recv` calls are reassembled correctly.
- **`BUFFER_SIZE` raised to 4096** — fewer syscalls per tick.
- **`TCP_NODELAY`** on market-consumer sockets — disables Nagle's algorithm so each tick is transmitted immediately.
- **Cache cleared on startup** — stale CSV files from previous runs are removed automatically.
- **`ALL_DATA_MAXLEN` cap** — in-memory sorted list bounded at 100 000 entries to prevent OOM on long runs.

### v2 — Robustness pass

- **Logging** — all `print()` replaced with Python `logging`; timestamps and levels on every message.
- **Error handling** — `try/except` on all socket operations, file I/O, and tick parsing.
- **No busy-wait** — broker's `while not connected: pass` replaced with `threading.Event.wait()`.
- **Race condition fix** — client list copied under lock before broadcast; dead clients removed cleanly.
- **Reconnection** — client reconnects automatically if the broker drops.
- **Graceful shutdown** — all three processes handle `Ctrl+C` (SIGINT) cleanly.
- **Type hints & docstrings** — added to all classes and functions.
- **`sys.executable`** — broker spawns subprocesses using the active Python interpreter.
