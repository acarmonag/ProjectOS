# ProjectOS — Real-Time Market Data Broker

A real-time market data broker built from scratch in Python using raw TCP sockets and a custom pub-sub architecture. Streams OHLCV (candlestick) data for 65+ currency pairs and commodities across 7 timeframes, with live visualization and technical indicators.

No external broker dependencies — no Kafka, no RabbitMQ, no Redis.

---

## Architecture

```
MonedasCSV/
(65 OHLCV files)
      │
      ▼
┌─────────────────────────────────────┐
│  market.py  (Producer)              │
│  Replays CSV files line-by-line     │
│  Spawns parallel MarketConsumer     │
│  processes per timeframe            │
└──────────────┬──────────────────────┘
               │  TCP push
               ▼
┌─────────────────────────────────────┐
│  broker.py  (TCP Broker)            │
│  Accepts producers + consumers      │
│  Routes data to subscribers         │
│  Thread-safe CSV cache writes       │
│  .cache/ ← runtime CSV output       │
└──────────────┬──────────────────────┘
               │  TCP subscribe
               ▼
┌─────────────────────────────────────┐
│  client.py  (Subscriber)            │
│  Live 3×3 candlestick grid          │
│  SMA(5) + SMA(13) per tick          │
│  Last 20 candles kept in memory     │
└─────────────────────────────────────┘
```

---

## Features

- **Custom pub-sub over TCP** — producers push data to the broker, consumers subscribe and receive routed updates. No external message broker required.
- **65+ instruments** — currency pairs and commodities across 7 timeframes: M1, M5, M15, M30, H1, H4, D1
- **Parallel timeframe streaming** — `market.py` spawns independent `MarketConsumer` processes per timeframe, enabling concurrent multi-timeframe feeds
- **Thread-safe writes** — concurrent producers write to `.cache/` CSV files with locks to prevent data corruption
- **Live candlestick UI** — `client.py` renders a 3×3 grid of live mplfinance charts, recalculating SMA(5) and SMA(13) on every incoming tick
- **Memory-bounded client** — only the last 20 candles are kept in memory per instrument to maintain UI responsiveness

---

## Stack

`Python 3` `socket` `threading` `pandas` `matplotlib` `mplfinance`

---

## Project structure

```
broker.py           # TCP broker — routes data between producers and consumers
market.py           # Producer — replays OHLCV CSV files line-by-line to broker
client.py           # Consumer — live candlestick grid with SMA indicators
MonedasCSV/         # 65 OHLCV data files across 7 timeframes (M1 → D1)
.cache/             # Runtime CSV output written by the broker
```

---

## Quickstart

**Start the broker:**
```bash
python broker.py
```

**Start a market producer** (replays CSV data into the broker):
```bash
python market.py
```

**Start the live client** (connects to broker and renders charts):
```bash
python client.py
```

All three components connect over localhost TCP. Start them in this order: broker → market → client.

---

## Key design decisions

**Raw TCP over a message broker** — intentionally avoids Kafka, RabbitMQ, or any external dependency. The broker implements the pub-sub pattern at the socket level, making the architecture transparent and portable.

**Process-per-timeframe parallelism** — each timeframe runs in its own `MarketConsumer` process rather than a thread, avoiding GIL contention when replaying multiple CSV streams simultaneously.

**Bounded client memory** — the client discards candles older than the last 20, keeping chart rendering fast regardless of session length.

**Lock-guarded CSV cache** — when multiple producers write concurrently, file writes are protected with threading locks to ensure consistent output in `.cache/`.

---

## What I'd improve in a production setup

- Replace the custom TCP pub-sub with a proper message broker (NATS or Redis Streams) for persistence and delivery guarantees
- Add a WebSocket layer so the client can run in a browser instead of a local matplotlib window
- Implement backpressure — currently fast producers can overwhelm slow consumers
- Add reconnection logic to the client and market producer on broker disconnect
- Stream real-time data from a market API instead of replaying CSV files
