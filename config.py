"""
Shared configuration constants for the ProjectOS market data streaming system.

All tuneable values live here — broker.py, market.py, and client.py import
from this module so there is a single source of truth.
"""

from typing import List, Tuple

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------
HOST: str = "localhost"
PORT: int = 54321
BUFFER_SIZE: int = 4096          # increased from 1024 — fewer syscalls per tick

# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------
CACHE_DIR: str = "caches"
CSV_DATA_DIR: str = "MonedasCSV"
MAX_CONNECTIONS: int = 10
PERIODS: List[str] = ["D1", "H1", "H4", "M1", "M15", "M30", "M5"]
ALL_DATA_MAXLEN: int = 100_000   # cap in-memory sorted tick list to prevent OOM
CLIENT_QUEUE_MAXSIZE: int = 500  # per-client send-queue depth; excess ticks dropped

# ---------------------------------------------------------------------------
# MarketConsumer
# ---------------------------------------------------------------------------
TICK_INTERVAL: float = 1.0       # seconds between successive ticks

# ---------------------------------------------------------------------------
# Client / charts
# ---------------------------------------------------------------------------
MAX_MARKETS: int = 9             # maximum subplots shown (3×3 grid)
MAX_CANDLES: int = 20            # rolling candle window per chart
SMA_SHORT: int = 5
SMA_LONG: int = 13
GRID_ROWS: int = 3
GRID_COLS: int = 3
CHART_FIGSIZE: Tuple[int, int] = (15, 15)
PAUSE_INTERVAL: float = 0.1      # seconds between matplotlib event-loop ticks
RECONNECT_DELAY: float = 5.0     # seconds before a reconnect attempt
