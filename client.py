"""
Client module for the ProjectOS market data streaming system.

Connects to the broker as a subscriber and renders a live 3×3 grid of
candlestick charts — one subplot per market — with SMA overlays.
Reconnects automatically if the broker drops.

Performance notes
-----------------
- Market data is stored in a pre-allocated numpy circular buffer per market.
  Each tick overwrites one slot (O(1)) instead of creating a new DataFrame
  with pd.concat on every update.
- SMA is computed with np.convolve directly on the numpy array — no pandas
  rolling needed.
- ``canvas.draw_idle()`` coalesces multiple redraw requests within a single
  ``plt.pause()`` cycle, eliminating redundant full-figure redraws when
  several markets arrive in the same batch.
"""

import logging
import signal
import socket
import sys
from datetime import datetime
from typing import Dict

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from mplfinance.original_flavor import candlestick_ohlc

from config import (
    HOST, PORT, BUFFER_SIZE,
    MAX_MARKETS, MAX_CANDLES, SMA_SHORT, SMA_LONG,
    GRID_ROWS, GRID_COLS, CHART_FIGSIZE,
    PAUSE_INTERVAL, RECONNECT_DELAY,
)

# ---------------------------------------------------------------------------
# Array column indices
# ---------------------------------------------------------------------------
_COL_INDEX  = 0
_COL_DATE   = 1
_COL_OPEN   = 2
_COL_HIGH   = 3
_COL_LOW    = 4
_COL_CLOSE  = 5
_COL_VOLUME = 6
_N_COLS     = 7

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class Client:
    """Subscriber client that visualises live market ticks as candlestick charts.

    Each market's data lives in a pre-allocated numpy circular buffer of shape
    ``(MAX_CANDLES, 7)``.  A write pointer wraps around the buffer so new
    candles overwrite the oldest slot — no memory allocation on the hot path.

    Up to *MAX_MARKETS* markets are displayed simultaneously.  The connection
    is re-established automatically if the broker becomes unavailable.
    """

    def __init__(
        self, broker_host: str = HOST, broker_port: int = PORT
    ) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port

        # Circular buffer storage — one numpy array per market.
        self.market_arrays: Dict[str, np.ndarray] = {}
        self.market_pos:    Dict[str, int]         = {}  # ever-incrementing write ptr
        self.market_count:  Dict[str, int]         = {}  # candles received (capped)

        self.ax_dict: Dict[str, plt.Axes] = {}
        self.fig, self.axes = plt.subplots(
            nrows=GRID_ROWS, ncols=GRID_COLS, figsize=CHART_FIGSIZE
        )
        plt.ion()
        self._running = True

        def _signal_handler(sig, frame) -> None:
            logger.info("SIGINT received — closing client.")
            self._running = False
            plt.close("all")
            sys.exit(0)

        signal.signal(signal.SIGINT, _signal_handler)

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect_to_broker(self) -> None:
        """Connect to the broker, reconnecting automatically on failure."""
        while self._running:
            try:
                self._run_session()
            except ConnectionRefusedError:
                logger.warning(
                    "Broker unavailable at %s:%s. Retrying in %.0fs…",
                    self.broker_host, self.broker_port, RECONNECT_DELAY,
                )
                plt.pause(RECONNECT_DELAY)
            except OSError as exc:
                logger.warning(
                    "Connection lost: %s. Retrying in %.0fs…", exc, RECONNECT_DELAY
                )
                plt.pause(RECONNECT_DELAY)

    def _run_session(self) -> None:
        """Open one broker session and process ticks until disconnected."""
        logger.info(
            "Connecting to broker at %s:%s…", self.broker_host, self.broker_port
        )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_host, self.broker_port))
            logger.info("Connected to broker.")
            s.sendall("CLIENT".encode())

            incomplete_data = ""
            while self._running:
                try:
                    chunk = s.recv(BUFFER_SIZE).decode()
                except OSError as exc:
                    logger.warning("Receive error: %s", exc)
                    break

                if not chunk:
                    logger.info("Broker closed the connection.")
                    break

                chunk = incomplete_data + chunk
                messages = chunk.split("\n")
                incomplete_data = messages.pop()   # last element may be partial

                for message in messages:
                    if message:
                        self.process_data(message)

                # draw_idle() coalesces all pending redraws into one pass.
                plt.pause(PAUSE_INTERVAL)

    # ------------------------------------------------------------------
    # Data processing
    # ------------------------------------------------------------------

    def process_data(self, message: str) -> None:
        """Parse one tick and update the relevant market chart.

        Expected format:
            ``<market>,<idx>,<timestamp>,<open>,<high>,<low>,<close>,<volume>``

        Args:
            message: A single comma-separated tick row.
        """
        try:
            parts = message.split(",")
            market, idx, timestamp, open_price, high, low, close, volume = parts[:8]
        except ValueError:
            logger.debug("Malformed tick — skipping: %r", message)
            return

        if market not in self.market_arrays:
            if len(self.market_arrays) >= MAX_MARKETS:
                return   # grid is full
            self.init_market(market)

        self.update_market_data(
            market, idx, timestamp, open_price, high, low, close, volume
        )
        self.redraw_market(market)

    # ------------------------------------------------------------------
    # Market initialisation
    # ------------------------------------------------------------------

    def init_market(self, market: str) -> None:
        """Allocate a circular buffer and assign a subplot for a new market.

        Args:
            market: Market identifier string.
        """
        self.market_arrays[market] = np.zeros((MAX_CANDLES, _N_COLS), dtype=np.float64)
        self.market_pos[market]    = 0
        self.market_count[market]  = 0

        slot    = len(self.market_arrays) - 1
        row_idx = slot // GRID_COLS
        col_idx = slot % GRID_COLS
        ax = self.axes[row_idx, col_idx]
        ax.set_title(market)
        self.ax_dict[market] = ax
        logger.info("Initialised chart for market: %s (slot %d)", market, slot)

    # ------------------------------------------------------------------
    # Data update — O(1) array write, no allocation
    # ------------------------------------------------------------------

    def update_market_data(
        self,
        market: str,
        idx: str,
        timestamp: str,
        open_price: str,
        high: str,
        low: str,
        close: str,
        volume: str,
    ) -> None:
        """Write a new candle into the circular buffer for *market*.

        Overwrites the oldest slot once the buffer is full — no memory
        allocation, no DataFrame creation.

        Args:
            market: Market identifier.
            idx: Sequential row index from the producer.
            timestamp: Candle timestamp (``YYYY-MM-DD HH:MM``).
            open_price: Opening price.
            high: High price.
            low: Low price.
            close: Closing price.
            volume: Trade volume.
        """
        arr  = self.market_arrays[market]
        pos  = self.market_pos[market]
        slot = pos % MAX_CANDLES

        arr[slot, _COL_INDEX]  = int(idx)
        arr[slot, _COL_DATE]   = mdates.date2num(
            datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
        )
        arr[slot, _COL_OPEN]   = float(open_price)
        arr[slot, _COL_HIGH]   = float(high)
        arr[slot, _COL_LOW]    = float(low)
        arr[slot, _COL_CLOSE]  = float(close)
        arr[slot, _COL_VOLUME] = float(volume)

        self.market_pos[market]   = pos + 1
        self.market_count[market] = min(self.market_count[market] + 1, MAX_CANDLES)

    # ------------------------------------------------------------------
    # Chart rendering
    # ------------------------------------------------------------------

    def redraw_market(self, market: str) -> None:
        """Redraw the candlestick chart for a single market subplot.

        Extracts the ordered window from the circular buffer, computes SMAs
        with ``np.convolve``, and schedules a redraw with ``draw_idle()`` so
        that multiple markets updated in the same batch share one render pass.

        Args:
            market: Market identifier whose subplot should be refreshed.
        """
        count = self.market_count[market]
        pos   = self.market_pos[market]
        arr   = self.market_arrays[market]

        if count == 0:
            return

        # Extract the candles in chronological order from the circular buffer.
        if count < MAX_CANDLES:
            window = arr[:count]
        else:
            start  = pos % MAX_CANDLES
            window = np.roll(arr, -start, axis=0)

        ohlc    = window[:, [_COL_INDEX, _COL_OPEN, _COL_HIGH, _COL_LOW, _COL_CLOSE]]
        closes  = window[:, _COL_CLOSE]
        indices = window[:, _COL_INDEX]

        ax = self.ax_dict[market]
        ax.clear()
        ax.set_title(market)
        candlestick_ohlc(ax, ohlc, width=0.6, colorup="g", colordown="r")

        # SMAs via np.convolve — O(n·w) on at most MAX_CANDLES rows, no pandas needed.
        if count >= SMA_SHORT:
            sma_s = np.convolve(closes, np.ones(SMA_SHORT) / SMA_SHORT, mode="valid")
            ax.plot(
                indices[SMA_SHORT - 1:], sma_s,
                label=f"SMA {SMA_SHORT}", color="blue",
            )
        if count >= SMA_LONG:
            sma_l = np.convolve(closes, np.ones(SMA_LONG) / SMA_LONG, mode="valid")
            ax.plot(
                indices[SMA_LONG - 1:], sma_l,
                label=f"SMA {SMA_LONG}", color="orange",
            )

        if count >= SMA_SHORT:
            ax.legend()
        ax.xaxis.set_major_locator(ticker.MaxNLocator(6))

        # draw_idle deduplicates multiple calls within the same plt.pause() cycle.
        ax.figure.canvas.draw_idle()


if __name__ == "__main__":
    client = Client()
    client.connect_to_broker()
