"""
MarketConsumer module for the ProjectOS market data streaming system.

Reads an OHLCV CSV file row by row and streams each tick to the broker
over a TCP socket, one row per second.

TCP_NODELAY is enabled so each tick is transmitted immediately without
Nagle's algorithm batching small packets.
"""

import logging
import os
import signal
import socket
import sys
import time

from config import HOST, PORT, BUFFER_SIZE, TICK_INTERVAL

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class MarketConsumer:
    """Streams OHLCV rows from a CSV file to the broker one tick at a time.

    Each row is prefixed with the market name and a sequential index so that
    the broker and clients can identify the data source.
    """

    def __init__(
        self, broker_host: str, broker_port: int, csv_file: str
    ) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.csv_file = csv_file
        self.market_name: str = os.path.basename(csv_file).split(".")[0]
        self._running = True

    def start(self) -> None:
        """Validate the CSV file, connect to the broker, and begin streaming."""
        if not os.path.isfile(self.csv_file):
            logger.error("CSV file not found: %s", self.csv_file)
            sys.exit(1)

        def _signal_handler(sig, frame) -> None:
            logger.info(
                "SIGINT received — stopping MarketConsumer %s.", self.market_name
            )
            self._running = False

        signal.signal(signal.SIGINT, _signal_handler)

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Disable Nagle's algorithm — send each tick immediately.
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s.connect((self.broker_host, self.broker_port))
                logger.info(
                    "MarketConsumer %s connected to broker.", self.market_name
                )
                s.sendall(f"MARKET_CONSUMER_{self.market_name}".encode())

                confirmation = s.recv(BUFFER_SIZE).decode()
                if confirmation != "CLIENT_CONNECTED":
                    logger.error(
                        "Unexpected broker response: %r — aborting.", confirmation
                    )
                    return

                logger.info(
                    "Client confirmed — starting stream for %s.", self.market_name
                )
                self._stream(s)

        except ConnectionRefusedError:
            logger.error(
                "Cannot connect to broker at %s:%s.", self.broker_host, self.broker_port
            )
        except OSError as exc:
            logger.error(
                "Socket error in MarketConsumer %s: %s", self.market_name, exc
            )

    def _stream(self, s: socket.socket) -> None:
        """Read the CSV and send each row to the broker with a fixed delay.

        Args:
            s: The connected broker socket.
        """
        try:
            with open(self.csv_file, "r") as file:
                for index, line in enumerate(file):
                    if not self._running:
                        break
                    payload = f"{self.market_name},{index},{line.strip()}\n"
                    try:
                        s.sendall(payload.encode())
                    except OSError as exc:
                        logger.error(
                            "Send failed for %s at row %d: %s",
                            self.market_name, index, exc,
                        )
                        break
                    time.sleep(TICK_INTERVAL)
        except OSError as exc:
            logger.error("Cannot read CSV file %s: %s", self.csv_file, exc)

        logger.info("MarketConsumer %s finished streaming.", self.market_name)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python market.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]
    mc = MarketConsumer(HOST, PORT, csv_file)
    mc.start()
