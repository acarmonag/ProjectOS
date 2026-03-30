"""
Broker module for the ProjectOS market data streaming system.

Acts as the central message broker: accepts TCP connections from
MarketConsumer producers and Client subscribers, caches every incoming
tick to CSV files, and forwards each tick to all connected clients.

Performance notes
-----------------
- ``bisect.insort`` maintains ``all_data`` in date order in O(n) instead of
  sorting the whole list on every tick O(n log n).
- File I/O runs *outside* the lock so concurrent producer threads are not
  blocked by each other's disk writes.
- Each client gets a bounded ``queue.Queue``; the thread that accepted the
  client connection owns the socket and drains the queue.  This decouples
  slow clients from producers — a lagging client drops ticks instead of
  stalling the entire pipeline.
- ``subprocess.Popen`` replaces the ``Thread(subprocess.run)`` pattern so
  market-consumer processes are fire-and-forget without tying up a thread.
"""

import bisect
import glob
import logging
import os
import queue
import signal
import socket
import subprocess
import sys
from datetime import datetime
from threading import Event, Lock, Thread
from typing import List, Tuple

from config import (
    HOST, PORT, BUFFER_SIZE,
    CACHE_DIR, CSV_DATA_DIR, MAX_CONNECTIONS, PERIODS,
    ALL_DATA_MAXLEN, CLIENT_QUEUE_MAXSIZE,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class Broker:
    """Central message broker for market data streaming.

    Listens for incoming TCP connections from MarketConsumer producers and
    Client subscribers.  Incoming ticks are inserted into a sorted in-memory
    list, appended to CSV caches, and forwarded to every connected client via
    per-client queues.
    """

    def __init__(self, host: str = HOST, port: int = PORT) -> None:
        self.host = host
        self.port = port
        self.lock = Lock()
        # Per-client send queues — broker holds references, _handle_client owns the socket.
        self.client_queues: List[queue.Queue] = []
        # Sorted list of (datetime_key, data_line) tuples — bisect keeps it ordered.
        self.all_data: List[Tuple[datetime, str]] = []
        self.client_connected = Event()
        self.running = True
        self.cache_directory = CACHE_DIR

        os.makedirs(self.cache_directory, exist_ok=True)
        self._clear_cache()

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def _clear_cache(self) -> None:
        """Remove all CSV files from a previous run so data never mixes."""
        for path in glob.glob(os.path.join(self.cache_directory, "*.csv")):
            try:
                os.remove(path)
                logger.debug("Cleared stale cache: %s", path)
            except OSError as exc:
                logger.warning("Could not remove cache file %s: %s", path, exc)

    def _flush_sorted_cache(self) -> None:
        """Write the final date-sorted snapshot to disk (called on shutdown)."""
        sorted_csv = os.path.join(self.cache_directory, "date_sorted.csv")
        try:
            with self.lock:
                lines = [item[1] for item in self.all_data]
            with open(sorted_csv, "w") as f:
                f.writelines(lines)
            logger.info("Flushed date_sorted.csv (%d rows).", len(lines))
        except OSError as exc:
            logger.error("Failed to flush date_sorted.csv: %s", exc)

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Bind the server socket and accept incoming connections."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server_socket.bind((self.host, self.port))
        except OSError as exc:
            logger.error("Cannot bind to %s:%s — %s", self.host, self.port, exc)
            sys.exit(1)

        server_socket.listen(MAX_CONNECTIONS)

        def _signal_handler(sig, frame) -> None:
            logger.info("SIGINT received — stopping broker.")
            self.running = False
            self._flush_sorted_cache()
            server_socket.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, _signal_handler)

        logger.info(
            "Broker listening on %s:%s. Press Ctrl+C to stop.", self.host, self.port
        )
        while self.running:
            try:
                conn, addr = server_socket.accept()
                logger.debug("New connection from %s", addr)
                Thread(
                    target=self.handle_connection, args=(conn,), daemon=True
                ).start()
            except OSError:
                # Socket was closed by the signal handler — exit cleanly.
                break

    # ------------------------------------------------------------------
    # Connection dispatch
    # ------------------------------------------------------------------

    def handle_connection(self, conn: socket.socket) -> None:
        """Read the handshake and route to the appropriate handler.

        Args:
            conn: The accepted socket.
        """
        try:
            initial_message = conn.recv(BUFFER_SIZE).decode()
        except OSError as exc:
            logger.warning("Failed to read handshake: %s", exc)
            conn.close()
            return

        if initial_message.startswith("MARKET_CONSUMER"):
            market_name = initial_message[len("MARKET_CONSUMER_"):]
            self._handle_market_consumer(conn, market_name)
        elif initial_message == "CLIENT":
            self._handle_client(conn)
        else:
            logger.warning("Unknown handshake: %r", initial_message)
            conn.close()

    # ------------------------------------------------------------------
    # Producer handler
    # ------------------------------------------------------------------

    def _handle_market_consumer(self, conn: socket.socket, market_name: str) -> None:
        """Receive ticks from a MarketConsumer, update caches, and fan out.

        Partial reads are buffered so a tick split across two ``recv`` calls
        is reassembled before processing.  File I/O runs outside the lock to
        avoid blocking other concurrent producers.

        Args:
            conn: Socket connected to the MarketConsumer.
            market_name: Identifier used as the market-specific CSV filename stem.
        """
        logger.info(
            "MarketConsumer connected: %s — waiting for a client.", market_name
        )
        self.client_connected.wait()

        try:
            conn.sendall("CLIENT_CONNECTED".encode())
        except OSError as exc:
            logger.error("Could not notify MarketConsumer %s: %s", market_name, exc)
            conn.close()
            return

        all_csv    = os.path.join(self.cache_directory, "all_data.csv")
        market_csv = os.path.join(self.cache_directory, f"{market_name}.csv")
        sorted_csv = os.path.join(self.cache_directory, "date_sorted.csv")

        incomplete = ""
        while True:
            try:
                chunk = conn.recv(BUFFER_SIZE).decode()
            except OSError as exc:
                logger.warning(
                    "Connection error from MarketConsumer %s: %s", market_name, exc
                )
                break
            if not chunk:
                logger.info("MarketConsumer %s disconnected.", market_name)
                break

            # Reassemble partial messages split across recv calls.
            chunk = incomplete + chunk
            lines = chunk.split("\n")
            incomplete = lines.pop()   # last element is partial (or empty)

            for line in lines:
                if not line:
                    continue
                data = line + "\n"

                # Parse the date for sorted insertion — bail on malformed ticks.
                try:
                    key = datetime.strptime(line.split(",")[2], "%Y-%m-%d %H:%M")
                except (ValueError, IndexError) as exc:
                    logger.error(
                        "Cannot parse date from tick '%s': %s", line, exc
                    )
                    continue

                # --- In-memory update (lock held — fast pointer ops only) ------
                with self.lock:
                    bisect.insort(self.all_data, (key, data))
                    if len(self.all_data) > ALL_DATA_MAXLEN:
                        self.all_data.pop(0)          # drop oldest entry
                    all_data_snapshot = list(self.all_data)   # shallow copy of tuples
                    queues_snapshot   = list(self.client_queues)
                # -----------------------------------------------------------------

                # File I/O — outside the lock so producers don't block each other.
                try:
                    with open(all_csv, "a") as f:
                        f.write(data)
                    with open(market_csv, "a") as f:
                        f.write(data)
                    with open(sorted_csv, "w") as f:
                        f.writelines(item[1] for item in all_data_snapshot)
                except OSError as exc:
                    logger.error("Cache write error for %s: %s", market_name, exc)

                # Non-blocking enqueue to each client — slow clients drop ticks.
                encoded = data.encode()
                for q in queues_snapshot:
                    try:
                        q.put_nowait(encoded)
                    except queue.Full:
                        logger.warning(
                            "Client queue full — dropping tick (market=%s).",
                            market_name,
                        )

        conn.close()

    # ------------------------------------------------------------------
    # Subscriber handler
    # ------------------------------------------------------------------

    def _handle_client(self, conn: socket.socket) -> None:
        """Register a subscriber and drain its send queue until it disconnects.

        This thread owns the client socket for its lifetime.  The market-consumer
        threads only put bytes into the queue — they never touch the socket directly.

        Args:
            conn: Socket connected to the subscriber.
        """
        q: queue.Queue = queue.Queue(maxsize=CLIENT_QUEUE_MAXSIZE)
        with self.lock:
            self.client_queues.append(q)
        self.client_connected.set()
        logger.info("Client connected. Total clients: %d", len(self.client_queues))

        while True:
            try:
                data = q.get(timeout=1.0)
                conn.sendall(data)
            except queue.Empty:
                continue   # keep waiting; timeout lets us detect shutdown later
            except OSError as exc:
                logger.warning("Client send error — removing: %s", exc)
                break

        with self.lock:
            if q in self.client_queues:
                self.client_queues.remove(q)
        conn.close()
        logger.info("Client disconnected. Total clients: %d", len(self.client_queues))

    # ------------------------------------------------------------------
    # Market-consumer process launcher
    # ------------------------------------------------------------------

    def start_market_consumers(self, periodo: str) -> None:
        """Spawn one MarketConsumer process per CSV file matching *periodo*.

        Uses ``subprocess.Popen`` (fire-and-forget) instead of wrapping
        blocking ``subprocess.run`` in a thread.

        Args:
            periodo: Timeframe suffix to filter CSV files (e.g. ``"H1"``).
        """
        archivos = glob.glob(os.path.join(CSV_DATA_DIR, f"*_{periodo}.csv"))
        if not archivos:
            logger.warning(
                "No CSV files found for period '%s' in '%s'.", periodo, CSV_DATA_DIR
            )
            return

        for archivo in archivos:
            nombre_mercado = os.path.basename(archivo).split("_")[0]
            logger.info(
                "Starting MarketConsumer for %s (period=%s).", nombre_mercado, periodo
            )
            proc = subprocess.Popen([sys.executable, "market.py", archivo])
            logger.debug(
                "MarketConsumer PID %d for %s.", proc.pid, nombre_mercado
            )


# ---------------------------------------------------------------------------
# Period selection
# ---------------------------------------------------------------------------

def seleccionar_periodo() -> str:
    """Interactively prompt the user to select a timeframe period.

    Returns:
        The selected period string (e.g. ``"H1"``).
    """
    print("Seleccione un periodo:")
    for i, periodo in enumerate(PERIODS, start=1):
        print(f"  {i}. {periodo}")

    while True:
        try:
            seleccion = int(input("Ingrese el número del periodo: "))
            if 1 <= seleccion <= len(PERIODS):
                return PERIODS[seleccion - 1]
            print("Número no válido. Por favor, intente de nuevo.")
        except ValueError:
            print("Entrada no válida. Por favor, ingrese un número.")


if __name__ == "__main__":
    periodo = seleccionar_periodo()
    broker = Broker()
    broker.start_market_consumers(periodo)
    broker.start()
