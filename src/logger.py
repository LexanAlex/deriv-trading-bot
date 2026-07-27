"""
src/logger.py
-------------
Centralized logging configuration for the Deriv Trading Bot.
Logs to both a rotating file and the console.

Non-blocking by design
-----------------------
logger.info()/logger.debug() calls happen inside the trading engine's async
hot path - between receiving a tick and submitting an order, several log
calls fire (proposal requested, proposal received, buy submitted). A plain
RotatingFileHandler does its file write synchronously, inline, in the same
thread that's running the event loop. On a fast local disk that write is a
few tens of microseconds and invisible next to network latency, but on a
slower or contended disk (network-backed volumes, antivirus-scanned
filesystems, a rotation boundary triggering a file rename) it can block the
event loop for the write's full duration - directly delaying tick
processing and order submission for however long that disk operation takes.

QueueHandler/QueueListener (stdlib, logging.handlers) removes that risk
entirely: logger calls in the hot path only push a record onto an in-memory
queue (near-instant, no I/O), and a single background thread owns the real
file/console handlers and does the disk writes off the critical path. This
changes nothing about *what* gets logged or *when* callers see it in the
log file (order is preserved, nothing is dropped under normal operation) -
it only moves the I/O off the thread that's also processing ticks and
sending orders.
"""

import atexit
import logging
import os
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from queue import Queue

from config import LOG_DIR, LOG_FILE, LOG_LEVEL

_listener: QueueListener = None  # type: ignore[assignment]
_log_queue: Queue = None  # type: ignore[assignment]


def _ensure_listener() -> Queue:
    """Create the shared log queue and its background writer thread once."""
    global _listener, _log_queue
    if _log_queue is not None:
        return _log_queue

    os.makedirs(LOG_DIR, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)

    # No size cap: under normal operation the listener thread drains records
    # as fast as they arrive (file I/O is far faster than log call volume).
    # An unbounded queue only grows if the disk stalls for a long time, which
    # is exactly the scenario this exists to protect the hot path from.
    _log_queue = Queue(-1)
    _listener = QueueListener(
        _log_queue, file_handler, console_handler, respect_handler_level=True
    )
    _listener.start()
    atexit.register(_listener.stop)
    return _log_queue


def get_logger(name: str) -> logging.Logger:
    """
    Returns a named logger whose calls enqueue instantly and never block on
    file/console I/O. Uses a RotatingFileHandler (via a background listener
    thread) to prevent unbounded log growth.
    """
    queue = _ensure_listener()

    logger = logging.getLogger(name)
    if logger.handlers:
        # Avoid adding duplicate handlers if called multiple times
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    logger.addHandler(QueueHandler(queue))
    logger.propagate = False

    return logger
