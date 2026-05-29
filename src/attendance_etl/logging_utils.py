"""Shared logging utilities for Attendance ETL components.

Centralising logger acquisition here keeps component logger names stable and
leaves room for a future LoggingManager that can own runtime level changes,
component-specific levels, handler registration, and log sink management.
ETLP-23 only provides startup-time configuration.
"""

import logging
from typing import Dict

VALID_LOG_LEVELS: Dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_LOG_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure process logging once without adding duplicate handlers."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    formatter = logging.Formatter(_LOG_FORMAT)
    if root_logger.handlers:
        for handler in root_logger.handlers:
            handler.setLevel(level)
            handler.setFormatter(formatter)
        return

    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a logger with a stable responsibility-oriented component name."""
    return logging.getLogger(name)


def parse_log_level(level: str) -> int:
    """Parse a supported log level name case-insensitively."""
    normalized_level = level.strip().upper()
    if normalized_level not in VALID_LOG_LEVELS:
        supported_levels = ", ".join(VALID_LOG_LEVELS)
        raise ValueError("unsupported log level '{}'; supported levels: {}".format(level, supported_levels))

    return VALID_LOG_LEVELS[normalized_level]
