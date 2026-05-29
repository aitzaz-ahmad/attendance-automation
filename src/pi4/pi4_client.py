"""Compatibility entry point for the packaged Raspberry Pi client."""

import argparse
import logging
from pathlib import Path
import sys

src_root = Path(__file__).resolve().parents[1]
if str(src_root) not in sys.path:
    sys.path.insert(0, str(src_root))


def _parse_log_level(value):
    from attendance_etl.logging_utils import parse_log_level

    try:
        return parse_log_level(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run the Raspberry Pi attendance ingestion client.")
    parser.add_argument(
        "-V",
        "--verbosity",
        default=logging.INFO,
        metavar="LEVEL",
        type=_parse_log_level,
        help="logging verbosity: DEBUG, INFO, WARNING, ERROR, or CRITICAL",
    )
    return parser.parse_args(argv)


def main(argv=None):
    from attendance_etl.ingestion.client import main as _main

    args = parse_args(argv)
    _main(verbosity=args.verbosity)


if __name__ == "__main__":
    main()
