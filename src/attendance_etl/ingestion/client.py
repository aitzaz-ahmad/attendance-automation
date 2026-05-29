"""Compatibility facade for the Raspberry Pi ingestion runtime."""

import logging

from attendance_etl.logging_utils import get_logger

__all__ = ["bootstrap_pi4", "main", "run"]

_WORKFLOW = None
logger = get_logger("IngestionClient")


def bootstrap_pi4(verbosity=logging.INFO):
    global _WORKFLOW
    from attendance_etl.pi4 import runtime

    logger.debug("bootstrapping Pi runtime")
    _WORKFLOW = runtime.bootstrap_pi4(verbosity=verbosity)


def run():
    from attendance_etl.pi4 import runtime

    logger.debug("starting Pi runtime workflow")
    runtime.run(_WORKFLOW)


def main(verbosity=logging.INFO):
    bootstrap_pi4(verbosity=verbosity)
    run()


if __name__ == "__main__":
    main()
