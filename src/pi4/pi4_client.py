"""Compatibility entry point for the packaged Raspberry Pi client."""

from pathlib import Path
import sys

src_root = Path(__file__).resolve().parents[1]
if str(src_root) not in sys.path:
    sys.path.insert(0, str(src_root))

from attendance_etl.ingestion.client import main as _main


if __name__ == "__main__":
    _main()
