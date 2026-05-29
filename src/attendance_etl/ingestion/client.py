"""Compatibility facade for the Raspberry Pi ingestion runtime."""

__all__ = ["bootstrap_pi4", "main", "run"]

_WORKFLOW = None


def bootstrap_pi4():
    global _WORKFLOW
    from attendance_etl.pi4 import runtime

    _WORKFLOW = runtime.bootstrap_pi4()


def run():
    from attendance_etl.pi4 import runtime

    runtime.run(_WORKFLOW)


def main():
    bootstrap_pi4()
    run()


if __name__ == "__main__":
    main()
