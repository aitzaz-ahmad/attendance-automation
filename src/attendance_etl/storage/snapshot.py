import json

from attendance_etl import config
from attendance_etl.logging_utils import get_logger
from attendance_etl.models import RuntimeState

logger = get_logger("RuntimeState")


def load_snapshot(path=config.SNAPSHOT_FILE) -> RuntimeState:
    """
    loads the snapshot as the shared runtime state domain model.
    """
    logger.debug("loading runtime state from %s", path)
    with open(path) as json_file:
        return RuntimeState.from_dict(json.load(json_file))


def save_snapshot(runtime_state: RuntimeState, path=config.SNAPSHOT_FILE) -> None:
    """
    saves the runtime snapshot to disk using the legacy JSON shape.
    """
    if not isinstance(runtime_state, RuntimeState):
        raise TypeError("save_snapshot expects a RuntimeState instance")

    logger.debug("saving runtime state to %s", path)
    with open(path, "w") as json_file:
        json.dump(runtime_state.to_dict(), json_file)
