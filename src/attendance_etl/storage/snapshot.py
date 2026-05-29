import json

from attendance_etl import config
from attendance_etl.logging_utils import get_logger

logger = get_logger("RuntimeState")


class SnapshotStore:
    def __init__(self, path=config.SNAPSHOT_FILE):
        self.path = path

    def load(self):
        return load_snapshot(self.path)

    def save(self, pi4_state, system_flags, review_sheet_id, last_stored_timestamp):
        save_snapshot(
            pi4_state=pi4_state,
            system_flags=system_flags,
            review_sheet_id=review_sheet_id,
            last_stored_timestamp=last_stored_timestamp,
            path=self.path,
        )


def load_snapshot(path=config.SNAPSHOT_FILE):
    """
    loads the snapshot captured during checkpointing to resume operation
    without losing state information in case of a (device) shutdown
    """
    logger.debug("loading runtime state from %s", path)
    with open(path) as json_file:
        return json.load(json_file)


def save_snapshot(
    pi4_state,
    system_flags,
    review_sheet_id,
    last_stored_timestamp,
    path=config.SNAPSHOT_FILE,
):
    """
    saves the runtime snapshot to disk using the legacy JSON shape.
    """
    snapshot = {}
    snapshot["pi4_state"] = pi4_state
    snapshot["sys_flags"] = system_flags
    snapshot["sheet_id"] = review_sheet_id
    snapshot["last_stored_timestamp"] = last_stored_timestamp

    logger.debug("saving runtime state to %s", path)
    with open(path, "w") as json_file:
        json.dump(snapshot, json_file)
