import json

# constants defined for on-disk files
SNAPSHOT_FILE = "snapshot.json"


class SnapshotStore:
    def __init__(self, path=SNAPSHOT_FILE):
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


def load_snapshot(path=SNAPSHOT_FILE):
    """
    loads the snapshot captured during checkpointing to resume operation
    without losing state information in case of a (device) shutdown
    """
    with open(path) as json_file:
        return json.load(json_file)


def save_snapshot(pi4_state, system_flags, review_sheet_id, last_stored_timestamp, path=SNAPSHOT_FILE):
    """
    saves the runtime snapshot to disk using the legacy JSON shape.
    """
    snapshot = {}
    snapshot["pi4_state"] = pi4_state
    snapshot["sys_flags"] = system_flags
    snapshot["sheet_id"] = review_sheet_id
    snapshot["last_stored_timestamp"] = last_stored_timestamp

    with open(path, "w") as json_file:
        json.dump(snapshot, json_file)
