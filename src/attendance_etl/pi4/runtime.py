import json
import logging

from attendance_etl import config
from attendance_etl.device.zkteco import ZKTecoDevice
from attendance_etl.logging_utils import configure_logging, get_logger
from attendance_etl.messaging.pubsub import PubSubMessenger
from attendance_etl.pi4.state import Pi4RuntimeState
from attendance_etl.pi4.workflow import Pi4Workflow
from attendance_etl.storage.review_period import ReviewPeriodStore
from attendance_etl.storage.snapshot import SnapshotStore

logger = get_logger("Pi4Runtime")


def setup_device_info(path=config.BIOMETRIC_DEVICE_CONFIG_FILE):
    """
    loads the configuration details of the attendance devices
    from the configuration file
    """
    with open(path) as json_file:
        device_info = json.load(json_file)

    logger.info("device config: %s", device_info)
    return device_info


def load_snapshot_into_state(snapshot_store, state):
    snapshot = snapshot_store.load()
    state.pi4_state = snapshot["pi4_state"]
    state.system_flags = snapshot["sys_flags"]
    state.review_sheet_id = snapshot["sheet_id"]
    state.last_stored_timestamp = snapshot["last_stored_timestamp"]

    logger.info(
        "loaded snapshot: pi4 state = %s, review sheet id = %s, last stored timestamp = %s",
        state.pi4_state,
        state.review_sheet_id,
        state.last_stored_timestamp,
    )


def bootstrap_pi4(verbosity=logging.INFO):
    """
    Add annotation
    """
    configure_logging(verbosity)

    state = Pi4RuntimeState()

    state.device_info = setup_device_info()
    device = ZKTecoDevice(state.device_info["ip"], state.device_info["port"])
    messenger = PubSubMessenger(state.device_info["identifier"])

    snapshot_store = SnapshotStore()
    review_period_store = ReviewPeriodStore()

    state.review_period_info = review_period_store.load()
    logger.info("review period info: %s", state.review_period_info)

    load_snapshot_into_state(snapshot_store, state)

    return Pi4Workflow(state, device, messenger, snapshot_store, review_period_store)


def run(workflow):
    workflow.run_forever()


def main():
    """
    main method - marks the start of execution
    """
    workflow = bootstrap_pi4()
    run(workflow)


if __name__ == "__main__":
    main()
