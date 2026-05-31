import json
import logging

from attendance_etl import config
from attendance_etl.devices.zkteco_device import ZKTecoDevice
from attendance_etl.logging_utils import configure_logging, get_logger
from attendance_etl.messaging.pubsub import PubSubMessenger
from attendance_etl.pi4.state import Pi4RuntimeState
from attendance_etl.pi4.workflow import Pi4Workflow
from attendance_etl.storage.review_period import load_review_period
from attendance_etl.storage.snapshot import load_snapshot

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


def load_snapshot_into_state(state, path=config.SNAPSHOT_FILE):
    snapshot = load_snapshot(path)
    state.pi4_state = snapshot.pi4_state
    state.system_flags = snapshot.sys_flags
    state.review_sheet_id = snapshot.sheet_id
    state.last_stored_timestamp = snapshot.last_stored_timestamp

    logger.info(
        "loaded runtime state: pi4 state = %s, review sheet id = %s, last stored timestamp = %s",
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

    review_period = load_review_period()
    state.review_period_info = review_period.to_dict()
    logger.info("review period info: %s", state.review_period_info)

    load_snapshot_into_state(state)

    return Pi4Workflow(state, device, messenger)


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
