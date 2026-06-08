import logging

from attendance_etl import config
from attendance_etl.devices.biometric_device_config import BiometricDeviceConfigBuilder
from attendance_etl.devices.biometric_device_factory import BiometricDeviceFactory
from attendance_etl.logging_utils import configure_logging, get_logger
from attendance_etl.messaging.pubsub import PubSubMessenger
from attendance_etl.pi4.state import Pi4RuntimeState
from attendance_etl.pi4.workflow import Pi4Workflow
from attendance_etl.storage.review_period import load_review_period
from attendance_etl.storage.snapshot import load_snapshot
from attendance_etl.transform.transformation_strategy_factory import TransformationStrategyFactory

logger = get_logger("Pi4Runtime")


def load_biometric_device_config(path=config.BIOMETRIC_DEVICE_CONFIG_FILE):
    device_config = BiometricDeviceConfigBuilder.from_json_file(path)
    logger.info("device config: %s", device_config)
    return device_config


def device_info_from_config(device_config):
    return {"site_id": device_config.site_id}


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

    device_config = load_biometric_device_config()
    state.device_info = device_info_from_config(device_config)
    device = BiometricDeviceFactory.create(device_config)
    strategy = TransformationStrategyFactory.create(device_config.vendor)
    messenger = PubSubMessenger(state.device_info["site_id"])

    review_period = load_review_period()
    state.review_period_info = review_period.to_dict()
    logger.info("review period info: %s", state.review_period_info)

    load_snapshot_into_state(state)

    return Pi4Workflow(state, device, strategy, messenger)


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
