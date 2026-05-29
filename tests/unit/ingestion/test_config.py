import unittest
from datetime import timedelta

from attendance_etl import config
from attendance_etl.device import zkteco
from attendance_etl.messaging.pubsub import PubSubMessenger
from attendance_etl.pi4 import runtime
from attendance_etl.pi4.state import FETCH_REVIEW_PERIOD, Pi4RuntimeState
from attendance_etl.pi4.workflow import Pi4Workflow
from attendance_etl.storage.review_period import ReviewPeriodStore
from attendance_etl.storage.snapshot import SnapshotStore


class SnapshotSpy:
    def save(self, pi4_state, system_flags, review_sheet_id, last_stored_timestamp):
        pass


class ReviewPeriodStoreSpy:
    def save(self, review_period_info):
        pass


class DeviceStub:
    pass


class MessengerSpy:
    def __init__(self):
        self.published = []

    def publish_message_to_topic(self, topic_name, data):
        self.published.append((topic_name, data))


class FakeConnection:
    def disable_device(self):
        pass

    def enable_device(self):
        pass

    def get_firmware_version(self):
        return "fake"

    def get_users(self):
        return []

    def get_attendance(self):
        return []

    def clear_attendance(self):
        pass

    def disconnect(self):
        pass


class FakeZK:
    instances = []

    def __init__(self, device_ip, port, timeout, force_udp, ommit_ping):
        self.device_ip = device_ip
        self.port = port
        self.timeout = timeout
        self.force_udp = force_udp
        self.ommit_ping = ommit_ping
        FakeZK.instances.append(self)

    def connect(self):
        return FakeConnection()


class ConfigTests(unittest.TestCase):
    def test_config_constants_preserve_existing_default_values(self):
        self.assertEqual(config.PROJECT_ID, "attend1")
        self.assertEqual(config.GET_REVIEW_PERIOD_TOPIC, "get_review_period")
        self.assertEqual(config.NEW_REVIEW_PERIOD_TOPIC, "new_review_period")
        self.assertEqual(config.CREATE_REVIEW_SHEET_TOPIC, "create_review_sheet")
        self.assertEqual(config.NEW_REVIEW_SHEET_TOPIC, "new_review_sheet")
        self.assertEqual(config.STORE_ATTEND_RECORDS_TOPIC, "store_attend_records")
        self.assertEqual(config.LAST_STORED_TIMESTAMP_TOPIC, "last_stored_timestamp")
        self.assertEqual(config.SUBSCRIPTION_NAME, "sub_{}_{}")
        self.assertEqual(config.ACK_DEADLINE, 10)
        self.assertEqual(config.SUBSCRIPTION_TTL, 7776000)
        self.assertEqual(config.MAX_LIMIT, 1)
        self.assertEqual(config.PULL_MSG_TIMEOUT, 30.0)
        self.assertEqual(config.SNAPSHOT_FILE, "snapshot.json")
        self.assertEqual(config.REVIEW_PERIOD_JSON, "review_period.json")
        self.assertEqual(config.BIOMETRIC_DEVICE_CONFIG_FILE, "biometric_device_config.json")
        self.assertEqual(config.POLLING_DELAY, timedelta(minutes=15))
        self.assertEqual(config.DEEP_SLEEP_DURATION, timedelta(hours=1))
        self.assertEqual(config.ZKTECO_TIMEOUT, 10)
        self.assertIs(config.ZKTECO_FORCE_UDP, False)
        self.assertIs(config.ZKTECO_OMMIT_PING, False)

    def test_runtime_components_use_config_defaults(self):
        messenger = PubSubMessenger("att-dev-dk-01")

        self.assertEqual(messenger.project_id, config.PROJECT_ID)
        self.assertEqual(
            messenger.subscription_name(config.NEW_REVIEW_PERIOD_TOPIC),
            "sub_new_review_period_att-dev-dk-01",
        )
        self.assertEqual(SnapshotStore().path, config.SNAPSHOT_FILE)
        self.assertEqual(ReviewPeriodStore().path, config.REVIEW_PERIOD_JSON)
        self.assertEqual(runtime.setup_device_info.__defaults__, (config.BIOMETRIC_DEVICE_CONFIG_FILE,))

    def test_workflow_publishes_to_configured_topics(self):
        state = Pi4RuntimeState(pi4_state=FETCH_REVIEW_PERIOD)
        state.review_period_info = {"month": "May"}
        messenger = MessengerSpy()
        workflow = Pi4Workflow(
            state,
            DeviceStub(),
            messenger,
            SnapshotSpy(),
            ReviewPeriodStoreSpy(),
        )

        workflow.handler_fetch_review_period()
        workflow.handler_request_review_sheet()

        self.assertEqual(
            messenger.published,
            [
                (config.GET_REVIEW_PERIOD_TOPIC, {"month": "May"}),
                (config.CREATE_REVIEW_SHEET_TOPIC, {"month": "May"}),
            ],
        )

    def test_zkteco_connection_uses_configured_defaults(self):
        original_zk = zkteco.ZK
        FakeZK.instances = []
        zkteco.ZK = FakeZK
        try:
            zkteco.pull_records_from_device("192.0.2.10", 4370)
        finally:
            zkteco.ZK = original_zk

        self.assertEqual(len(FakeZK.instances), 1)
        instance = FakeZK.instances[0]
        self.assertEqual(instance.device_ip, "192.0.2.10")
        self.assertEqual(instance.port, 4370)
        self.assertEqual(instance.timeout, config.ZKTECO_TIMEOUT)
        self.assertEqual(instance.force_udp, config.ZKTECO_FORCE_UDP)
        self.assertEqual(instance.ommit_ping, config.ZKTECO_OMMIT_PING)


if __name__ == "__main__":
    unittest.main()
