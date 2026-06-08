import ast
import unittest
from datetime import datetime
from pathlib import Path

import attendance_etl.pi4.workflow as workflow_module
from attendance_etl.models import AttendanceEvent, Employee, EventType, RuntimeState
from attendance_etl.pi4.state import (
    AWAIT_LAST_STORED_TIMESTAMP,
    AWAIT_REVIEW_PERIOD,
    FETCH_REVIEW_PERIOD,
    FINAL_ALARM_RAISED,
    Pi4RuntimeState,
)
from attendance_etl.pi4.workflow import Pi4Workflow
from attendance_etl.transform import ExtractedBiometricData, TimeRange

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_PATH = REPO_ROOT / "src/attendance_etl/pi4/workflow.py"


def imported_modules(path):
    module_names = set()
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            module_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            module_names.add(node.module)
            module_names.update("{}.".format(node.module) + alias.name for alias in node.names)

    return module_names


def calls_named(path, name):
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == name:
                return True
            if isinstance(node.func, ast.Attribute) and node.func.attr == name:
                return True

    return False


class SnapshotSpy:
    def __init__(self):
        self.saves = []

    def save(self, runtime_state):
        self.saves.append(runtime_state)


class ReviewPeriodSaveSpy:
    def __init__(self):
        self.saved = []

    def save(self, review_period_info):
        self.saved.append(review_period_info)


class MessengerStub:
    def __init__(self, payload):
        self.payload = payload
        self.created_subscriptions = []

    def subscription_name(self, topic_name):
        return "sub_{}_att-dev-dk-01".format(topic_name)

    def create_subscription(self, topic_name, subscription_name):
        self.created_subscriptions.append((topic_name, subscription_name))

    def sync_pull_message(self, subscription_name):
        return self.payload


class DeviceStub:
    pass


class DeviceExtractionSpy:
    def __init__(self, raw_data, site_id="munich-office"):
        self.raw_data = raw_data
        self.site_id = site_id
        self.extract_calls = []
        self.clear_calls = 0

    def extract_biometric_data(self):
        self.extract_calls.append(())
        return self.raw_data

    def pull_records(self):
        raise AssertionError("workflow must not pull raw biometric records")

    def clear_records(self):
        self.clear_calls += 1


class StrategyStub:
    def __init__(self, attendance_events=None):
        self.attendance_events = attendance_events or []

    def transform(self, request):
        return self.attendance_events


class StrategySpy:
    def __init__(self, attendance_events):
        self.attendance_events = attendance_events
        self.requests = []

    def transform(self, request):
        self.requests.append(request)
        return self.attendance_events


class MessengerPublishSpy:
    def __init__(self):
        self.published = []

    def publish_message_to_topic(self, topic_name, data):
        self.published.append((topic_name, data))


class Pi4WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.original_save_snapshot = workflow_module.save_snapshot
        self.original_save_review_period = workflow_module.save_review_period
        self.original_sleep = workflow_module.time.sleep

    def tearDown(self):
        workflow_module.save_snapshot = self.original_save_snapshot
        workflow_module.save_review_period = self.original_save_review_period
        workflow_module.time.sleep = self.original_sleep

    def make_workflow(self, state=None, messenger=None):
        return Pi4Workflow(
            state or Pi4RuntimeState(),
            DeviceStub(),
            StrategyStub(),
            messenger or MessengerStub({}),
        )

    def test_transition_skips_snapshot_for_waiting_states(self):
        snapshot_saver = SnapshotSpy()
        workflow_module.save_snapshot = snapshot_saver.save
        state = Pi4RuntimeState(
            pi4_state=FETCH_REVIEW_PERIOD,
            system_flags=1,
            review_sheet_id="sheet-123",
            last_stored_timestamp="27-05-2026 08:59:12",
        )
        workflow = self.make_workflow(state=state)

        workflow.transition_state(AWAIT_REVIEW_PERIOD)

        self.assertEqual(state.pi4_state, AWAIT_REVIEW_PERIOD)
        self.assertEqual(snapshot_saver.saves, [])

        workflow.transition_state(FETCH_REVIEW_PERIOD)

        self.assertEqual(
            [snapshot.to_dict() for snapshot in snapshot_saver.saves],
            [
                {
                    "pi4_state": FETCH_REVIEW_PERIOD,
                    "sys_flags": 1,
                    "sheet_id": "sheet-123",
                    "last_stored_timestamp": "27-05-2026 08:59:12",
                }
            ],
        )

    def test_empty_review_period_preserves_final_alarm_flag_as_next_state_value(self):
        snapshot_saver = SnapshotSpy()
        review_period_saver = ReviewPeriodSaveSpy()
        workflow_module.save_snapshot = snapshot_saver.save
        workflow_module.save_review_period = review_period_saver.save
        state = Pi4RuntimeState(system_flags=0)
        workflow = self.make_workflow(
            state=state,
            messenger=MessengerStub({}),
        )

        workflow.handler_await_review_period()

        self.assertEqual(FINAL_ALARM_RAISED, FETCH_REVIEW_PERIOD)
        self.assertEqual(state.pi4_state, FETCH_REVIEW_PERIOD)
        self.assertEqual(review_period_saver.saved, [])
        self.assertIsInstance(snapshot_saver.saves[0], RuntimeState)
        self.assertEqual(snapshot_saver.saves[0].pi4_state, FETCH_REVIEW_PERIOD)

    def test_relay_attendance_records_delegates_extraction_and_transformation(self):
        workflow_module.time.sleep = lambda seconds: None
        snapshot_saver = SnapshotSpy()
        workflow_module.save_snapshot = snapshot_saver.save
        raw_data = ExtractedBiometricData(employees=["raw-user"], attendance_records=["raw-record"])
        device = DeviceExtractionSpy(raw_data)
        event = AttendanceEvent(
            site_id="munich-office",
            employee=Employee(id="10042", name="Ada Lovelace"),
            event_type=EventType.CLOCK_IN,
            timestamp=datetime(2026, 5, 27, 8, 5, 0),
        )
        strategy = StrategySpy([event])
        messenger = MessengerPublishSpy()
        state = Pi4RuntimeState(
            pi4_state=FETCH_REVIEW_PERIOD,
            device_info={"site_id": "munich-office"},
            review_sheet_id="sheet-123",
        )
        state.review_period_info = {"start_date": "05/27/2026", "end_date": "12/31/2099"}
        workflow = Pi4Workflow(state, device, strategy, messenger)

        workflow.handler_relay_attendance_records()

        self.assertEqual(device.extract_calls, [()])
        self.assertEqual(len(strategy.requests), 1)
        self.assertIs(strategy.requests[0].raw_data, raw_data)
        self.assertEqual(strategy.requests[0].site_id, "munich-office")
        self.assertEqual(strategy.requests[0].time_range, TimeRange(start_time=datetime(2026, 5, 27)))
        self.assertEqual(
            messenger.published,
            [
                (
                    workflow_module.config.STORE_ATTEND_RECORDS_TOPIC,
                    {
                        "sheet_id": "sheet-123",
                        "device_id": "munich-office",
                        "start_date": "05/27/2026",
                        "records": [event.to_dict()],
                    },
                )
            ],
        )
        self.assertEqual(state.pi4_state, AWAIT_LAST_STORED_TIMESTAMP)
        self.assertEqual(snapshot_saver.saves, [])

    def test_review_period_expiry_delegates_extraction_before_clearing_device_records(self):
        snapshot_saver = SnapshotSpy()
        workflow_module.save_snapshot = snapshot_saver.save
        raw_data = ExtractedBiometricData(employees=[], attendance_records=[])
        device = DeviceExtractionSpy(raw_data)
        strategy = StrategySpy([])
        state = Pi4RuntimeState(
            pi4_state=FETCH_REVIEW_PERIOD,
            device_info={"site_id": "munich-office"},
        )
        state.review_period_info = {"start_date": "05/27/2026", "end_date": "05/29/2026"}
        workflow = Pi4Workflow(state, device, strategy, MessengerStub({}))

        workflow.handler_review_period_expired()

        self.assertEqual(device.extract_calls, [()])
        self.assertEqual(len(strategy.requests), 1)
        self.assertIs(strategy.requests[0].raw_data, raw_data)
        self.assertEqual(strategy.requests[0].site_id, "munich-office")
        self.assertEqual(strategy.requests[0].time_range, TimeRange(start_time=datetime(2026, 5, 27)))
        self.assertEqual(device.clear_calls, 1)
        self.assertIsNone(state.review_sheet_id)
        self.assertIsNone(state.last_stored_timestamp)
        self.assertEqual(state.pi4_state, FETCH_REVIEW_PERIOD)

    def test_workflow_has_no_raw_zkteco_extraction_sequence(self):
        imported = imported_modules(WORKFLOW_PATH)

        self.assertNotIn("attendance_etl.transform.zkteco_records", imported)
        self.assertNotIn("attendance_etl.transform.zkteco_records.convert_to_map", imported)
        self.assertNotIn("attendance_etl.transform.zkteco_records.decode_zk_format", imported)
        self.assertNotIn("attendance_etl.transform.zkteco_records.filter_records", imported)
        self.assertFalse(calls_named(WORKFLOW_PATH, "pull_records"))
        self.assertFalse(calls_named(WORKFLOW_PATH, "extract_attendance_records"))
        self.assertFalse(calls_named(WORKFLOW_PATH, "convert_to_map"))
        self.assertFalse(calls_named(WORKFLOW_PATH, "decode_zk_format"))
        self.assertFalse(calls_named(WORKFLOW_PATH, "filter_records"))
        self.assertFalse(calls_named(WORKFLOW_PATH, "AttendanceEvent"))
        self.assertNotIn("punch", WORKFLOW_PATH.read_text())
        self.assertTrue(calls_named(WORKFLOW_PATH, "extract_biometric_data"))
        self.assertTrue(calls_named(WORKFLOW_PATH, "transform"))


if __name__ == "__main__":
    unittest.main()
