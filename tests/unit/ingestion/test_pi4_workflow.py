import unittest

import attendance_etl.pi4.workflow as workflow_module
from attendance_etl.models import RuntimeState
from attendance_etl.pi4.state import (
    AWAIT_REVIEW_PERIOD,
    FETCH_REVIEW_PERIOD,
    FINAL_ALARM_RAISED,
    Pi4RuntimeState,
)
from attendance_etl.pi4.workflow import Pi4Workflow


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


class Pi4WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.original_save_snapshot = workflow_module.save_snapshot
        self.original_save_review_period = workflow_module.save_review_period

    def tearDown(self):
        workflow_module.save_snapshot = self.original_save_snapshot
        workflow_module.save_review_period = self.original_save_review_period

    def make_workflow(self, state=None, messenger=None):
        return Pi4Workflow(
            state or Pi4RuntimeState(),
            DeviceStub(),
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


if __name__ == "__main__":
    unittest.main()
