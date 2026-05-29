import unittest

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

    def save(self, pi4_state, system_flags, review_sheet_id, last_stored_timestamp):
        self.saves.append(
            {
                "pi4_state": pi4_state,
                "sys_flags": system_flags,
                "sheet_id": review_sheet_id,
                "last_stored_timestamp": last_stored_timestamp,
            }
        )


class ReviewPeriodStoreSpy:
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
    def make_workflow(self, state=None, messenger=None, snapshot_store=None, review_period_store=None):
        return Pi4Workflow(
            state or Pi4RuntimeState(),
            DeviceStub(),
            messenger or MessengerStub({}),
            snapshot_store or SnapshotSpy(),
            review_period_store or ReviewPeriodStoreSpy(),
        )

    def test_transition_skips_snapshot_for_waiting_states(self):
        snapshot_store = SnapshotSpy()
        state = Pi4RuntimeState(
            pi4_state=FETCH_REVIEW_PERIOD,
            system_flags=1,
            review_sheet_id="sheet-123",
            last_stored_timestamp="27-05-2026 08:59:12",
        )
        workflow = self.make_workflow(state=state, snapshot_store=snapshot_store)

        workflow.transition_state(AWAIT_REVIEW_PERIOD)

        self.assertEqual(state.pi4_state, AWAIT_REVIEW_PERIOD)
        self.assertEqual(snapshot_store.saves, [])

        workflow.transition_state(FETCH_REVIEW_PERIOD)

        self.assertEqual(
            snapshot_store.saves,
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
        snapshot_store = SnapshotSpy()
        review_period_store = ReviewPeriodStoreSpy()
        state = Pi4RuntimeState(system_flags=0)
        workflow = self.make_workflow(
            state=state,
            messenger=MessengerStub({}),
            snapshot_store=snapshot_store,
            review_period_store=review_period_store,
        )

        workflow.handler_await_review_period()

        self.assertEqual(FINAL_ALARM_RAISED, FETCH_REVIEW_PERIOD)
        self.assertEqual(state.pi4_state, FETCH_REVIEW_PERIOD)
        self.assertEqual(review_period_store.saved, [])
        self.assertEqual(snapshot_store.saves[0]["pi4_state"], FETCH_REVIEW_PERIOD)


if __name__ == "__main__":
    unittest.main()
