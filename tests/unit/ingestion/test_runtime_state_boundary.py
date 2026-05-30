import unittest
from datetime import datetime

import attendance_etl.pi4.runtime as runtime_module
from attendance_etl.models import RuntimeState
from attendance_etl.pi4.runtime import load_snapshot_into_state
from attendance_etl.pi4.state import Pi4RuntimeState


class RuntimeStateBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.original_load_snapshot = runtime_module.load_snapshot

    def tearDown(self):
        runtime_module.load_snapshot = self.original_load_snapshot

    def test_load_snapshot_into_state_consumes_runtime_state_attributes(self):
        snapshot = RuntimeState.from_dict(
            {
                "pi4_state": 7,
                "sys_flags": 1,
                "sheet_id": "sheet-123",
                "last_stored_timestamp": "27-05-2026 08:59:12",
            }
        )

        def fail_to_dict():
            raise AssertionError("runtime should consume RuntimeState attributes directly")

        snapshot.to_dict = fail_to_dict
        state = Pi4RuntimeState()
        runtime_module.load_snapshot = lambda path: snapshot

        load_snapshot_into_state(state, "snapshot.json")

        self.assertEqual(state.pi4_state, 7)
        self.assertEqual(state.system_flags, 1)
        self.assertEqual(state.review_sheet_id, "sheet-123")
        self.assertEqual(state.last_stored_timestamp, datetime(2026, 5, 27, 8, 59, 12))


if __name__ == "__main__":
    unittest.main()
