import json
import os
import tempfile
import unittest
from datetime import datetime

import attendance_etl.storage.review_period as review_period_module
import attendance_etl.storage.snapshot as snapshot_module
from attendance_etl.models import ReviewPeriod, RuntimeState
from attendance_etl.storage.review_period import load_review_period, save_review_period
from attendance_etl.storage.snapshot import load_snapshot, save_snapshot


class StoragePersistenceTests(unittest.TestCase):
    def test_snapshot_load_save_preserves_legacy_shape(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "snapshot.json")
            runtime_state = RuntimeState(
                pi4_state=7,
                sys_flags=1,
                sheet_id="sheet-123",
                last_stored_timestamp=datetime(2026, 5, 27, 8, 59, 12),
            )

            save_snapshot(runtime_state, path)

            with open(path) as json_file:
                raw = json.load(json_file)

            self.assertEqual(
                raw,
                {
                    "pi4_state": 7,
                    "sys_flags": 1,
                    "sheet_id": "sheet-123",
                    "last_stored_timestamp": "27-05-2026 08:59:12",
                },
            )

            helper_snapshot = load_snapshot(path)
            self.assertIsInstance(helper_snapshot, RuntimeState)
            self.assertEqual(helper_snapshot.to_dict(), raw)

    def test_snapshot_save_accepts_runtime_state_and_preserves_legacy_shape(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "snapshot.json")

            save_snapshot(
                RuntimeState(
                    pi4_state=3,
                    sys_flags=4,
                    sheet_id=None,
                    last_stored_timestamp=datetime(2026, 5, 28, 10, 11, 12),
                ),
                path,
            )

            with open(path) as json_file:
                self.assertEqual(
                    json.load(json_file),
                    {
                        "pi4_state": 3,
                        "sys_flags": 4,
                        "sheet_id": None,
                        "last_stored_timestamp": "28-05-2026 10:11:12",
                    },
                )

    def test_removed_model_helper_no_longer_exists(self):
        removed_helper = "_".join(("load_snapshot", "model"))
        self.assertFalse(hasattr(snapshot_module, removed_helper))

    def test_persistence_wrapper_classes_do_not_exist(self):
        removed_snapshot_wrapper = "".join(("Snapshot", "Store"))
        removed_review_period_wrapper = "".join(("ReviewPeriod", "Store"))

        self.assertFalse(hasattr(snapshot_module, removed_snapshot_wrapper))
        self.assertFalse(hasattr(review_period_module, removed_review_period_wrapper))

    def test_review_period_load_save_preserves_json_shape(self):
        review_period = {
            "start_date": "05/01/2026",
            "end_date": "05/29/2026",
            "location": "att-dev-dk-01",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "review_period.json")

            save_review_period(ReviewPeriod.from_dict(review_period), path)

            review_period_model = load_review_period(path)
            self.assertIsInstance(review_period_model, ReviewPeriod)
            self.assertEqual(review_period_model.to_dict(), review_period)


if __name__ == "__main__":
    unittest.main()
