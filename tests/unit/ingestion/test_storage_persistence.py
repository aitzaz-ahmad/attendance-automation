import json
import os
import tempfile
import unittest

from attendance_etl.storage.review_period import load_review_period, save_review_period
from attendance_etl.storage.snapshot import load_snapshot, save_snapshot


class StoragePersistenceTests(unittest.TestCase):
    def test_snapshot_load_save_preserves_legacy_shape(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "snapshot.json")

            save_snapshot(
                pi4_state=7,
                system_flags=1,
                review_sheet_id="sheet-123",
                last_stored_timestamp="27-05-2026 08:59:12",
                path=path,
            )

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
            self.assertEqual(load_snapshot(path), raw)

    def test_review_period_load_save_preserves_json_shape(self):
        review_period = {
            "start_date": "05/01/2026",
            "end_date": "05/29/2026",
            "location": "att-dev-dk-01",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "review_period.json")

            save_review_period(review_period, path)

            self.assertEqual(load_review_period(path), review_period)


if __name__ == "__main__":
    unittest.main()
