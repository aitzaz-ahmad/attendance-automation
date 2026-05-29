import unittest
from datetime import datetime
from types import SimpleNamespace

from attendance_etl.transform.zkteco_records import convert_to_map, decode_zk_format, filter_records


class ZKTecoRecordTransformTests(unittest.TestCase):
    def test_decode_preserves_transitional_record_fields(self):
        users = [SimpleNamespace(user_id="10042", name="Ayesha Khan")]
        records = [
            SimpleNamespace(user_id="10042", timestamp=datetime(2026, 5, 27, 8, 59, 12), punch=0),
            SimpleNamespace(user_id="deleted-user", timestamp=datetime(2026, 5, 27, 9, 5, 0), punch=1),
        ]

        decoded = decode_zk_format(records, convert_to_map(users), "att-dev-dk-01")

        self.assertEqual(
            decoded,
            [
                {
                    "username": "Ayesha Khan",
                    "timestamp": "27-05-2026 08:59:12",
                    "entry": "Check In",
                    "device": "att-dev-dk-01",
                }
            ],
        )

    def test_filter_records_preserves_exclusive_start_and_inclusive_end(self):
        records = [
            SimpleNamespace(timestamp=datetime(2026, 5, 27, 8, 0, 0)),
            SimpleNamespace(timestamp=datetime(2026, 5, 27, 9, 0, 0)),
            SimpleNamespace(timestamp=datetime(2026, 5, 27, 10, 0, 0)),
            SimpleNamespace(timestamp=datetime(2026, 5, 27, 11, 0, 0)),
        ]

        filtered = filter_records(
            records,
            datetime(2026, 5, 27, 8, 0, 0),
            datetime(2026, 5, 27, 10, 0, 0),
        )

        self.assertEqual(filtered, records[1:3])
        self.assertEqual(filter_records(records, datetime(2026, 5, 27, 8, 0, 0)), records[1:])


if __name__ == "__main__":
    unittest.main()
