import unittest
from datetime import datetime
from types import SimpleNamespace

from attendance_etl.models import Employee
from attendance_etl.transform.zkteco_records import convert_to_dict, convert_to_map, decode_zk_format, filter_records


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

    def test_convert_to_map_returns_employee_values_by_user_id(self):
        users = [SimpleNamespace(user_id=10042, name="Ayesha Khan")]

        user_mapping = convert_to_map(users)

        self.assertEqual(list(user_mapping), [10042])
        self.assertIsInstance(user_mapping[10042], Employee)
        self.assertEqual(user_mapping[10042].employee_id, "10042")
        self.assertEqual(user_mapping[10042].name, "Ayesha Khan")
        self.assertIsNone(user_mapping[10042].source_device_id)

    def test_convert_to_dict_preserves_external_attendance_payload_shape(self):
        users = [SimpleNamespace(user_id="10042", name="Ayesha Khan")]
        record = SimpleNamespace(user_id="10042", timestamp=datetime(2026, 5, 27, 8, 59, 12), punch=1)

        decoded = convert_to_dict(record, convert_to_map(users))

        self.assertEqual(
            decoded,
            {
                "username": "Ayesha Khan",
                "timestamp": "27-05-2026 08:59:12",
                "entry": "Check Out",
            },
        )
        self.assertEqual(set(decoded), {"username", "timestamp", "entry"})

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
