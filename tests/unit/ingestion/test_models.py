import unittest
from datetime import datetime

from attendance_etl.models import AttendanceEvent, Employee, ISerializable, ReviewPeriod, RuntimeState


class DomainModelTests(unittest.TestCase):
    def test_attendance_event_to_dict_preserves_current_backend_payload_shape(self):
        event = AttendanceEvent(
            source_device_id="att-dev-dk-01",
            event_timestamp=datetime(2026, 5, 27, 8, 59, 12),
            employee_name="Ayesha Khan",
            raw_event_type="Check In",
        )

        self.assertEqual(
            event.to_dict(),
            {
                "username": "Ayesha Khan",
                "timestamp": "27-05-2026 08:59:12",
                "entry": "Check In",
                "device": "att-dev-dk-01",
            },
        )

    def test_attendance_event_from_dict_accepts_current_backend_payload_shape(self):
        event = AttendanceEvent.from_dict(
            {
                "username": "Ayesha Khan",
                "timestamp": "2026-05-27T08:59:12Z",
                "entry": "Check In",
                "device": "att-dev-dk-01",
            }
        )

        self.assertEqual(event.employee_name, "Ayesha Khan")
        self.assertEqual(event.event_timestamp.isoformat(), "2026-05-27T08:59:12+00:00")
        self.assertEqual(event.raw_event_type, "Check In")
        self.assertEqual(event.source_device_id, "att-dev-dk-01")
        self.assertIsNone(event.event_id)
        self.assertIsNone(event.employee_id)
        self.assertIsNone(event.ingested_at)
        self.assertEqual(event.metadata, {})

    def test_attendance_event_model_retains_internal_semantics(self):
        event = AttendanceEvent(
            source_device_id="att-dev-dk-01",
            event_timestamp=datetime(2026, 5, 27, 8, 59, 12),
            employee_name="Ayesha Khan",
            raw_event_type="Check In",
            employee_id="10042",
            event_type="clock_in",
            event_id="att-dev-dk-01-10042-2026-05-27T08:59:12Z",
            ingested_at=datetime(2026, 5, 27, 9, 0, 3),
            source_record_id="zk-879221",
            site_id="dk",
            metadata={"source_format": "zkteco"},
        )

        self.assertEqual(event.event_id, "att-dev-dk-01-10042-2026-05-27T08:59:12Z")
        self.assertEqual(event.source_device_id, "att-dev-dk-01")
        self.assertEqual(event.employee_id, "10042")
        self.assertEqual(event.event_timestamp, datetime(2026, 5, 27, 8, 59, 12))
        self.assertEqual(event.event_type, "clock_in")
        self.assertEqual(event.ingested_at, datetime(2026, 5, 27, 9, 0, 3))
        self.assertEqual(event.source_record_id, "zk-879221")
        self.assertEqual(event.employee_name, "Ayesha Khan")
        self.assertEqual(event.site_id, "dk")
        self.assertEqual(event.raw_event_type, "Check In")
        self.assertEqual(event.metadata, {"source_format": "zkteco"})

    def test_employee_contract_contains_only_normalised_identity(self):
        employee = Employee(id="10042", name="Ayesha Khan")

        self.assertIn(ISerializable, Employee.__mro__)
        self.assertEqual(employee.id, "10042")
        self.assertEqual(employee.name, "Ayesha Khan")
        self.assertEqual(set(employee.__dataclass_fields__), {"id", "name"})
        self.assertFalse(hasattr(Employee, "from_dict"))
        self.assertFalse(hasattr(Employee, "from_zkteco_user"))

    def test_employee_to_dict_serializes_normalised_identity(self):
        employee = Employee(id="10042", name="Ayesha Khan")

        self.assertEqual(employee.to_dict(), {"id": "10042", "name": "Ayesha Khan"})

    def test_review_period_round_trip_preserves_legacy_keys_and_metadata(self):
        payload = {
            "month": "May",
            "start_date": "05/01/2026",
            "end_date": "05/29/2026",
            "duration_weeks": 4,
            "sheetname": "May Attendance",
        }

        review_period = ReviewPeriod.from_dict(payload)

        self.assertEqual(review_period.start, datetime(2026, 5, 1))
        self.assertEqual(review_period.end, datetime(2026, 5, 29))
        self.assertEqual(review_period.to_dict(), payload)

    def test_runtime_state_round_trip_preserves_snapshot_keys_and_timestamp_format(self):
        payload = {
            "pi4_state": 7,
            "sys_flags": 1,
            "sheet_id": "sheet-123",
            "last_stored_timestamp": "27-05-2026 08:59:12",
        }

        runtime_state = RuntimeState.from_dict(payload)
        serialized = runtime_state.to_dict()

        self.assertEqual(runtime_state.last_stored_timestamp, datetime(2026, 5, 27, 8, 59, 12))
        self.assertEqual(
            set(serialized),
            {"pi4_state", "sys_flags", "sheet_id", "last_stored_timestamp"},
        )
        self.assertEqual(serialized, payload)

    def test_runtime_state_round_trip_preserves_null_timestamp_and_sheet_id(self):
        payload = {
            "pi4_state": 1,
            "sys_flags": 0,
            "sheet_id": None,
            "last_stored_timestamp": None,
        }

        self.assertEqual(RuntimeState.from_dict(payload).to_dict(), payload)


if __name__ == "__main__":
    unittest.main()
