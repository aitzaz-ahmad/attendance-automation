import unittest
from datetime import datetime
from typing import runtime_checkable

from attendance_etl.models import AttendanceEvent, Employee, EventType, ISerializable, ReviewPeriod, RuntimeState

ISerializable = runtime_checkable(ISerializable)


class DomainModelTests(unittest.TestCase):
    def test_attendance_event_to_dict_preserves_current_backend_payload_shape(self):
        event = AttendanceEvent(
            site_id="att-dev-dk-01",
            employee=Employee(id="10042", name="Ayesha Khan"),
            event_type=EventType.CLOCK_IN,
            timestamp=datetime(2026, 5, 27, 8, 59, 12),
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

    def test_attendance_event_contract_contains_only_canonical_fields(self):
        employee = Employee(id="10042", name="Ayesha Khan")
        event = AttendanceEvent(
            site_id="att-dev-dk-01",
            employee=employee,
            event_type=EventType.CLOCK_OUT,
            timestamp=datetime(2026, 5, 27, 17, 45, 3),
        )

        self.assertTrue(issubclass(AttendanceEvent, ISerializable))
        self.assertEqual(event.site_id, "att-dev-dk-01")
        self.assertIs(event.employee, employee)
        self.assertEqual(event.event_type, EventType.CLOCK_OUT)
        self.assertEqual(event.timestamp, datetime(2026, 5, 27, 17, 45, 3))
        self.assertEqual(
            set(event.__dataclass_fields__),
            {"site_id", "employee", "event_type", "timestamp"},
        )
        self.assertFalse(hasattr(AttendanceEvent, "from_dict"))

    def test_attendance_event_contract_removes_legacy_fields(self):
        event = AttendanceEvent(
            site_id="att-dev-dk-01",
            employee=Employee(id="10042", name="Ayesha Khan"),
            event_type=EventType.CLOCK_IN,
            timestamp=datetime(2026, 5, 27, 8, 59, 12),
        )

        for field_name in (
            "source_device_id",
            "event_timestamp",
            "employee_name",
            "raw_event_type",
            "employee_id",
            "event_id",
            "ingested_at",
            "source_record_id",
            "metadata",
        ):
            self.assertFalse(hasattr(event, field_name))

    def test_event_type_values_preserve_backend_payload_contract(self):
        self.assertEqual(EventType.CLOCK_IN.value, "Check In")
        self.assertEqual(EventType.CLOCK_OUT.value, "Check Out")

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
