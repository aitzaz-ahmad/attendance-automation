import unittest
from datetime import datetime
from types import SimpleNamespace
from typing import Any, get_args, get_origin

from attendance_etl.models import Employee, EventType
from attendance_etl.transform import ExtractedBiometricData, NormalisedAttendance, TimeRange, TransformationRequest


class TransformationFoundationTests(unittest.TestCase):
    def test_event_type_values_are_vendor_neutral(self):
        self.assertEqual(EventType.CLOCK_IN.value, "Check In")
        self.assertEqual(EventType.CLOCK_OUT.value, "Check Out")
        self.assertEqual(set(EventType), {EventType.CLOCK_IN, EventType.CLOCK_OUT})

    def test_extracted_biometric_data_structure_groups_raw_device_data(self):
        employees = [SimpleNamespace(user_id="10042")]
        attendance_records = [SimpleNamespace(user_id="10042")]

        raw_data = ExtractedBiometricData(employees=employees, attendance_records=attendance_records)

        self.assertEqual(raw_data.employees, employees)
        self.assertEqual(raw_data.attendance_records, attendance_records)
        self.assertEqual(set(raw_data.__dataclass_fields__), {"employees", "attendance_records"})

    def test_time_range_structure_defaults_end_time_to_none(self):
        start_time = datetime(2026, 5, 27, 8, 0, 0)

        time_range = TimeRange(start_time=start_time)

        self.assertEqual(time_range.start_time, start_time)
        self.assertIsNone(time_range.end_time)
        self.assertEqual(set(time_range.__dataclass_fields__), {"start_time", "end_time"})

    def test_transformation_request_structure_groups_request_context(self):
        raw_data = ExtractedBiometricData(employees=[], attendance_records=[])
        time_range = TimeRange(start_time=datetime(2026, 5, 27, 8, 0, 0))

        request = TransformationRequest(raw_data=raw_data, site_id="dk", time_range=time_range)

        self.assertEqual(request.raw_data, raw_data)
        self.assertEqual(request.site_id, "dk")
        self.assertEqual(request.time_range, time_range)
        self.assertEqual(set(request.__dataclass_fields__), {"raw_data", "site_id", "time_range"})

    def test_normalised_attendance_structure_composes_employee_and_event_type(self):
        employee = Employee(id="10042", name="Ayesha Khan")
        timestamp = datetime(2026, 5, 27, 8, 59, 12)

        attendance = NormalisedAttendance(employee=employee, punch=EventType.CLOCK_IN, timestamp=timestamp)

        self.assertEqual(attendance.employee, employee)
        self.assertEqual(attendance.punch, EventType.CLOCK_IN)
        self.assertEqual(attendance.timestamp, timestamp)
        self.assertEqual(set(attendance.__dataclass_fields__), {"employee", "punch", "timestamp"})

        annotations = NormalisedAttendance.__annotations__
        self.assertIs(annotations["employee"], Employee)
        self.assertIs(annotations["punch"], EventType)
        self.assertIs(annotations["timestamp"], datetime)

    def test_transformation_foundation_dataclasses_are_frozen(self):
        self.assertTrue(ExtractedBiometricData.__dataclass_params__.frozen)
        self.assertTrue(TimeRange.__dataclass_params__.frozen)
        self.assertTrue(TransformationRequest.__dataclass_params__.frozen)
        self.assertTrue(NormalisedAttendance.__dataclass_params__.frozen)

    def test_extracted_data_annotations_accept_raw_sequences(self):
        annotations = ExtractedBiometricData.__annotations__

        self.assertIs(get_origin(annotations["employees"]), get_origin(annotations["attendance_records"]))
        self.assertEqual(get_args(annotations["employees"]), (Any,))
        self.assertEqual(get_args(annotations["attendance_records"]), (Any,))


if __name__ == "__main__":
    unittest.main()
