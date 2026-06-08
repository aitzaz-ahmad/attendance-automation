import unittest
from datetime import datetime

from attendance_etl.models import Employee, EventType
from attendance_etl.transform import ExtractedBiometricData, NormalisedAttendance, ZKTecoTransformationStrategy


class FakeUser:
    def __init__(self, user_id, name):
        self.user_id = user_id
        self.name = name


class FakeAttendanceRecord:
    def __init__(self, user_id, timestamp, punch):
        self.user_id = user_id
        self.timestamp = timestamp
        self.punch = punch


class ZKTecoTransformationStrategyTests(unittest.TestCase):
    def test_zkteco_users_are_normalised_to_employee(self):
        timestamp = datetime(2026, 5, 27, 8, 59, 12)
        raw_data = ExtractedBiometricData(
            employees=[FakeUser(10042, "Ayesha Khan")],
            attendance_records=[FakeAttendanceRecord(10042, timestamp, 0)],
        )

        normalised = ZKTecoTransformationStrategy().normalise(raw_data)

        self.assertEqual(len(normalised), 1)
        self.assertEqual(normalised[0].employee, Employee(id="10042", name="Ayesha Khan"))

    def test_zkteco_attendance_records_are_normalised_to_normalised_attendance(self):
        timestamp = datetime(2026, 5, 27, 8, 59, 12)
        raw_data = ExtractedBiometricData(
            employees=[FakeUser("10042", "Ayesha Khan")],
            attendance_records=[FakeAttendanceRecord("10042", timestamp, 0)],
        )

        normalised = ZKTecoTransformationStrategy().normalise(raw_data)

        self.assertEqual(
            normalised,
            [
                NormalisedAttendance(
                    employee=Employee(id="10042", name="Ayesha Khan"),
                    punch=EventType.CLOCK_IN,
                    timestamp=timestamp,
                )
            ],
        )

    def test_punch_zero_maps_to_clock_in(self):
        timestamp = datetime(2026, 5, 27, 8, 59, 12)
        raw_data = ExtractedBiometricData(
            employees=[FakeUser("10042", "Ayesha Khan")],
            attendance_records=[FakeAttendanceRecord("10042", timestamp, 0)],
        )

        normalised = ZKTecoTransformationStrategy().normalise(raw_data)

        self.assertEqual(normalised[0].punch, EventType.CLOCK_IN)

    def test_multiple_non_zero_punch_values_map_to_clock_out(self):
        timestamps = [
            datetime(2026, 5, 27, 9, 0, 0),
            datetime(2026, 5, 27, 12, 30, 0),
            datetime(2026, 5, 27, 17, 5, 0),
        ]
        raw_data = ExtractedBiometricData(
            employees=[FakeUser("10042", "Ayesha Khan")],
            attendance_records=[
                FakeAttendanceRecord("10042", timestamps[0], 1),
                FakeAttendanceRecord("10042", timestamps[1], 2),
                FakeAttendanceRecord("10042", timestamps[2], 255),
            ],
        )

        normalised = ZKTecoTransformationStrategy().normalise(raw_data)

        self.assertEqual([attendance.punch for attendance in normalised], [EventType.CLOCK_OUT] * 3)

    def test_unknown_or_deleted_users_are_skipped_without_placeholder_employees(self):
        known_timestamp = datetime(2026, 5, 27, 9, 0, 0)
        unknown_timestamp = datetime(2026, 5, 27, 10, 0, 0)
        raw_data = ExtractedBiometricData(
            employees=[FakeUser("10042", "Ayesha Khan")],
            attendance_records=[
                FakeAttendanceRecord("10042", known_timestamp, 0),
                FakeAttendanceRecord("deleted-user", unknown_timestamp, 1),
            ],
        )

        normalised = ZKTecoTransformationStrategy().normalise(raw_data)

        self.assertEqual(len(normalised), 1)
        self.assertEqual(normalised[0].employee, Employee(id="10042", name="Ayesha Khan"))
        self.assertNotEqual(normalised[0].employee.id, "deleted-user")
        self.assertNotEqual(normalised[0].timestamp, unknown_timestamp)

    def test_retained_records_preserve_input_ordering(self):
        timestamps = [
            datetime(2026, 5, 27, 17, 0, 0),
            datetime(2026, 5, 27, 8, 0, 0),
            datetime(2026, 5, 27, 12, 0, 0),
        ]
        raw_data = ExtractedBiometricData(
            employees=[FakeUser("10042", "Ayesha Khan")],
            attendance_records=[
                FakeAttendanceRecord("10042", timestamps[0], 1),
                FakeAttendanceRecord("10042", timestamps[1], 0),
                FakeAttendanceRecord("10042", timestamps[2], 2),
            ],
        )

        normalised = ZKTecoTransformationStrategy().normalise(raw_data)

        self.assertEqual([attendance.timestamp for attendance in normalised], timestamps)

    def test_temporary_lookup_is_not_exposed_on_strategy(self):
        strategy = ZKTecoTransformationStrategy()
        raw_data = ExtractedBiometricData(
            employees=[FakeUser("10042", "Ayesha Khan")],
            attendance_records=[FakeAttendanceRecord("10042", datetime(2026, 5, 27, 8, 0, 0), 0)],
        )

        strategy.normalise(raw_data)

        self.assertFalse(hasattr(strategy, "user_mapping"))
        self.assertFalse(hasattr(strategy, "employee_by_user_id"))


if __name__ == "__main__":
    unittest.main()
