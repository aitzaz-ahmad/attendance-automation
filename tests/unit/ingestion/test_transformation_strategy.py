import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from attendance_etl.models import AttendanceEvent, Employee, EventType
from attendance_etl.transform import (
    ExtractedBiometricData,
    NormalisedAttendance,
    TimeRange,
    TransformationRequest,
    TransformationStrategy,
)


class ConcreteTransformationStrategy(TransformationStrategy):
    def __init__(self, normalised_attendance=None):
        self.normalised_attendance = normalised_attendance or []

    def normalise(self, raw_data):
        return list(self.normalised_attendance)


class TransformationStrategyTests(unittest.TestCase):
    def test_transform_calls_stages_in_order(self):
        employee = Employee(id="10042", name="Ayesha Khan")
        timestamp = datetime(2026, 5, 27, 8, 59, 12)
        normalised_attendance = [NormalisedAttendance(employee=employee, punch=EventType.CLOCK_IN, timestamp=timestamp)]
        attendance_events = [
            AttendanceEvent(
                site_id="dk",
                employee=employee,
                event_type=EventType.CLOCK_IN,
                timestamp=timestamp,
            )
        ]
        test_case = self

        class RecordingTransformationStrategy(TransformationStrategy):
            def __init__(self):
                self.calls = []

            def normalise(self, raw_data):
                self.calls.append("normalise")
                return normalised_attendance

            def validate(self, attendance):
                self.calls.append("validate")
                test_case.assertIs(attendance, normalised_attendance)

            def canonicalise(self, attendance, site_id):
                self.calls.append("canonicalise")
                test_case.assertIs(attendance, normalised_attendance)
                test_case.assertEqual(site_id, "dk")
                return attendance_events

            def filter(self, attendance_events_to_filter, time_range):
                self.calls.append("filter")
                test_case.assertIs(attendance_events_to_filter, attendance_events)
                return attendance_events_to_filter

        strategy = RecordingTransformationStrategy()
        request = TransformationRequest(
            raw_data=ExtractedBiometricData(employees=[], attendance_records=[]),
            site_id="dk",
            time_range=TimeRange(start_time=datetime(2026, 5, 27, 0, 0, 0)),
        )

        self.assertEqual(strategy.transform(request), attendance_events)
        self.assertEqual(strategy.calls, ["normalise", "validate", "canonicalise", "filter"])

    def test_normalise_is_abstract_and_must_be_implemented_by_subclasses(self):
        with self.assertRaises(TypeError):
            TransformationStrategy()

    def test_validate_accepts_valid_normalised_attendance(self):
        attendance = [
            NormalisedAttendance(
                employee=Employee(id="10042", name="Ayesha Khan"),
                punch=EventType.CLOCK_IN,
                timestamp=datetime(2026, 5, 27, 8, 59, 12),
            )
        ]

        ConcreteTransformationStrategy().validate(attendance)

    def test_validate_rejects_non_normalised_attendance(self):
        with self.assertRaisesRegex(ValueError, "NormalisedAttendance"):
            ConcreteTransformationStrategy().validate([object()])

    def test_validate_rejects_empty_employee_id(self):
        attendance = [
            NormalisedAttendance(
                employee=Employee(id="", name="Ayesha Khan"),
                punch=EventType.CLOCK_IN,
                timestamp=datetime(2026, 5, 27, 8, 59, 12),
            )
        ]

        with self.assertRaisesRegex(ValueError, "employee.id"):
            ConcreteTransformationStrategy().validate(attendance)

    def test_validate_rejects_empty_employee_name(self):
        attendance = [
            NormalisedAttendance(
                employee=Employee(id="10042", name=""),
                punch=EventType.CLOCK_IN,
                timestamp=datetime(2026, 5, 27, 8, 59, 12),
            )
        ]

        with self.assertRaisesRegex(ValueError, "employee.name"):
            ConcreteTransformationStrategy().validate(attendance)

    def test_validate_rejects_non_event_type_punch(self):
        attendance = [
            NormalisedAttendance(
                employee=Employee(id="10042", name="Ayesha Khan"),
                punch="Check In",
                timestamp=datetime(2026, 5, 27, 8, 59, 12),
            )
        ]

        with self.assertRaisesRegex(ValueError, "EventType"):
            ConcreteTransformationStrategy().validate(attendance)

    def test_validate_rejects_non_datetime_timestamp(self):
        attendance = [
            NormalisedAttendance(
                employee=Employee(id="10042", name="Ayesha Khan"),
                punch=EventType.CLOCK_IN,
                timestamp="27-05-2026 08:59:12",
            )
        ]

        with self.assertRaisesRegex(ValueError, "datetime"):
            ConcreteTransformationStrategy().validate(attendance)

    def test_canonicalise_produces_attendance_event(self):
        attendance = [
            NormalisedAttendance(
                employee=Employee(id="10042", name="Ayesha Khan"),
                punch=EventType.CLOCK_IN,
                timestamp=datetime(2026, 5, 27, 8, 59, 12),
            )
        ]

        events = ConcreteTransformationStrategy().canonicalise(attendance, "dk")

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], AttendanceEvent)

    def test_canonicalise_maps_site_id_employee_punch_and_timestamp_exactly(self):
        employee = Employee(id="10042", name="Ayesha Khan")
        timestamp = datetime(2026, 5, 27, 8, 59, 12)
        attendance = [NormalisedAttendance(employee=employee, punch=EventType.CLOCK_OUT, timestamp=timestamp)]

        event = ConcreteTransformationStrategy().canonicalise(attendance, "att-dev-dk-01")[0]

        self.assertEqual(event.site_id, "att-dev-dk-01")
        self.assertIs(event.employee, employee)
        self.assertEqual(event.event_type, EventType.CLOCK_OUT)
        self.assertIs(event.timestamp, timestamp)

    def test_filter_uses_inclusive_boundaries(self):
        employee = Employee(id="10042", name="Ayesha Khan")
        before_start = AttendanceEvent("dk", employee, EventType.CLOCK_IN, datetime(2026, 5, 27, 7, 59, 59))
        at_start = AttendanceEvent("dk", employee, EventType.CLOCK_IN, datetime(2026, 5, 27, 8, 0, 0))
        middle = AttendanceEvent("dk", employee, EventType.CLOCK_OUT, datetime(2026, 5, 27, 12, 0, 0))
        at_end = AttendanceEvent("dk", employee, EventType.CLOCK_OUT, datetime(2026, 5, 27, 17, 0, 0))
        after_end = AttendanceEvent("dk", employee, EventType.CLOCK_OUT, datetime(2026, 5, 27, 17, 0, 1))
        time_range = TimeRange(
            start_time=datetime(2026, 5, 27, 8, 0, 0),
            end_time=datetime(2026, 5, 27, 17, 0, 0),
        )

        filtered_events = ConcreteTransformationStrategy().filter(
            [before_start, at_start, middle, at_end, after_end],
            time_range,
        )

        self.assertEqual(filtered_events, [at_start, middle, at_end])

    def test_filter_preserves_input_ordering(self):
        employee = Employee(id="10042", name="Ayesha Khan")
        later_event = AttendanceEvent("dk", employee, EventType.CLOCK_OUT, datetime(2026, 5, 27, 17, 0, 0))
        earlier_event = AttendanceEvent("dk", employee, EventType.CLOCK_IN, datetime(2026, 5, 27, 8, 0, 0))
        middle_event = AttendanceEvent("dk", employee, EventType.CLOCK_OUT, datetime(2026, 5, 27, 12, 0, 0))
        events = [later_event, earlier_event, middle_event]
        time_range = TimeRange(
            start_time=datetime(2026, 5, 27, 0, 0, 0),
            end_time=datetime(2026, 5, 27, 23, 59, 59),
        )

        filtered_events = ConcreteTransformationStrategy().filter(events, time_range)

        self.assertEqual(filtered_events, events)

    def test_filter_uses_datetime_now_when_time_range_end_time_is_none(self):
        employee = Employee(id="10042", name="Ayesha Khan")
        included_event = AttendanceEvent("dk", employee, EventType.CLOCK_IN, datetime(2026, 5, 27, 9, 0, 0))
        future_event = AttendanceEvent("dk", employee, EventType.CLOCK_OUT, datetime(2026, 5, 27, 10, 0, 1))
        time_range = TimeRange(start_time=datetime(2026, 5, 27, 8, 0, 0))

        with patch("attendance_etl.transform.transformation_strategy.datetime") as datetime_mock:
            datetime_mock.now.return_value = datetime(2026, 5, 27, 10, 0, 0)

            filtered_events = ConcreteTransformationStrategy().filter([included_event, future_event], time_range)

        datetime_mock.now.assert_called_once_with()
        self.assertEqual(filtered_events, [included_event])

    def test_filter_rejects_raw_sdk_records(self):
        raw_sdk_record = SimpleNamespace(timestamp=datetime(2026, 5, 27, 9, 0, 0))
        time_range = TimeRange(
            start_time=datetime(2026, 5, 27, 8, 0, 0),
            end_time=datetime(2026, 5, 27, 10, 0, 0),
        )

        with self.assertRaisesRegex(ValueError, "AttendanceEvent"):
            ConcreteTransformationStrategy().filter([raw_sdk_record], time_range)


if __name__ == "__main__":
    unittest.main()
