from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Sequence

from attendance_etl.models import AttendanceEvent, EventType
from attendance_etl.transform.normalised_attendance import NormalisedAttendance
from attendance_etl.transform.transformation_request import ExtractedBiometricData, TimeRange, TransformationRequest


class TransformationStrategy(ABC):
    """Template method base class for biometric attendance transformation."""

    def transform(self, request: TransformationRequest) -> List[AttendanceEvent]:
        normalised_attendance = self.normalise(request.raw_data)
        self.validate(normalised_attendance)
        attendance_events = self.canonicalise(normalised_attendance, request.site_id)
        return self.filter(attendance_events, request.time_range)

    @abstractmethod
    def normalise(self, raw_data: ExtractedBiometricData) -> List[NormalisedAttendance]:
        raise NotImplementedError

    def validate(self, attendance: Sequence[NormalisedAttendance]) -> None:
        for index, attendance_record in enumerate(attendance):
            if not isinstance(attendance_record, NormalisedAttendance):
                raise ValueError("attendance[{}] must be NormalisedAttendance".format(index))

            employee_id = getattr(attendance_record.employee, "id", None)
            if not isinstance(employee_id, str) or employee_id == "":
                raise ValueError("attendance[{}].employee.id must be a non-empty string".format(index))

            employee_name = getattr(attendance_record.employee, "name", None)
            if not isinstance(employee_name, str) or employee_name == "":
                raise ValueError("attendance[{}].employee.name must be a non-empty string".format(index))

            if not isinstance(attendance_record.punch, EventType):
                raise ValueError("attendance[{}].punch must be EventType".format(index))

            if not isinstance(attendance_record.timestamp, datetime):
                raise ValueError("attendance[{}].timestamp must be datetime".format(index))

    def canonicalise(
        self,
        attendance: Sequence[NormalisedAttendance],
        site_id: str,
    ) -> List[AttendanceEvent]:
        return [
            AttendanceEvent(
                site_id=site_id,
                employee=attendance_record.employee,
                event_type=attendance_record.punch,
                timestamp=attendance_record.timestamp,
            )
            for attendance_record in attendance
        ]

    def filter(
        self,
        attendance_events: Sequence[AttendanceEvent],
        time_range: TimeRange,
    ) -> List[AttendanceEvent]:
        effective_end_time = time_range.end_time if time_range.end_time is not None else datetime.now()
        filtered_events = []

        for index, attendance_event in enumerate(attendance_events):
            if not isinstance(attendance_event, AttendanceEvent):
                raise ValueError("attendance_events[{}] must be AttendanceEvent".format(index))

            if time_range.start_time <= attendance_event.timestamp <= effective_end_time:
                filtered_events.append(attendance_event)

        return filtered_events
