from typing import List

from attendance_etl.models import Employee, EventType
from attendance_etl.transform.normalised_attendance import NormalisedAttendance
from attendance_etl.transform.transformation_request import ExtractedBiometricData
from attendance_etl.transform.transformation_strategy import TransformationStrategy


class ZKTecoTransformationStrategy(TransformationStrategy):
    """Normalise raw ZKTeco users and attendance records."""

    def normalise(self, raw_data: ExtractedBiometricData) -> List[NormalisedAttendance]:
        employee_by_user_id = {
            user.user_id: Employee(
                id=str(user.user_id),
                name=user.name,
            )
            for user in raw_data.employees
        }

        normalised_attendance: List[NormalisedAttendance] = []
        for attendance_record in raw_data.attendance_records:
            if attendance_record.user_id not in employee_by_user_id:
                continue

            normalised_attendance.append(
                NormalisedAttendance(
                    employee=employee_by_user_id[attendance_record.user_id],
                    punch=EventType.CLOCK_IN if attendance_record.punch == 0 else EventType.CLOCK_OUT,
                    timestamp=attendance_record.timestamp,
                )
            )

        return normalised_attendance
