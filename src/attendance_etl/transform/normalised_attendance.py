from dataclasses import dataclass
from datetime import datetime

from attendance_etl.models import Employee, EventType


@dataclass(frozen=True)
class NormalisedAttendance:
    """Internal attendance representation used before canonicalisation."""

    employee: Employee
    punch: EventType
    timestamp: datetime
