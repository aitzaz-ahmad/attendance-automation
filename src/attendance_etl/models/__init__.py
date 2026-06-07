"""Core domain models shared across attendance ETL modules."""

from attendance_etl.models.attendance_event import AttendanceEvent, EventType
from attendance_etl.models.employee import Employee
from attendance_etl.models.interfaces import IDeserializable, ISerializable
from attendance_etl.models.review_period import ReviewPeriod
from attendance_etl.models.runtime_state import RuntimeState

__all__ = [
    "AttendanceEvent",
    "Employee",
    "EventType",
    "IDeserializable",
    "ISerializable",
    "ReviewPeriod",
    "RuntimeState",
]
