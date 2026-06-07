from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict

from attendance_etl.models.employee import Employee
from attendance_etl.models.interfaces import ISerializable

ATTENDANCE_TIMESTAMP_FORMAT = "%d-%m-%Y %H:%M:%S"


# TODO: These values preserve backend-compatible Pub/Sub payloads. Future
# canonical contract migration should change them to "Clock In" and "Clock Out"
# through a dedicated contract migration issue.
class EventType(Enum):
    CLOCK_IN = "Check In"
    CLOCK_OUT = "Check Out"


@dataclass(frozen=True)
class AttendanceEvent(ISerializable):
    """Canonical attendance event produced by the transformation layer."""

    site_id: str
    employee: Employee
    event_type: EventType
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        # TODO: This serialises to the legacy backend-compatible Pub/Sub shape:
        # {"username": ..., "timestamp": ..., "entry": ..., "device": ...}.
        # Long-term contract evolution should rename "timestamp" to
        # "event_timestamp", add "event_id", "ingested_at", and
        # "source_device_id", and preserve timezone information for event
        # timestamps. These changes are intentionally deferred to later
        # transformation/messaging-layer work and must be introduced through a
        # dedicated contract migration.
        return {
            "username": self.employee.name,
            "timestamp": self.timestamp.strftime(ATTENDANCE_TIMESTAMP_FORMAT),
            "entry": self.event_type.value,
            "device": self.site_id,
        }
