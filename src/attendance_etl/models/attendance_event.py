from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

ATTENDANCE_TIMESTAMP_FORMAT = "%d-%m-%Y %H:%M:%S"


class EventType(Enum):
    CLOCK_IN = "CLOCK_IN"
    CLOCK_OUT = "CLOCK_OUT"


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.strptime(value, ATTENDANCE_TIMESTAMP_FORMAT)
    except ValueError:
        pass

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


@dataclass
class AttendanceEvent:
    """Attendance event model prepared for current and canonical payloads."""

    source_device_id: str
    event_timestamp: datetime
    employee_name: Optional[str] = None
    raw_event_type: Optional[str] = None
    employee_id: Optional[str] = None
    event_type: Optional[str] = None
    event_id: Optional[str] = None
    ingested_at: Optional[datetime] = None
    source_record_id: Optional[str] = None
    site_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "username": self.employee_name,
            "timestamp": self.event_timestamp.strftime(ATTENDANCE_TIMESTAMP_FORMAT),
            "entry": self.raw_event_type or self.event_type,
            "device": self.source_device_id,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "AttendanceEvent":
        if "timestamp" in payload:
            return cls(
                source_device_id=payload["device"],
                event_timestamp=_parse_datetime(payload["timestamp"]),
                employee_name=payload.get("username"),
                raw_event_type=payload.get("entry"),
                event_type=payload.get("event_type"),
                employee_id=payload.get("employee_id"),
                event_id=payload.get("event_id"),
                ingested_at=_parse_datetime(payload["ingested_at"]) if payload.get("ingested_at") else None,
                source_record_id=payload.get("source_record_id"),
                site_id=payload.get("site_id"),
                metadata=dict(payload.get("metadata", {})),
            )

        return cls(
            source_device_id=payload["source_device_id"],
            event_timestamp=_parse_datetime(payload["event_timestamp"]),
            employee_name=payload.get("employee_name"),
            raw_event_type=payload.get("raw_event_type"),
            employee_id=payload.get("employee_id"),
            event_type=payload.get("event_type"),
            event_id=payload.get("event_id"),
            ingested_at=_parse_datetime(payload["ingested_at"]) if payload.get("ingested_at") else None,
            source_record_id=payload.get("source_record_id"),
            site_id=payload.get("site_id"),
            metadata=dict(payload.get("metadata", {})),
        )
