from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

RUNTIME_TIMESTAMP_FORMAT = "%d-%m-%Y %H:%M:%S"


@dataclass
class RuntimeState:
    """Persisted ingestion runtime state backed by the legacy snapshot.json shape."""

    pi4_state: int
    sys_flags: int
    sheet_id: Optional[str]
    last_stored_timestamp: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        last_stored_timestamp = None
        if self.last_stored_timestamp is not None:
            last_stored_timestamp = self.last_stored_timestamp.strftime(RUNTIME_TIMESTAMP_FORMAT)

        return {
            "pi4_state": self.pi4_state,
            "sys_flags": self.sys_flags,
            "sheet_id": self.sheet_id,
            "last_stored_timestamp": last_stored_timestamp,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "RuntimeState":
        timestamp = payload["last_stored_timestamp"]
        if isinstance(timestamp, str):
            timestamp = datetime.strptime(timestamp, RUNTIME_TIMESTAMP_FORMAT)

        return cls(
            pi4_state=payload["pi4_state"],
            sys_flags=payload["sys_flags"],
            sheet_id=payload["sheet_id"],
            last_stored_timestamp=timestamp,
        )
