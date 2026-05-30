from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class Employee:
    """Minimal employee representation used for source-device user mapping."""

    employee_id: str
    name: Optional[str] = None
    source_device_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_zkteco_user(cls, user: Any, source_device_id: Optional[str] = None) -> "Employee":
        return cls(
            employee_id=str(user.user_id),
            name=getattr(user, "name", None),
            source_device_id=source_device_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "employee_id": self.employee_id,
            "name": self.name,
            "source_device_id": self.source_device_id,
        }
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Employee":
        return cls(
            employee_id=payload["employee_id"],
            name=payload.get("name"),
            source_device_id=payload.get("source_device_id"),
            metadata=dict(payload.get("metadata", {})),
        )
