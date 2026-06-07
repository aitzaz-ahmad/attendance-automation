from dataclasses import dataclass
from typing import Any, Dict

from attendance_etl.models.interfaces import ISerializable


@dataclass(frozen=True)
class Employee(ISerializable):
    """Normalised employee identity used during transformation."""

    id: str
    name: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
        }
