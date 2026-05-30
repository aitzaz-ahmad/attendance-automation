from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

REVIEW_PERIOD_DATE_FORMAT = "%m/%d/%Y"


@dataclass
class ReviewPeriod:
    """Review-period contract backed by the legacy review_period.json shape."""

    start: datetime
    end: datetime
    sheet_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "start_date": self.start.strftime(REVIEW_PERIOD_DATE_FORMAT),
            "end_date": self.end.strftime(REVIEW_PERIOD_DATE_FORMAT),
        }
        if self.sheet_id is not None:
            payload["sheet_id"] = self.sheet_id
        payload.update(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ReviewPeriod":
        metadata = {key: value for key, value in payload.items() if key not in {"start_date", "end_date", "sheet_id"}}
        return cls(
            start=datetime.strptime(payload["start_date"], REVIEW_PERIOD_DATE_FORMAT),
            end=datetime.strptime(payload["end_date"], REVIEW_PERIOD_DATE_FORMAT),
            sheet_id=payload.get("sheet_id"),
            metadata=metadata,
        )
