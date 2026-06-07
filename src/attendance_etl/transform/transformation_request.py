from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence


@dataclass(frozen=True)
class ExtractedBiometricData:
    """Raw biometric-device data supplied to the transformation layer."""

    employees: Sequence[Any]
    attendance_records: Sequence[Any]


@dataclass(frozen=True)
class TimeRange:
    """Filtering boundary for transformation requests."""

    start_time: datetime
    end_time: Optional[datetime] = None


@dataclass(frozen=True)
class TransformationRequest:
    """Grouped inputs for a transformation operation."""

    raw_data: ExtractedBiometricData
    site_id: str
    time_range: TimeRange
