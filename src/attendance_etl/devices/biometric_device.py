"""Project-owned biometric device access contracts."""

from abc import ABC, abstractmethod
from typing import Any, Sequence, Tuple

from attendance_etl.transform.transformation_request import ExtractedBiometricData


class BiometricDevice(ABC):
    """Runtime-facing contract for a commissioned biometric terminal."""

    def __init__(self, site_id: str):
        if not isinstance(site_id, str) or not site_id:
            raise ValueError("site_id must be a non-empty string")
        self._site_id = site_id

    @property
    def site_id(self) -> str:
        """Office site identity for this commissioned device."""
        return self._site_id

    @abstractmethod
    def extract_biometric_data(self) -> ExtractedBiometricData:
        """Return raw biometric data extracted from the device."""

    @abstractmethod
    def pull_records(self) -> Tuple[Sequence[Any], Sequence[Any]]:
        """Return raw device users and raw attendance records."""

    @abstractmethod
    def clear_records(self) -> None:
        """Clear attendance records from the device."""
