"""Project-owned biometric device access contracts."""

from typing import Any, Protocol, Sequence, Tuple


class DeviceClient(Protocol):
    """Runtime-facing contract for biometric device access."""

    def pull_records(self) -> Tuple[Sequence[Any], Sequence[Any]]:
        """Return raw device users and raw attendance records."""
        ...

    def clear_records(self) -> None:
        """Clear attendance records from the device."""
        ...
