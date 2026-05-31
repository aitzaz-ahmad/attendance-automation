"""Biometric device configuration contracts and loading."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Set, Union

ZKTECO_VENDOR = "zkteco"


@dataclass(frozen=True)
class VendorOptions:
    """Base abstraction for vendor-specific device options."""

    pass


@dataclass(frozen=True)
class ZKTecoOptions(VendorOptions):
    """ZKTeco-specific device connection options."""

    ip_address: str
    comm_port: int
    timeout: Optional[int] = None
    force_udp: Optional[bool] = None
    ommit_ping: Optional[bool] = None


@dataclass(frozen=True)
class BiometricDeviceConfig:
    """Vendor-neutral biometric device configuration."""

    site_id: str
    vendor: str
    device_options: VendorOptions


class BiometricDeviceConfigBuilder:
    """Load and validate biometric device configuration."""

    @classmethod
    def from_json_file(cls, path: Union[str, Path]) -> BiometricDeviceConfig:
        config_path = Path(path)
        try:
            with config_path.open() as json_file:
                payload = json.load(json_file)
        except FileNotFoundError as exc:
            raise FileNotFoundError("Biometric device config file not found: {}".format(config_path)) from exc
        except json.JSONDecodeError as exc:
            raise ValueError("Invalid biometric device config JSON: {}".format(config_path)) from exc
        except OSError as exc:
            raise OSError("Biometric device config file is not readable: {}".format(config_path)) from exc

        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Any) -> BiometricDeviceConfig:
        root = _require_mapping(payload, "biometric device config")
        site_id = _required_string(root, "site_id", "biometric device config field")
        vendor = _required_string(root, "vendor", "biometric device config field")
        device_options = _required_mapping(root, "device_options", "biometric device config field")

        if vendor == ZKTECO_VENDOR:
            return BiometricDeviceConfig(
                site_id=site_id,
                vendor=vendor,
                device_options=cls._zkteco_options(device_options),
            )

        raise ValueError("Unsupported device vendor: {}".format(vendor))

    @staticmethod
    def _zkteco_options(payload: Mapping[str, Any]) -> ZKTecoOptions:
        allowed_keys = {
            "ip_address",
            "comm_port",
            "timeout",
            "force_udp",
            "ommit_ping",
        }
        _reject_unknown_keys(payload, allowed_keys, "ZKTeco option")

        return ZKTecoOptions(
            ip_address=_required_string(payload, "ip_address", "ZKTeco option"),
            comm_port=_required_integer(payload, "comm_port", "ZKTeco option"),
            timeout=_optional_integer(payload, "timeout", "ZKTeco option"),
            force_udp=_optional_boolean(payload, "force_udp", "ZKTeco option"),
            ommit_ping=_optional_boolean(payload, "ommit_ping", "ZKTeco option"),
        )


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("{} must be an object".format(label))
    return value


def _required_mapping(payload: Mapping[str, Any], field_name: str, label: str) -> Mapping[str, Any]:
    if field_name not in payload:
        raise ValueError("Missing required {}: {}".format(label, field_name))
    return _require_mapping(payload[field_name], "{} {}".format(label, field_name))


def _required_string(payload: Mapping[str, Any], field_name: str, label: str) -> str:
    if field_name not in payload:
        raise ValueError("Missing required {}: {}".format(label, field_name))

    value = payload[field_name]
    if not isinstance(value, str) or not value:
        raise ValueError("{} {} must be a non-empty string".format(label, field_name))

    return value


def _required_integer(payload: Mapping[str, Any], field_name: str, label: str) -> int:
    if field_name not in payload:
        raise ValueError("Missing required {}: {}".format(label, field_name))

    value = payload[field_name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("{} {} must be an integer".format(label, field_name))

    return value


def _optional_integer(payload: Mapping[str, Any], field_name: str, label: str) -> Optional[int]:
    if field_name not in payload:
        return None

    value = payload[field_name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("{} {} must be an integer".format(label, field_name))

    return value


def _optional_boolean(payload: Mapping[str, Any], field_name: str, label: str) -> Optional[bool]:
    if field_name not in payload:
        return None

    value = payload[field_name]
    if not isinstance(value, bool):
        raise ValueError("{} {} must be a boolean".format(label, field_name))

    return value


def _reject_unknown_keys(payload: Mapping[str, Any], allowed_keys: Set[str], label: str) -> None:
    unknown_keys = sorted(set(payload) - allowed_keys)
    if unknown_keys:
        raise ValueError("Unsupported {}: {}".format(label, unknown_keys[0]))
