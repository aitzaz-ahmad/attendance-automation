"""Biometric device composition boundary."""

from attendance_etl.devices.biometric_device import BiometricDevice
from attendance_etl.devices.biometric_device_config import BiometricDeviceConfig, ZKTECO_VENDOR, ZKTecoOptions
from attendance_etl.devices.zkteco_device import ZKTecoDevice


class BiometricDeviceFactory:
    @staticmethod
    def create(device_config: BiometricDeviceConfig) -> BiometricDevice:
        if not isinstance(device_config, BiometricDeviceConfig):
            raise TypeError("device_config must be BiometricDeviceConfig")

        if device_config.vendor == ZKTECO_VENDOR:
            if not isinstance(device_config.device_options, ZKTecoOptions):
                raise ValueError("Invalid ZKTeco device options")
            return ZKTecoDevice(device_config.device_options)

        raise ValueError("Unsupported device vendor: {}".format(device_config.vendor))
