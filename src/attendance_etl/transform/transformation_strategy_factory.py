"""Transformation strategy composition boundary."""

from attendance_etl.devices.biometric_device_config import ZKTECO_VENDOR
from attendance_etl.transform.transformation_strategy import TransformationStrategy
from attendance_etl.transform.zkteco_transformation_strategy import ZKTecoTransformationStrategy


class TransformationStrategyFactory:
    @staticmethod
    def create(vendor: str) -> TransformationStrategy:
        if vendor == ZKTECO_VENDOR:
            return ZKTecoTransformationStrategy()

        raise ValueError("Unsupported transformation strategy vendor: {}".format(vendor))
