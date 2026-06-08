from typing import Any, Sequence, Tuple

from zk import ZK

from attendance_etl.devices.biometric_device import BiometricDevice
from attendance_etl.devices.biometric_device_config import ZKTecoOptions
from attendance_etl.logging_utils import get_logger
from attendance_etl.transform.transformation_request import ExtractedBiometricData

logger = get_logger("ZKTecoDevice")

DEFAULT_TIMEOUT = 10
DEFAULT_FORCE_UDP = False
DEFAULT_OMMIT_PING = False


class ZKTecoDevice(BiometricDevice):
    def __init__(self, site_id: str, options: ZKTecoOptions):
        super().__init__(site_id)
        self.device_ip = options.ip_address
        self.comm_port = options.comm_port
        self.timeout = DEFAULT_TIMEOUT if options.timeout is None else options.timeout
        self.force_udp = DEFAULT_FORCE_UDP if options.force_udp is None else options.force_udp
        self.ommit_ping = DEFAULT_OMMIT_PING if options.ommit_ping is None else options.ommit_ping

    def clear_records(self) -> None:
        self._clear_records_from_device()

    def extract_biometric_data(self) -> ExtractedBiometricData:
        logger.debug("extract_biometric_data invoked")
        logger.info("fetching data from site %s", self.site_id)
        users, records = self.pull_records()

        return ExtractedBiometricData(employees=users, attendance_records=records)

    def pull_records(self) -> Tuple[Sequence[Any], Sequence[Any]]:
        return self._pull_records_from_device()

    def _zk_client(self):
        return ZK(
            self.device_ip,
            port=self.comm_port,
            timeout=self.timeout,
            force_udp=self.force_udp,
            ommit_ping=self.ommit_ping,
        )

    def _clear_records_from_device(self):
        """
        interface for communicating with the ZKTeco biometric device to
        clear all the attendance records from the machine
        """
        conn = None
        zk = self._zk_client()
        try:
            logger.info("Connecting to device ...")
            conn = zk.connect()
            logger.info("Disabling device ...")
            conn.disable_device()
            logger.info("Firmware Version: : %s", conn.get_firmware_version())

            logger.info("deleting all attendance records stored on the biometric device")
            conn.clear_attendance()

            logger.info("Enabling device ...")
            conn.enable_device()
        except Exception as e:
            logger.error("Process terminate : %s", e)
        finally:
            if conn:
                conn.disconnect()

    def _pull_records_from_device(self):
        """
        interface for communicating with the ZKTeco biometric device to fetch
        the attendance records stored on the machine
        """
        conn = None
        users = []
        records = []
        zk = self._zk_client()
        try:
            logger.info("Connecting to device ...")
            conn = zk.connect()
            logger.info("Disabling device ...")
            conn.disable_device()
            logger.info("Firmware Version: : %s", conn.get_firmware_version())

            logger.info("Fetching list of users...")
            users = conn.get_users()
            logger.info("Fetching attendance records...")
            records = conn.get_attendance()

            logger.info("Enabling device ...")
            conn.enable_device()
        except Exception as e:
            logger.error("Process terminate : %s", e)
        finally:
            if conn:
                conn.disconnect()

        return users, records
