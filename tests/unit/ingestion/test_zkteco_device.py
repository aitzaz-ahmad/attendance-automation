import unittest

from attendance_etl.devices.biometric_device_config import ZKTecoOptions
from attendance_etl.devices import zkteco_device


class ConnectionSpy:
    def __init__(self, events):
        self.events = events

    def disable_device(self):
        self.events.append("disable_device")

    def enable_device(self):
        self.events.append("enable_device")

    def get_firmware_version(self):
        self.events.append("get_firmware_version")
        return "fake"

    def get_users(self):
        self.events.append("get_users")
        return ["user-1"]

    def get_attendance(self):
        self.events.append("get_attendance")
        return ["record-1"]

    def clear_attendance(self):
        self.events.append("clear_attendance")

    def disconnect(self):
        self.events.append("disconnect")


class ZKSpy:
    events = []
    instances = []

    def __init__(self, device_ip, port, timeout, force_udp, ommit_ping):
        self.device_ip = device_ip
        self.port = port
        self.timeout = timeout
        self.force_udp = force_udp
        self.ommit_ping = ommit_ping
        ZKSpy.events.append("instantiate")
        ZKSpy.instances.append(self)

    def connect(self):
        ZKSpy.events.append("connect")
        return ConnectionSpy(ZKSpy.events)


class ZKTecoDeviceTests(unittest.TestCase):
    def setUp(self):
        self.original_zk = zkteco_device.ZK
        zkteco_device.ZK = ZKSpy
        ZKSpy.events = []
        ZKSpy.instances = []

    def tearDown(self):
        zkteco_device.ZK = self.original_zk

    def zkteco_options(self, **overrides):
        values = {
            "ip_address": "192.0.2.10",
            "comm_port": 4370,
        }
        values.update(overrides)
        return ZKTecoOptions(**values)

    def test_pull_records_preserves_connection_lifecycle(self):
        users, records = zkteco_device.ZKTecoDevice(self.zkteco_options()).pull_records()

        self.assertEqual(users, ["user-1"])
        self.assertEqual(records, ["record-1"])
        self.assertEqual(
            ZKSpy.events,
            [
                "instantiate",
                "connect",
                "disable_device",
                "get_firmware_version",
                "get_users",
                "get_attendance",
                "enable_device",
                "disconnect",
            ],
        )

    def test_clear_records_preserves_connection_lifecycle(self):
        zkteco_device.ZKTecoDevice(self.zkteco_options()).clear_records()

        self.assertEqual(
            ZKSpy.events,
            [
                "instantiate",
                "connect",
                "disable_device",
                "get_firmware_version",
                "clear_attendance",
                "enable_device",
                "disconnect",
            ],
        )

    def test_zkteco_device_uses_explicit_connection_options(self):
        zkteco_device.ZKTecoDevice(
            self.zkteco_options(
                timeout=15,
                force_udp=True,
                ommit_ping=True,
            )
        ).pull_records()

        instance = ZKSpy.instances[0]
        self.assertEqual(instance.timeout, 15)
        self.assertIs(instance.force_udp, True)
        self.assertIs(instance.ommit_ping, True)


if __name__ == "__main__":
    unittest.main()
