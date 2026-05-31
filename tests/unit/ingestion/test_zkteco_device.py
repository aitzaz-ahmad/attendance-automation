import unittest

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

    def __init__(self, device_ip, port, timeout, force_udp, ommit_ping):
        self.device_ip = device_ip
        self.port = port
        self.timeout = timeout
        self.force_udp = force_udp
        self.ommit_ping = ommit_ping
        ZKSpy.events.append("instantiate")

    def connect(self):
        ZKSpy.events.append("connect")
        return ConnectionSpy(ZKSpy.events)


class ZKTecoDeviceTests(unittest.TestCase):
    def setUp(self):
        self.original_zk = zkteco_device.ZK
        zkteco_device.ZK = ZKSpy
        ZKSpy.events = []

    def tearDown(self):
        zkteco_device.ZK = self.original_zk

    def test_pull_records_preserves_connection_lifecycle(self):
        users, records = zkteco_device.ZKTecoDevice("192.0.2.10", 4370).pull_records()

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
        zkteco_device.ZKTecoDevice("192.0.2.10", 4370).clear_records()

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


if __name__ == "__main__":
    unittest.main()
