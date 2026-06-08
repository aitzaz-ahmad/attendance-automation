import ast
import inspect
import unittest
from datetime import datetime
from pathlib import Path

from attendance_etl.devices.biometric_device import BiometricDevice
from attendance_etl.devices.biometric_device_config import ZKTecoOptions
from attendance_etl.devices import zkteco_device
from attendance_etl.transform import ExtractedBiometricData

REPO_ROOT = Path(__file__).resolve().parents[3]
ZKTECO_DEVICE_PATH = REPO_ROOT / "src/attendance_etl/devices/zkteco_device.py"


def imported_modules(path):
    module_names = set()
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            module_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            module_names.add(node.module)
            module_names.update("{}.".format(node.module) + alias.name for alias in node.names)

    return module_names


def calls_named(path, name):
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == name:
                return True
            if isinstance(node.func, ast.Attribute) and node.func.attr == name:
                return True

    return False


class FakeUser:
    def __init__(self, user_id, name):
        self.user_id = user_id
        self.name = name


class FakeAttendanceRecord:
    def __init__(self, user_id, timestamp, punch):
        self.user_id = user_id
        self.timestamp = timestamp
        self.punch = punch


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
        return list(ZKSpy.users)

    def get_attendance(self):
        self.events.append("get_attendance")
        return list(ZKSpy.records)

    def clear_attendance(self):
        self.events.append("clear_attendance")

    def disconnect(self):
        self.events.append("disconnect")


class ZKSpy:
    events = []
    instances = []
    users = ["user-1"]
    records = ["record-1"]

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
        ZKSpy.users = ["user-1"]
        ZKSpy.records = ["record-1"]

    def tearDown(self):
        zkteco_device.ZK = self.original_zk

    def zkteco_options(self, **overrides):
        values = {
            "ip_address": "192.0.2.10",
            "comm_port": 4370,
        }
        values.update(overrides)
        return ZKTecoOptions(**values)

    def test_zkteco_device_satisfies_biometric_device(self):
        device = zkteco_device.ZKTecoDevice("munich-office", self.zkteco_options())

        typed_device: BiometricDevice = device

        self.assertIs(typed_device, device)
        self.assertIsInstance(device, BiometricDevice)
        self.assertEqual(device.site_id, "munich-office")

    def test_zkteco_device_inherits_read_only_site_id_from_biometric_device(self):
        device = zkteco_device.ZKTecoDevice("munich-office", self.zkteco_options())

        self.assertIs(zkteco_device.ZKTecoDevice.site_id, BiometricDevice.site_id)
        self.assertEqual(device.site_id, "munich-office")
        with self.assertRaises(AttributeError):
            device.site_id = "berlin-office"

    def test_zkteco_device_public_adapter_api_is_minimal(self):
        public_methods = {
            name
            for name, value in vars(zkteco_device.ZKTecoDevice).items()
            if callable(value) and not name.startswith("_")
        }

        self.assertEqual(public_methods, {"extract_biometric_data", "pull_records", "clear_records"})
        self.assertEqual(
            list(inspect.signature(zkteco_device.ZKTecoDevice.extract_biometric_data).parameters),
            ["self"],
        )
        self.assertEqual(list(inspect.signature(zkteco_device.ZKTecoDevice.pull_records).parameters), ["self"])
        self.assertEqual(list(inspect.signature(zkteco_device.ZKTecoDevice.clear_records).parameters), ["self"])
        self.assertFalse(hasattr(zkteco_device.ZKTecoDevice, "connect"))
        self.assertFalse(hasattr(zkteco_device.ZKTecoDevice, "disconnect"))

    def test_pull_records_preserves_connection_lifecycle(self):
        users, records = zkteco_device.ZKTecoDevice("munich-office", self.zkteco_options()).pull_records()

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

    def test_extract_biometric_data_returns_raw_extracted_biometric_data(self):
        ZKSpy.users = [FakeUser(100, "Ada Lovelace")]
        ZKSpy.records = [
            FakeAttendanceRecord(100, datetime(2026, 5, 27, 7, 59, 59), 0),
            FakeAttendanceRecord(100, datetime(2026, 5, 27, 8, 1, 0), 1),
        ]

        raw_data = zkteco_device.ZKTecoDevice("munich-office", self.zkteco_options()).extract_biometric_data()

        self.assertIsInstance(raw_data, ExtractedBiometricData)
        self.assertEqual(raw_data.employees, ZKSpy.users)
        self.assertEqual(raw_data.attendance_records, ZKSpy.records)
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

    def test_zkteco_device_does_not_own_filtering_decoding_or_user_correlation(self):
        imported = imported_modules(ZKTECO_DEVICE_PATH)

        self.assertNotIn("attendance_etl.transform.zkteco_records", imported)
        self.assertNotIn("attendance_etl.transform.zkteco_records.convert_to_map", imported)
        self.assertNotIn("attendance_etl.transform.zkteco_records.decode_zk_format", imported)
        self.assertNotIn("attendance_etl.transform.zkteco_records.filter_records", imported)
        self.assertFalse(calls_named(ZKTECO_DEVICE_PATH, "convert_to_map"))
        self.assertFalse(calls_named(ZKTECO_DEVICE_PATH, "decode_zk_format"))
        self.assertFalse(calls_named(ZKTECO_DEVICE_PATH, "filter_records"))
        self.assertFalse(calls_named(ZKTECO_DEVICE_PATH, "to_dict"))

    def test_clear_records_preserves_connection_lifecycle(self):
        zkteco_device.ZKTecoDevice("munich-office", self.zkteco_options()).clear_records()

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
            "munich-office",
            self.zkteco_options(
                timeout=15,
                force_udp=True,
                ommit_ping=True,
            ),
        ).pull_records()

        instance = ZKSpy.instances[0]
        self.assertEqual(instance.timeout, 15)
        self.assertIs(instance.force_udp, True)
        self.assertIs(instance.ommit_ping, True)

    def test_zkteco_device_uses_implementation_defaults_for_absent_optional_options(self):
        zkteco_device.ZKTecoDevice("munich-office", self.zkteco_options()).pull_records()

        self.assertEqual(len(ZKSpy.instances), 1)
        instance = ZKSpy.instances[0]
        self.assertEqual(instance.device_ip, "192.0.2.10")
        self.assertEqual(instance.port, 4370)
        self.assertEqual(instance.timeout, zkteco_device.DEFAULT_TIMEOUT)
        self.assertEqual(instance.force_udp, zkteco_device.DEFAULT_FORCE_UDP)
        self.assertEqual(instance.ommit_ping, zkteco_device.DEFAULT_OMMIT_PING)


if __name__ == "__main__":
    unittest.main()
