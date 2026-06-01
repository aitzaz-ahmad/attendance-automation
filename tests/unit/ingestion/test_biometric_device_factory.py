import ast
import inspect
import unittest
from pathlib import Path

from attendance_etl.devices.biometric_device import BiometricDevice
from attendance_etl.devices.biometric_device_config import (
    BiometricDeviceConfig,
    VendorOptions,
    ZKTECO_VENDOR,
    ZKTecoOptions,
)
from attendance_etl.devices.biometric_device_factory import BiometricDeviceFactory
from attendance_etl.devices.zkteco_device import ZKTecoDevice

REPO_ROOT = Path(__file__).resolve().parents[3]
FACTORY_PATH = REPO_ROOT / "src/attendance_etl/devices/biometric_device_factory.py"
RUNTIME_PATH = REPO_ROOT / "src/attendance_etl/pi4/runtime.py"


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


class BiometricDeviceFactoryTests(unittest.TestCase):
    def zkteco_config(self):
        return BiometricDeviceConfig(
            site_id="munich-office",
            vendor=ZKTECO_VENDOR,
            device_options=ZKTecoOptions(
                ip_address="192.0.2.10",
                comm_port=4370,
            ),
        )

    def test_create_accepts_only_biometric_device_config(self):
        create_signature = inspect.signature(BiometricDeviceFactory.create)

        self.assertEqual(list(create_signature.parameters), ["device_config"])
        self.assertIsNone(BiometricDeviceFactory.create.__defaults__)
        with self.assertRaisesRegex(TypeError, "^device_config must be BiometricDeviceConfig$"):
            BiometricDeviceFactory.create("192.0.2.10")

    def test_create_returns_biometric_device_for_zkteco_config(self):
        device = BiometricDeviceFactory.create(self.zkteco_config())

        self.assertIsInstance(device, BiometricDevice)

    def test_create_constructs_zkteco_device_for_zkteco_vendor(self):
        device = BiometricDeviceFactory.create(self.zkteco_config())

        self.assertIsInstance(device, ZKTecoDevice)
        self.assertEqual(device.site_id, "munich-office")
        self.assertEqual(device.device_ip, "192.0.2.10")
        self.assertEqual(device.comm_port, 4370)
        self.assertFalse(hasattr(device, "identifier"))

    def test_unsupported_vendor_fails_clearly(self):
        device_config = BiometricDeviceConfig(
            site_id="munich-office",
            vendor="hikvision",
            device_options=VendorOptions(),
        )

        with self.assertRaisesRegex(ValueError, "^Unsupported device vendor: hikvision$"):
            BiometricDeviceFactory.create(device_config)

    def test_invalid_vendor_options_fail_fast(self):
        device_config = BiometricDeviceConfig(
            site_id="munich-office",
            vendor=ZKTECO_VENDOR,
            device_options=VendorOptions(),
        )

        with self.assertRaisesRegex(ValueError, "^Invalid ZKTeco device options$"):
            BiometricDeviceFactory.create(device_config)

    def test_factory_has_no_plugin_registry_or_di_mechanism(self):
        source = FACTORY_PATH.read_text().lower()
        public_methods = {
            name for name, value in vars(BiometricDeviceFactory).items() if callable(value) and not name.startswith("_")
        }

        self.assertEqual(public_methods, {"create"})
        self.assertNotIn("plugin", source)
        self.assertNotIn("registry", source)
        self.assertNotIn("importlib", source)
        self.assertNotIn("__import__", source)
        self.assertNotIn("container", source)
        self.assertNotIn("inject", source)

    def test_runtime_uses_factory_without_direct_zkteco_construction(self):
        imported = imported_modules(RUNTIME_PATH)

        self.assertIn(
            "attendance_etl.devices.biometric_device_factory.BiometricDeviceFactory",
            imported,
        )
        self.assertNotIn("attendance_etl.devices.zkteco_device", imported)
        self.assertNotIn("attendance_etl.devices.zkteco_device.ZKTecoDevice", imported)
        self.assertFalse(calls_named(RUNTIME_PATH, "ZKTecoDevice"))
        self.assertTrue(calls_named(RUNTIME_PATH, "create"))


if __name__ == "__main__":
    unittest.main()
