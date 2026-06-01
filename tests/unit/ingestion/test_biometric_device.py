import ast
import inspect
import unittest
from pathlib import Path
from typing import Any, Sequence, Tuple

from attendance_etl.devices.biometric_device import BiometricDevice
from attendance_etl.devices.biometric_device_config import ZKTecoOptions
from attendance_etl.devices.zkteco_device import ZKTecoDevice

REPO_ROOT = Path(__file__).resolve().parents[3]


class GenericDevice:
    def __init__(self):
        self.cleared = False

    def pull_records(self):
        return ["user-1"], ["record-1"]

    def clear_records(self):
        self.cleared = True


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


def imports_zk_sdk(path):
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == "zk" or alias.name.startswith("zk.") for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "zk" or node.module.startswith("zk."):
                return True

    return False


class BiometricDeviceTests(unittest.TestCase):
    def test_biometric_device_module_has_no_vendor_sdk_dependency(self):
        import attendance_etl.devices.biometric_device as biometric_device_module

        module_globals = vars(biometric_device_module)

        self.assertNotIn("zk", module_globals)

    def test_biometric_device_exposes_minimal_audited_contract(self):
        public_methods = {
            name for name, value in vars(BiometricDevice).items() if callable(value) and not name.startswith("_")
        }

        self.assertEqual(public_methods, {"pull_records", "clear_records"})

        pull_records_signature = inspect.signature(BiometricDevice.pull_records)
        clear_records_signature = inspect.signature(BiometricDevice.clear_records)
        self.assertEqual(list(pull_records_signature.parameters), ["self"])
        self.assertEqual(
            pull_records_signature.return_annotation,
            Tuple[Sequence[Any], Sequence[Any]],
        )
        self.assertEqual(list(clear_records_signature.parameters), ["self"])
        self.assertIs(clear_records_signature.return_annotation, None)

    def test_biometric_device_can_type_non_zkteco_implementation(self):
        device = GenericDevice()

        typed_device: BiometricDevice = device
        users, records = typed_device.pull_records()
        typed_device.clear_records()

        self.assertEqual(users, ["user-1"])
        self.assertEqual(records, ["record-1"])
        self.assertTrue(device.cleared)

    def test_zkteco_device_satisfies_biometric_device(self):
        device = ZKTecoDevice(
            ZKTecoOptions(
                ip_address="192.0.2.10",
                comm_port=4370,
            )
        )

        typed_device: BiometricDevice = device

        self.assertIs(typed_device, device)
        self.assertIsInstance(device, BiometricDevice)

    def test_zkteco_device_public_adapter_api_is_minimal(self):
        public_methods = {
            name for name, value in vars(ZKTecoDevice).items() if callable(value) and not name.startswith("_")
        }

        self.assertEqual(public_methods, {"pull_records", "clear_records"})
        self.assertEqual(list(inspect.signature(ZKTecoDevice.pull_records).parameters), ["self"])
        self.assertEqual(list(inspect.signature(ZKTecoDevice.clear_records).parameters), ["self"])
        self.assertFalse(hasattr(ZKTecoDevice, "connect"))
        self.assertFalse(hasattr(ZKTecoDevice, "disconnect"))

    def test_runtime_and_tests_do_not_import_old_device_paths(self):
        paths = [REPO_ROOT / "src/attendance_etl/pi4/runtime.py"]
        paths.extend((REPO_ROOT / "tests/unit/ingestion").glob("*.py"))

        imported = set()
        for path in paths:
            imported.update(imported_modules(path))

        self.assertNotIn("attendance_etl.device", imported)
        self.assertNotIn("attendance_etl.device.base", imported)
        self.assertNotIn("attendance_etl.device.zkteco", imported)
        self.assertNotIn("attendance_etl.devices.base", imported)
        self.assertNotIn("attendance_etl.devices.zkteco", imported)
        old_device_module = "attendance_etl.devices.{}".format("device_client")
        self.assertNotIn(old_device_module, imported)
        self.assertIn("attendance_etl.devices.zkteco_device", imported)
        self.assertIn("attendance_etl.devices.biometric_device", imported)

    def test_zkteco_sdk_import_isolated_to_zkteco_device_module(self):
        sdk_import_paths = {
            path.relative_to(REPO_ROOT)
            for path in (REPO_ROOT / "src/attendance_etl").rglob("*.py")
            if imports_zk_sdk(path)
        }

        self.assertEqual(
            sdk_import_paths,
            {Path("src/attendance_etl/devices/zkteco_device.py")},
        )


if __name__ == "__main__":
    unittest.main()
