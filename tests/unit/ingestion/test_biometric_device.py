import ast
import inspect
import unittest
from pathlib import Path
from typing import Any, Sequence, Tuple

from attendance_etl.devices.biometric_device import BiometricDevice
from attendance_etl.transform import ExtractedBiometricData

REPO_ROOT = Path(__file__).resolve().parents[3]


class DummyBiometricDevice(BiometricDevice):
    def __init__(self, site_id="munich-office"):
        super().__init__(site_id)
        self.cleared = False

    def extract_biometric_data(self):
        return ExtractedBiometricData(employees=["user-1"], attendance_records=["record-1"])

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

        self.assertEqual(public_methods, {"extract_biometric_data", "pull_records", "clear_records"})

        extract_signature = inspect.signature(BiometricDevice.extract_biometric_data)
        pull_records_signature = inspect.signature(BiometricDevice.pull_records)
        clear_records_signature = inspect.signature(BiometricDevice.clear_records)
        self.assertEqual(
            list(extract_signature.parameters),
            ["self"],
        )
        self.assertEqual(
            extract_signature.return_annotation,
            ExtractedBiometricData,
        )
        self.assertEqual(list(pull_records_signature.parameters), ["self"])
        self.assertEqual(
            pull_records_signature.return_annotation,
            Tuple[Sequence[Any], Sequence[Any]],
        )
        self.assertEqual(list(clear_records_signature.parameters), ["self"])
        self.assertIs(clear_records_signature.return_annotation, None)

    def test_biometric_device_site_id_is_constructor_injected_and_read_only(self):
        device = DummyBiometricDevice("berlin-office")

        self.assertEqual(device.site_id, "berlin-office")
        with self.assertRaises(AttributeError):
            device.site_id = "munich-office"

    def test_biometric_device_rejects_invalid_site_id(self):
        with self.assertRaisesRegex(ValueError, "^site_id must be a non-empty string$"):
            DummyBiometricDevice("")

    def test_biometric_device_can_type_non_zkteco_implementation(self):
        device = DummyBiometricDevice()

        typed_device: BiometricDevice = device
        raw_data = typed_device.extract_biometric_data()
        users, records = typed_device.pull_records()
        typed_device.clear_records()

        self.assertEqual(raw_data, ExtractedBiometricData(employees=["user-1"], attendance_records=["record-1"]))
        self.assertEqual(users, ["user-1"])
        self.assertEqual(records, ["record-1"])
        self.assertTrue(device.cleared)

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
