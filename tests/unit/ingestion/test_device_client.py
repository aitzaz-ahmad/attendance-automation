import inspect
import unittest
from typing import Any, Sequence, Tuple

from attendance_etl.device.base import DeviceClient


class ZKImportBlocker:
    def __init__(self):
        self.attempted_imports = []

    def find_spec(self, fullname, path=None, target=None):
        if fullname == "zk" or fullname.startswith("zk."):
            self.attempted_imports.append(fullname)
            raise AssertionError("DeviceClient base abstraction imported vendor SDK dependency")
        return None


class GenericDevice:
    def __init__(self):
        self.cleared = False

    def pull_records(self):
        return ["user-1"], ["record-1"]

    def clear_records(self):
        self.cleared = True


class DeviceClientTests(unittest.TestCase):
    def test_device_client_base_module_has_no_vendor_sdk_dependency(self):
        import attendance_etl.device.base as base_module

        module_globals = vars(base_module)

        self.assertNotIn("zk", module_globals)

    def test_device_client_exposes_minimal_audited_contract(self):
        public_methods = {
            name for name, value in vars(DeviceClient).items() if callable(value) and not name.startswith("_")
        }

        self.assertEqual(public_methods, {"pull_records", "clear_records"})

        pull_records_signature = inspect.signature(DeviceClient.pull_records)
        clear_records_signature = inspect.signature(DeviceClient.clear_records)
        self.assertEqual(list(pull_records_signature.parameters), ["self"])
        self.assertEqual(
            pull_records_signature.return_annotation,
            Tuple[Sequence[Any], Sequence[Any]],
        )
        self.assertEqual(list(clear_records_signature.parameters), ["self"])
        self.assertIs(clear_records_signature.return_annotation, None)

    def test_device_client_can_type_non_zkteco_implementation(self):
        device = GenericDevice()

        typed_device: DeviceClient = device
        users, records = typed_device.pull_records()
        typed_device.clear_records()

        self.assertEqual(users, ["user-1"])
        self.assertEqual(records, ["record-1"])
        self.assertTrue(device.cleared)


if __name__ == "__main__":
    unittest.main()
