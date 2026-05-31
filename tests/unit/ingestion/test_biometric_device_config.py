import json
import tempfile
import unittest
from dataclasses import fields
from pathlib import Path

from attendance_etl.devices.biometric_device_config import (
    BiometricDeviceConfig,
    BiometricDeviceConfigBuilder,
    VendorOptions,
    ZKTECO_VENDOR,
    ZKTecoOptions,
)


class BiometricDeviceConfigTests(unittest.TestCase):
    def valid_payload(self):
        return {
            "site_id": "munich-office",
            "vendor": ZKTECO_VENDOR,
            "device_options": {
                "ip_address": "192.0.2.10",
                "comm_port": 4370,
            },
        }

    def test_vendor_options_base_has_no_site_metadata(self):
        options = VendorOptions()

        self.assertEqual(fields(VendorOptions), ())
        self.assertFalse(hasattr(options, "identifier"))
        self.assertFalse(hasattr(options, "site_id"))

    def test_zkteco_options_extend_vendor_options(self):
        options = ZKTecoOptions(
            ip_address="192.0.2.10",
            comm_port=4370,
        )

        self.assertIsInstance(options, VendorOptions)
        self.assertEqual(
            {field.name for field in fields(ZKTecoOptions)},
            {"ip_address", "comm_port", "timeout", "force_udp", "ommit_ping"},
        )
        self.assertFalse(hasattr(options, "identifier"))
        self.assertFalse(hasattr(options, "site_id"))
        self.assertEqual(options.ip_address, "192.0.2.10")
        self.assertEqual(options.comm_port, 4370)
        self.assertIsNone(options.timeout)
        self.assertIsNone(options.force_udp)
        self.assertIsNone(options.ommit_ping)

    def test_biometric_device_config_contains_site_id_vendor_and_options(self):
        options = VendorOptions()
        device_config = BiometricDeviceConfig(
            site_id="munich-office",
            vendor=ZKTECO_VENDOR,
            device_options=options,
        )

        self.assertEqual(device_config.site_id, "munich-office")
        self.assertEqual(device_config.vendor, "zkteco")
        self.assertIs(device_config.device_options, options)

    def test_builder_loads_valid_zkteco_json_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "biometric_device_config.json"
            config_path.write_text(json.dumps(self.valid_payload()))

            device_config = BiometricDeviceConfigBuilder.from_json_file(config_path)

        self.assertEqual(device_config.site_id, "munich-office")
        self.assertEqual(device_config.vendor, ZKTECO_VENDOR)
        self.assertIsInstance(device_config.device_options, ZKTecoOptions)
        options = device_config.device_options
        self.assertFalse(hasattr(options, "identifier"))
        self.assertFalse(hasattr(options, "site_id"))
        self.assertEqual(options.ip_address, "192.0.2.10")
        self.assertEqual(options.comm_port, 4370)

    def test_builder_loads_valid_zkteco_options(self):
        payload = self.valid_payload()
        payload["device_options"].update(
            {
                "timeout": 15,
                "force_udp": True,
                "ommit_ping": True,
            }
        )

        device_config = BiometricDeviceConfigBuilder.from_dict(payload)

        self.assertEqual(device_config.site_id, "munich-office")
        self.assertEqual(device_config.vendor, ZKTECO_VENDOR)
        self.assertEqual(device_config.device_options.timeout, 15)
        self.assertIs(device_config.device_options.force_udp, True)
        self.assertIs(device_config.device_options.ommit_ping, True)

    def test_missing_config_file_fails_fast(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "missing.json"

            with self.assertRaisesRegex(FileNotFoundError, "^Biometric device config file not found: "):
                BiometricDeviceConfigBuilder.from_json_file(missing_path)

    def test_unreadable_config_file_fails_fast(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(OSError, "^Biometric device config file is not readable: "):
                BiometricDeviceConfigBuilder.from_json_file(temp_dir)

    def test_invalid_json_fails_fast(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "biometric_device_config.json"
            config_path.write_text("{")

            with self.assertRaisesRegex(ValueError, "^Invalid biometric device config JSON: "):
                BiometricDeviceConfigBuilder.from_json_file(config_path)

    def test_missing_required_root_fields_fail_fast(self):
        with self.assertRaisesRegex(ValueError, "^Missing required biometric device config field: site_id$"):
            BiometricDeviceConfigBuilder.from_dict(
                {
                    "vendor": ZKTECO_VENDOR,
                    "device_options": {},
                }
            )

        with self.assertRaisesRegex(ValueError, "^Missing required biometric device config field: vendor$"):
            BiometricDeviceConfigBuilder.from_dict(
                {
                    "site_id": "munich-office",
                    "device_options": {},
                }
            )

        with self.assertRaisesRegex(ValueError, "^Missing required biometric device config field: device_options$"):
            BiometricDeviceConfigBuilder.from_dict(
                {
                    "site_id": "munich-office",
                    "vendor": ZKTECO_VENDOR,
                }
            )

    def test_invalid_site_id_fails_fast(self):
        payload = self.valid_payload()
        payload["site_id"] = ""

        with self.assertRaisesRegex(ValueError, "^biometric device config field site_id must be a non-empty string$"):
            BiometricDeviceConfigBuilder.from_dict(payload)

        payload = self.valid_payload()
        payload["site_id"] = 42

        with self.assertRaisesRegex(ValueError, "^biometric device config field site_id must be a non-empty string$"):
            BiometricDeviceConfigBuilder.from_dict(payload)

    def test_identifier_is_not_supported_as_site_id_alias(self):
        payload = self.valid_payload()
        del payload["site_id"]
        payload["identifier"] = "att-dev-dk-01"

        with self.assertRaisesRegex(ValueError, "^Missing required biometric device config field: site_id$"):
            BiometricDeviceConfigBuilder.from_dict(payload)

    def test_unsupported_vendor_fails_clearly(self):
        payload = self.valid_payload()
        payload["vendor"] = "hikvision"

        with self.assertRaisesRegex(ValueError, "^Unsupported device vendor: hikvision$"):
            BiometricDeviceConfigBuilder.from_dict(payload)

    def test_missing_required_zkteco_options_fail_fast(self):
        payload = self.valid_payload()
        del payload["device_options"]["ip_address"]

        with self.assertRaisesRegex(ValueError, "^Missing required ZKTeco option: ip_address$"):
            BiometricDeviceConfigBuilder.from_dict(payload)

    def test_invalid_zkteco_options_fail_fast(self):
        payload = self.valid_payload()
        payload["device_options"]["comm_port"] = "4370"

        with self.assertRaisesRegex(ValueError, "^ZKTeco option comm_port must be an integer$"):
            BiometricDeviceConfigBuilder.from_dict(payload)

    def test_unknown_zkteco_options_fail_fast(self):
        payload = self.valid_payload()
        payload["device_options"]["port"] = 4370

        with self.assertRaisesRegex(ValueError, "^Unsupported ZKTeco option: port$"):
            BiometricDeviceConfigBuilder.from_dict(payload)

    def test_site_id_is_not_a_zkteco_option(self):
        payload = self.valid_payload()
        payload["device_options"]["site_id"] = "munich-office"

        with self.assertRaisesRegex(ValueError, "^Unsupported ZKTeco option: site_id$"):
            BiometricDeviceConfigBuilder.from_dict(payload)


if __name__ == "__main__":
    unittest.main()
