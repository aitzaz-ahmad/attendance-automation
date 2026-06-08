import unittest
from pathlib import Path

from attendance_etl.devices.biometric_device_config import ZKTECO_VENDOR
from attendance_etl.transform import TransformationStrategy, TransformationStrategyFactory, ZKTecoTransformationStrategy

REPO_ROOT = Path(__file__).resolve().parents[3]
FACTORY_PATH = REPO_ROOT / "src/attendance_etl/transform/transformation_strategy_factory.py"


class TransformationStrategyFactoryTests(unittest.TestCase):
    def test_create_returns_zkteco_strategy_for_zkteco_vendor(self):
        strategy = TransformationStrategyFactory.create(ZKTECO_VENDOR)

        self.assertIsInstance(strategy, TransformationStrategy)
        self.assertIsInstance(strategy, ZKTecoTransformationStrategy)

    def test_unsupported_vendor_fails_clearly(self):
        with self.assertRaisesRegex(ValueError, "^Unsupported transformation strategy vendor: hikvision$"):
            TransformationStrategyFactory.create("hikvision")

    def test_factory_stays_minimal_without_broader_composition_abstractions(self):
        source = FACTORY_PATH.read_text().lower()
        public_methods = {
            name
            for name, value in vars(TransformationStrategyFactory).items()
            if callable(value) and not name.startswith("_")
        }

        self.assertEqual(public_methods, {"create"})
        self.assertNotIn("biometricdevicefactory", source)
        self.assertNotIn("plugin", source)
        self.assertNotIn("registry", source)
        self.assertNotIn("importlib", source)
        self.assertNotIn("container", source)
        self.assertNotIn("inject", source)


if __name__ == "__main__":
    unittest.main()
