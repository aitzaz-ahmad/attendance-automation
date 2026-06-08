import unittest

from attendance_etl.devices.biometric_device_config import BiometricDeviceConfig, ZKTECO_VENDOR, ZKTecoOptions
from attendance_etl.pi4 import runtime


class DeviceStub:
    pass


class StrategyStub:
    pass


class ReviewPeriodStub:
    def to_dict(self):
        return {"month": "May"}


class WorkflowSpy:
    def __init__(self, state, device, strategy, messenger):
        self.state = state
        self.device = device
        self.strategy = strategy
        self.messenger = messenger


class Pi4RuntimeDeviceConfigTests(unittest.TestCase):
    def setUp(self):
        self.original_load_biometric_device_config = runtime.load_biometric_device_config
        self.original_factory = runtime.BiometricDeviceFactory
        self.original_strategy_factory = runtime.TransformationStrategyFactory
        self.original_messenger = runtime.PubSubMessenger
        self.original_load_review_period = runtime.load_review_period
        self.original_load_snapshot_into_state = runtime.load_snapshot_into_state
        self.original_workflow = runtime.Pi4Workflow

    def tearDown(self):
        runtime.load_biometric_device_config = self.original_load_biometric_device_config
        runtime.BiometricDeviceFactory = self.original_factory
        runtime.TransformationStrategyFactory = self.original_strategy_factory
        runtime.PubSubMessenger = self.original_messenger
        runtime.load_review_period = self.original_load_review_period
        runtime.load_snapshot_into_state = self.original_load_snapshot_into_state
        runtime.Pi4Workflow = self.original_workflow

    def device_config(self):
        return BiometricDeviceConfig(
            site_id="munich-office",
            vendor=ZKTECO_VENDOR,
            device_options=ZKTecoOptions(
                ip_address="192.0.2.10",
                comm_port=4370,
            ),
        )

    def test_bootstrap_loads_device_config_before_factory_create(self):
        events = []
        device_config = self.device_config()

        def load_config():
            events.append("load_config")
            return device_config

        class FactorySpy:
            @staticmethod
            def create(config):
                events.append(("create", config))
                return DeviceStub()

        class StrategyFactorySpy:
            @staticmethod
            def create(vendor):
                events.append(("strategy", vendor))
                return StrategyStub()

        class MessengerSpy:
            def __init__(self, site_id):
                self.site_id = site_id
                events.append(("messenger", site_id))

        def load_snapshot(state):
            events.append(("snapshot", state.device_info))

        runtime.load_biometric_device_config = load_config
        runtime.BiometricDeviceFactory = FactorySpy
        runtime.TransformationStrategyFactory = StrategyFactorySpy
        runtime.PubSubMessenger = MessengerSpy
        runtime.load_review_period = lambda: ReviewPeriodStub()
        runtime.load_snapshot_into_state = load_snapshot
        runtime.Pi4Workflow = WorkflowSpy

        workflow = runtime.bootstrap_pi4()

        self.assertEqual(events[0], "load_config")
        self.assertEqual(events[1], ("create", device_config))
        self.assertEqual(events[2], ("strategy", ZKTECO_VENDOR))
        self.assertEqual(events[3], ("messenger", "munich-office"))
        self.assertEqual(workflow.state.device_info, {"site_id": "munich-office"})
        self.assertIsInstance(workflow.device, DeviceStub)
        self.assertIsInstance(workflow.strategy, StrategyStub)

    def test_bootstrap_does_not_construct_or_communicate_when_config_loading_fails(self):
        events = []

        def load_config():
            raise ValueError("invalid biometric device config")

        class FactorySpy:
            @staticmethod
            def create(config):
                events.append(("create", config))
                return DeviceStub()

        class StrategyFactorySpy:
            @staticmethod
            def create(vendor):
                events.append(("strategy", vendor))
                return StrategyStub()

        class MessengerSpy:
            def __init__(self, site_id):
                events.append(("messenger", site_id))

        runtime.load_biometric_device_config = load_config
        runtime.BiometricDeviceFactory = FactorySpy
        runtime.TransformationStrategyFactory = StrategyFactorySpy
        runtime.PubSubMessenger = MessengerSpy

        with self.assertRaisesRegex(ValueError, "^invalid biometric device config$"):
            runtime.bootstrap_pi4()

        self.assertEqual(events, [])


if __name__ == "__main__":
    unittest.main()
