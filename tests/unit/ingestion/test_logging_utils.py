import logging
import unittest

from attendance_etl.logging_utils import configure_logging, get_logger, parse_log_level


class LoggingUtilsTests(unittest.TestCase):
    def setUp(self):
        self.root_logger = logging.getLogger()
        self.original_handlers = list(self.root_logger.handlers)
        self.original_level = self.root_logger.level
        self.root_logger.handlers[:] = []

    def tearDown(self):
        for handler in self.root_logger.handlers:
            if handler not in self.original_handlers:
                handler.close()
        self.root_logger.handlers[:] = self.original_handlers
        self.root_logger.setLevel(self.original_level)

    def test_parse_log_level_accepts_valid_values_case_insensitively(self):
        self.assertEqual(parse_log_level("DEBUG"), logging.DEBUG)
        self.assertEqual(parse_log_level("info"), logging.INFO)
        self.assertEqual(parse_log_level("Warning"), logging.WARNING)
        self.assertEqual(parse_log_level("error"), logging.ERROR)
        self.assertEqual(parse_log_level("critical"), logging.CRITICAL)

    def test_parse_log_level_rejects_invalid_values(self):
        with self.assertRaisesRegex(ValueError, "unsupported log level 'TRACE'"):
            parse_log_level("TRACE")

    def test_configure_logging_is_idempotent(self):
        configure_logging(logging.INFO)
        first_handlers = list(self.root_logger.handlers)

        configure_logging(logging.DEBUG)

        self.assertEqual(self.root_logger.handlers, first_handlers)
        self.assertEqual(len(self.root_logger.handlers), 1)
        self.assertEqual(self.root_logger.level, logging.DEBUG)
        self.assertEqual(self.root_logger.handlers[0].level, logging.DEBUG)

    def test_get_logger_returns_named_logger(self):
        self.assertEqual(get_logger("Pi4Runtime").name, "Pi4Runtime")


if __name__ == "__main__":
    unittest.main()
