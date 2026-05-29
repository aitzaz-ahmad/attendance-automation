import logging
import unittest

from pi4 import pi4_client


class Pi4ClientCliTests(unittest.TestCase):
    def test_argument_parsing_defaults_verbosity_to_info(self):
        args = pi4_client.parse_args([])

        self.assertEqual(args.verbosity, logging.INFO)

    def test_argument_parsing_accepts_long_verbosity(self):
        args = pi4_client.parse_args(["--verbosity", "DEBUG"])

        self.assertEqual(args.verbosity, logging.DEBUG)

    def test_argument_parsing_accepts_short_verbosity(self):
        args = pi4_client.parse_args(["-V", "WARNING"])

        self.assertEqual(args.verbosity, logging.WARNING)

    def test_argument_parsing_rejects_invalid_verbosity(self):
        with self.assertRaises(SystemExit):
            pi4_client.parse_args(["--verbosity", "TRACE"])


if __name__ == "__main__":
    unittest.main()
