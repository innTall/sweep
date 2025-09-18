import unittest
from datetime import datetime, timezone
import main

class TestMain(unittest.TestCase):
    def test_format_message(self):
        candle = {"timestamp": 1691232000000, "close": 27345.67}
        msg = main.format_message("BTCUSDT", "1h", candle)
        self.assertIn("BTCUSDT", msg)
        self.assertIn("27345.67", msg)
        self.assertIn("2023", msg)  # check date is formatted

if __name__ == "__main__":
    unittest.main()
  
# python -m tests.test_main
# python -m unittest discover -s tests -p "test_*.py"