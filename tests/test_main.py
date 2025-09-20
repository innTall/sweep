# tests/test_main.py
import unittest
from modules.breakouts import format_breakout_message

class TestMain(unittest.TestCase):
    def test_format_message(self):
        breakout = {
            "symbol": "BTCUSDT",
            "interval": "1h",
            "type": "HConfirm",
            "fractal_value": 27345.67,
            "fractal_time": 1691232000000,
            "candle_high": 27350.0,
            "candle_low": 27340.0,
            "candle_close": 27345.67,
            "candle_time": 1691232000000,
            "distance": 1,
        }
        msg = format_breakout_message(breakout, tz=None)
        self.assertIn("BTCUSDT", msg)
        self.assertIn("27345.67", msg)
        self.assertIn("05Aug", msg)  # check date is formatted

if __name__ == "__main__":
    unittest.main()
  
# python -m tests.test_main
# python -m unittest discover -s tests -p "test_*.py"