# tests/test_rest_confirmed.py
import unittest
from unittest.mock import patch, MagicMock
from utils import bingx_api

class TestRestConfirmed(unittest.TestCase):
    @patch("requests.get")
    def test_get_last_confirmed_candle(self, mock_get):
        fake_data = [
            {"time": 1000, "open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "10"},
            {"time": 2000, "open": "1.6", "high": "2.1", "low": "1.2", "close": "1.9", "volume": "20"},
            {"time": 3000, "open": "2.0", "high": "2.5", "low": "1.7", "close": "2.3", "volume": "30"},
        ]
        mock_resp = MagicMock()
        mock_resp.json.return_value = fake_data
        mock_resp.raise_for_status = lambda: None
        mock_get.return_value = mock_resp
        
        interval_map = {"1h": 3600}
        candle = bingx_api.get_last_confirmed_candle("BTCUSDT", "1h", interval_map)
        self.assertEqual(candle["close"], 1.9)
        self.assertEqual(candle["timestamp"], 2000 + interval_map["1h"]*1000)

if __name__ == "__main__":
    unittest.main()

# python -m tests.test_rest_confirmed
# python -m unittest discover -s tests -p "test_rest_confirmed.py"