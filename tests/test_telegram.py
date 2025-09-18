# tests/test_telegram.py
import unittest
from unittest.mock import patch, MagicMock
from core import telegram_bot

class TestTelegram(unittest.TestCase):
    @patch("requests.post")
    def test_send_signal(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = lambda: None
        mock_post.return_value = mock_resp

        telegram_bot.BOT_TOKEN = "dummy"
        telegram_bot.CHAT_ID = "123"
        telegram_bot.send_signal("Hello test")

        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertIn("Hello test", kwargs["json"]["text"])

if __name__ == "__main__":
    unittest.main()

# python -m tests.test_telegram
# python -m unittest discover -s tests -p "test_*.py"