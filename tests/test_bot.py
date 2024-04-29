import pytest
from bot import Bot

# Basic test setup for bot.py
class TestBot:
    def setup_method(self):
        self.bot = Bot()

    # Test for initial bot startup
    def test_initial_startup(self):
        assert self.bot is not None

    # Test for command parsing
    def test_command_parsing(self):
        command = "!test command"
        parsed_command = self.bot.parse_command(command)
        assert parsed_command == "command"
