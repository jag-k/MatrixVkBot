import pytest
from config import load_config

# Test setup for config.py
class TestConfig:
    def setup_method(self):
        self.default_config = load_config("config.py.example")

    # Test to validate default configuration loading
    def test_default_config_loading(self):
        assert self.default_config is not None
        assert self.default_config['server'] == "https://matrix.org"
        assert self.default_config['username'] == "vk2matrix"
        assert self.default_config['password'] == "XXXXXX"
        assert self.default_config['device_id'] == "vkbot"

    # Test to validate custom configuration loading
    def test_custom_config_loading(self):
        custom_config = load_config("custom_config.py")
        assert custom_config is not None
        assert custom_config['server'] == "https://custom.matrix.org"
        assert custom_config['username'] == "customuser"
        assert custom_config['password'] == "custompassword"
        assert custom_config['device_id'] == "customdevice"
