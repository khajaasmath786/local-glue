"""
ConfigHelper module provides utility functions for loading and retrieving configurations from a YAML file.

The YAML file should contain configuration values in the following format:
{
    "app_name": "my-app",
    "version": 1.0,
    "db_config": {
        "url": "jdbc:mysql://localhost:3306/mydb",
        "user": "myuser",
        "password": "mypassword"
    },
    "logging": {
        "log_level": "info",
        "log_file": "/var/log/my-app.log"
    }
}

Usage:
    - To load a config file, use `config = ConfigHelper.load_config_file(config_path)`
    - To get a config value, use `value = ConfigHelper.get_config(config, section, key, default=None)`

"""
import yaml
import logging


class ConfigLoader:
    @staticmethod
    def read_config_yaml(config_file):
        """
        This method loads the configuration from a YAML file.

        :param config_file: The path to the YAML configuration file.
        :type config_file: str
        :return: The configuration loaded from the YAML file.
        :rtype: dict
        """
        config = {}
        try:
            # Load the configuration from the YAML file
            with open(config_file, "r") as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
        except FileNotFoundError as e:
            logging.error(f"Could not find the YAML configuration file at {config_file}. {e}")
        except yaml.YAMLError as e:
            logging.error(f"Could not load the YAML configuration file {config_file}. {e}")
        return config

    @staticmethod
    def get_config(config, section, *keys, default=None):
        """
        This method retrieves the value of a specific set of keys in a specified section of the config object.

        :param config: The configuration object.
        :type config: dict
        :param section: The section of the config object to retrieve the key from.
        :type section: str
        :param keys: The keys whose value is to be retrieved.
        :type keys: str
        :param default: The default value to return if the specified key is not present in the config object.
        :type default: str
        :return: The value of the specified keys in the specified section.
        :rtype: str
        """
        key = None
        try:
            # Retrieve the value of the key in the specified section
            value = config[section]
            for key in keys:
                value = value[key]
        except KeyError as e:
            # Return the default value if the section or key does not exist in the config object
            if default is not None:
                return default
            elif key is not None:
                raise ValueError(f"Invalid key '{key}' in section '{section}' of configuration file. {e}")
            else:
                raise ValueError(f"Invalid section '{section}' in configuration file. {e}")
        return value
