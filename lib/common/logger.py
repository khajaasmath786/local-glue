# logger.py
# This script defines the Logger class for handling logging
# in a reusable way across different modules.

import logging


class Logger:
    """
    Handles logging for various classes.
    """
    def __init__(self, name):
        """
        Initialize the logger with a specific name.

        :param name: Name of the class or module to log.
        """
        self.logger = logging.getLogger(name)
        if not self.logger.hasHandlers():
            self.setup_logger()

    def setup_logger(self):
        """Sets up a logger configuration."""
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def info(self, message):
        """Logs an informational message."""
        self.logger.info(message)

    def debug(self, message):
        """Logs a debug message."""
        self.logger.debug(message)

    def error(self, message):
        """Logs an error message."""
        self.logger.error(message)
    
    def warn(self, message):
        """Logs an error message."""
        self.logger.warn(message)
        
    def warning(self, message):
        """Logs an error message."""
        self.logger.warn(message)
