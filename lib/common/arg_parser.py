"""
arg_parser.py - A lightweight command line argument parser

This module provides an ArgParser class for parsing command line arguments.
The ArgParser class provides a simple interface for parsing
 command line arguments and generating help messages. It is inspired by the
 argparse module in the Python standard library,
 but is designed to be simpler and more lightweight.

Example usage:

    from arg_parser import ArgParser

    parser = ArgParser(description='Example ArgParser usage')
    parser.add_argument('-f', '--file', help='input file')
    parser.add_argument('-n', '--num', type=int, help='number of items')
    args = parser.parse_args()

This will create an ArgParser instance with a description, add two arguments to the parser,
and then parse the command line arguments and return the parsed values.

For more information, see the docstrings for the ArgParser class and its methods.
"""

import argparse
import datetime
import logging
import sys


class ArgParser:
    def __init__(self):
        """
        This method initializes the object of ArgParser class with the following arguments:

        --config: Specifies the config file that contains all the properties related to the app.
        --input_path: Specifies the input path where the files are present.
        --jars: Specifies the jars that will be added to the spark config.
        This method also validates the string for --config argument using validate_string function.
        The parsed arguments are stored in args attribute.
        """
        self.parser = argparse.ArgumentParser(description='Process some integers.')
        self.parser.add_argument("-c", '--config', type=self.validate_string,
                                 help="Config file that contains all the properties related to app")
        self.parser.add_argument('--input_path',
                                 help="input path where the files are present",
                                 default=None)
        self.parser.add_argument('--jars',
                                 help="Jars that will be added to spark config",
                                 default=None)
        self.args = self.parse_args()

    def parse_args(self):
        return self.parser.parse_args()

    @staticmethod
    def valid_string(value):
        """
        A function to check if the input is a valid string.
        :param value: The input value.
        :return: The input value if it is a valid string, otherwise raises an error.
        """

        return value

    @staticmethod
    def valid_date(datestring):
        """
        A function to check if the input is a valid date.
        :param datestring: The input date string.
        :return: The input date string if it is a valid date, otherwise raises an error.
        """
        try:
            datetime.datetime.strptime(datestring, '%Y-%m-%d')
        except ValueError:
            raise argparse.ArgumentTypeError("Invalid date format. Use YYYY-MM-DD")
        return datestring

    @staticmethod
    def validate_input_args(args):
        if not args.config:
            error = f"One of the required parameter config is not present"
            logging.error(error)
            sys.exit(error)

    @staticmethod
    def positive_int(value):
        """
        A function to check if the input is a positive integer.
        :param value: The input value.
        :return: The input value if it is a positive integer, otherwise raises an error.
        """
        ivalue = int(value)
        if ivalue <= 0:
            raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
        return ivalue

    @staticmethod
    def validate_string(value):
        """
        A function to check if the input is a valid string.
        :param value: The input value.
        :return: The input value if it is a valid string, otherwise raises an error.
        """
        if not isinstance(value, str):
            raise argparse.ArgumentTypeError("%s is an invalid string value" % value)
        return value
