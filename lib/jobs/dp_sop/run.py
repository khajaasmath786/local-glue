# main.py
# This script serves as the entry point for the application.
# It initializes the necessary components and prints SQL queries.

import os
import sys

from pyspark import SparkFiles

from lib.common.load_config import ConfigHelper
from lib.common.logger import Logger
from lib.common.spark_session_manager import SparkSessionManager
from lib.tasks.sop.sop_executor import SOPExecutor


def parse_args(args):
    """
    Parse command-line arguments.

    :param args: List of command-line arguments.
    :return: Dictionary of parsed arguments.
    """
    param_dict = {}
    for i in range(0, len(args), 2):
        key = args[i].lstrip('--')
        param_dict[key] = args[i + 1]
    return param_dict


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: script <header_queries_path>")
        sys.exit(1)

    header_queries_path = sys.argv[1]

    # Initialize Spark session
    spark_manager = SparkSessionManager("SQLQueryPrinter")
    spark_manager.add_file(header_queries_path)

    # Set up logger
    logger = Logger("SQLQueryPrinter")

    # Use the base name of the file to ensure it's accessible on all nodes
    header_queries_filename = os.path.basename(SparkFiles.get(header_queries_path))
    logger.info(header_queries_filename)
    # sys.exit(0)
    # Initialize ConfigReader
    # config_reader = ConfigHelper(header_queries_filename)
    config_reader = ConfigHelper(header_queries_path)

    # Initialize and run SQLQueryPrinter
    query_printer = SOPExecutor(spark_manager.get_spark(), logger,
                                config_reader)
    query_printer.print_and_execute_queries()

    # Stop the Spark session
    spark_manager.stop()