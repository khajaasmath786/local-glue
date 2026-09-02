import argparse
import logging
import os

from lib.common.yaml_loader import ConfigLoader
from lib.tasks.task import TaskExecutor


def main():

    logging.basicConfig(level=logging.INFO)
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--env', type=str, default='dev', choices=['dev', 'qa', 'prod'],
                            help='Environment name (default: dev)')
    arg_parser.add_argument('--app', type=str, default='ap', choices=['ap', 'eu', 'gme', 'la', 'uc'],
                            help='Application name (default: ap)')
    args = arg_parser.parse_args()
    env = args.env


    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "", ".."))
    config_path = os.path.join(project_root, "configs", f"config-{env}.yaml")
    job_config = ConfigLoader.read_config_yaml(config_path)
    task = TaskExecutor(job_config)
    task.generate_sql_schema_files()

if __name__ == "__main__":
    main()
