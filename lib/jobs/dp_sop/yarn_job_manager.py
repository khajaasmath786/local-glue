import subprocess
import sys
from pyspark.sql import SparkSession
from lib.common.logger import Logger  # Adjust the import

logger = Logger("KillYarnJobs")

# Usage:
# List all running YARN jobs:
# python yarn_job_manager.py --list
#
# Grep YARN jobs by name (e.g., `PRS_CURT_RPT_EMP_PUNCH_IN_GLOBAL`):
# python yarn_job_manager.py --grep "PRS CURT RPT EMP PUNCH IN GLOBAL"
#
# Kill a specific YARN job by its application ID 
# (e.g., `application_1668665628849_2171509`):
# python yarn_job_manager.py --kill "application_1668665628849_2171509"
#
# Kill multiple YARN jobs by their application IDs:
# python yarn_job_manager.py --kill "application_1668665628849_2171509,application_1668665628849_2171510,application_1668665628849_2171511"
#
# Kill multiple YARN jobs by job name (e.g., `controltower`):
# python yarn_job_manager.py --kill "control tower"

class YarnJobManager:
    """
    Manages YARN jobs, including listing running jobs and 
    killing specific jobs.

    This class handles YARN command execution for listing and 
    killing jobs based on provided parameters. It leverages 
    subprocess to interact with the YARN CLI and extensively 
    uses logging to track actions and statuses.
    """

    def __init__(self, spark):
        """
        Initialize the YarnJobManager class with Spark session.

        Args:
            spark (SparkSession): The Spark session.
        """
        self.spark = spark
        self.yarn_command_base = ("yarn application -list "
                                  "-appStates RUNNING")

    def list_running_jobs(self):
        """
        Lists all currently running YARN jobs by parsing the 
        command output.

        Returns:
            list: List of dictionaries with job details.
        """
        logger.info("Listing all running jobs:")
        output = self._run_command(self.yarn_command_base)
        if output is None:
            return []

        jobs = []

        # Format the output as a table
        table_header = f"{'Application Id':<30}  {'State':<15} {'Application Name':<30}"
        separator = '*' * len(table_header)
        logger.info(separator)
        logger.info(table_header)
        logger.info(separator)

        lines = output.split("\n")
        start_index = 0
        valid_states = {"RUNNING", "ACCEPTED", "SUBMITTED", "KILLED", 
                        "FAILED", "FINISHED"}

        for i, line in enumerate(lines):
            if line.strip().startswith("Application-Id"):
                start_index = i + 1
                break

        for line in lines[start_index:]:
            if not line.strip() or line.strip().startswith("Total"):
                continue
            parts = line.split()
            if len(parts) >= 9 and parts[0].startswith("application_"):
                application_id = parts[0]
                state = parts[-4]  # 4th from the end for State
                if state in valid_states:
                    # Assuming "hadoop" is 6th from the end
                    hadoop_index = len(parts) - 6
                    # Extract application name correctly
                    application_name_parts = parts[1:hadoop_index]
                    application_name = " ".join(
                        application_name_parts).strip()
                    if application_name.endswith("SPARK"):
                        application_name = application_name[:-5].strip()
                    jobs.append({
                        "application_id": application_id,
                        "state": state,
                        "application_name": application_name
                    })
                    logger.info(
                        f"{application_id:<30} {state:<15} "
                        f"{application_name:<30}")

        logger.info(separator)
        return jobs

    def list_jobs_by_name(self, job_name):
        """
        Lists YARN jobs by name.

        Args:
            job_name (str): The name of the job to list.
        """
        logger.info(f"Listing jobs with name containing '{job_name}':")

        jobs = self.list_running_jobs()

        # Format the output as a table
        table_header = f"{'Application Id':<30} {'State':<15} {'Application Name':<30}"
        separator = '*' * len(table_header)
        logger.info(separator)
        logger.info(table_header)
        logger.info(separator)

        for job in jobs:
            if job_name in job["application_name"]:
                logger.info(
                    f"{job['application_id']:<30} {job['state']:<15} "
                    f"{job['application_name']:<30}")

        logger.info(separator)

    def kill_job(self, job_id):
        """
        Kills a specified YARN job by its ID.

        Args:
            job_id (str): The ID of the job to kill.
        """
        yarn_command = f"yarn application -kill {job_id}"
        output = self._run_command(yarn_command)
        if output is not None:
            logger.info(f"Job {job_id} killed successfully.")

    def kill_jobs(self, identifiers):
        """
        Kills multiple YARN jobs specified by their IDs or names.

        Args:
            identifiers (list): List of IDs or names of the jobs 
            to kill.
        """
        for identifier in identifiers:
            if identifier.startswith("application_"):
                for app_id in identifier.split(','):
                    self.kill_job(app_id.strip())
            else:
                for name in identifier.split(','):
                    self.kill_jobs_by_name(name.strip())

    def kill_jobs_by_name(self, job_name):
        """
        Kills all YARN jobs that match the given name.

        Args:
            job_name (str): The name of the jobs to kill.
        """
        jobs = self.list_running_jobs()
        for job in jobs:
            if job_name in job["application_name"]:
                self.kill_job(job["application_id"])

    def _run_command(self, yarn_command):
        """
        Executes a given YARN command as the hadoop user and 
        returns the output.

        Args:
            yarn_command (str): The YARN command to run.

        Returns:
            str: The output of the command, or None if the 
            command fails.
        """
        command = f"{yarn_command}"
        try:
            return subprocess.check_output(
                command, shell=True, universal_newlines=True, 
                stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e.output}")
            return None

    def execute(self, action=None, identifiers=None):
        """
        Executes listing or killing jobs based on the action 
        and identifiers parameters.

        Args:
            action (str, optional): The action to perform 
            ('list', 'kill', or 'grep').
            identifiers (list, optional): The IDs or names of 
            the job(s) to act upon.
        """
        if action == 'list':
            self.list_running_jobs()
        elif action == 'kill':
            if identifiers:
                self.kill_jobs(identifiers)
        elif action == 'grep':
            if identifiers and len(identifiers) == 1:
                self.list_jobs_by_name(identifiers[0])
        else:
            logger.error("Invalid action. Please specify 'list', "
                         "'kill', or 'grep'.")


def parse_args(args):
    """
    Parses command-line arguments to detect the action and 
    identifiers.

    Args:
        args (list): List of command-line arguments.

    Returns:
        tuple: action and identifiers if provided, else (None, None).
    """
    valid_actions = {'list', 'kill', 'grep'}
    if args and len(args) >= 2:
        act = args[0].replace('--', '')
        if act not in valid_actions:
            logger.error(f"Invalid action: {act}. Valid actions are "
                         "'list', 'kill', 'grep'.")
            sys.exit(1)
        ident = ' '.join(args[1:])  # Join the rest of the args as identifier
        return act, ident
    elif args and len(args) == 1:
        act = args[0].replace('--', '')
        if act not in valid_actions:
            logger.error(f"Invalid action: {act}. Valid actions are "
                         "'list', 'kill', 'grep'.")
            sys.exit(1)
        return act, None
    logger.error("No action provided. Valid actions are 'list', "
                 "'kill', 'grep'.")
    sys.exit(1)


if __name__ == "__main__":
    logger.info("Starting YARN Job Manager")
    action, identifier_str = parse_args(sys.argv[1:])
    identifiers = identifier_str.split(',') if identifier_str else []
    spark = SparkSession.builder.appName("YarnJobManager").getOrCreate()
    manager = YarnJobManager(spark)
    manager.execute(action, identifiers)
    spark.stop()