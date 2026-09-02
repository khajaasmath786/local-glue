# YARN Job Manager README

## Overview

This README provides a comprehensive guide on using the YARN Job Manager. The YARN Job Manager is a utility script designed to list, grep, and kill YARN jobs using command-line arguments. It simplifies managing YARN applications by providing clear and concise commands to handle various job-related tasks.

## Requirements

- Python 3.x
- PySpark
- Subprocess module
- Logger module (custom or standard)

## Usage

The YARN Job Manager can perform the following actions:
1. List all running YARN jobs.
2. Grep YARN jobs by name.
3. Kill specific YARN jobs by their application IDs or names.

The script accepts command-line arguments to specify the desired action and the job identifiers.

### General Command Structure

```bash
python yarn_job_manager.py --action <action> --identifier "<identifiers>"
```

### Actions

#### List Running Jobs

Lists all currently running YARN jobs.

**Command:**

```bash
python yarn_job_manager.py --action list
```
#### Kill Specific YARN Jobs by Application ID

Kills a specific YARN job by its application ID.

**Command:**

```bash
python yarn_job_manager.py --action kill --identifier "<application_id>"
```

**Example:**

```bash
python yarn_job_manager.py --action kill --identifier "application_1668665628849_2171509"
```

#### Kill Multiple YARN Jobs by Application IDs

Kills multiple YARN jobs specified by their application IDs.

**Command:**

```bash
python yarn_job_manager.py --action kill --identifier "<application_id1>,<application_id2>,<application_id3>"
```

**Example:**

```bash
python yarn_job_manager.py --action kill --identifier "application_1668665628849_2171509,application_1668665628849_2171510,application_1668665628849_2171511"
```

#### Kill YARN Jobs by Name

Kills all YARN jobs that match the given name.

**Command:**

```bash
python yarn_job_manager.py --action kill --identifier "<job_name>"
```

**Example:**

```bash
python yarn_job_manager.py --action kill --identifier "controltower"
```

## Identifiers

Identifiers can be either application IDs or job names. Application IDs should follow the format `application_<timestamp>_<id>`. Job names can be any part of the application name used in YARN. Multiple identifiers should be comma-separated and enclosed in quotes.

## Options

- `--action <action>`: Specifies the action to perform. Valid actions are `list`, `grep`, and `kill`.
- `--identifier "<identifiers>"`: Specifies the job identifiers. Can be application IDs or job names. Multiple identifiers should be comma-separated and enclosed in quotes.

## Example Usage

**List all running jobs:**

```bash
python yarn_job_manager.py --action list
```

**Grep jobs by name:**

```bash
python yarn_job_manager.py --action grep --identifier "PRS_CURT_RPT_EMP_PUNCH_IN_GLOBAL"
```

**Kill a specific job by application ID:**

```bash
python yarn_job_manager.py --action kill --identifier "application_1668665628849_2171509"
```

**Kill multiple jobs by application IDs:**

```bash
python yarn_job_manager.py --action kill --identifier "application_1668665628849_2171509,application_1668665628849_2171510,application_1668665628849_2171511"
```

**Kill jobs by name:**

```bash
python yarn_job_manager.py --action kill --identifier "controltower"
```
## Running from Control M

To run the YARN Job Manager from Control M, pass the parameters as follows:

ControlM Job Name: Kill_app <Exact name will be provided by control M team>
### General Command Structure

```bash
--action <action> --identifier "<identifiers>"
```

### Examples

**List all running jobs:**

```bash
--action list
```

**Grep jobs by name:**

```bash
--action grep --identifier "PRS_CURT_RPT_EMP_PUNCH_IN_GLOBAL"
```

**Kill a specific job by application ID:**

```bash
--action kill --identifier "application_1668665628849_2171509"
```

**Kill multiple jobs by application IDs:**

```bash
--action kill --identifier "application_1668665628849_2171509,application_1668665628849_2171510,application_1668665628849_2171511"
```

**Kill jobs by name:**

```bash
--action kill --identifier "controltower"
```

## Conclusion

The YARN Job Manager is a powerful tool for managing YARN jobs. By following this guide, you can efficiently list and kill YARN jobs using simple command-line arguments. This utility helps in maintaining control over YARN applications and ensures smooth operations.
