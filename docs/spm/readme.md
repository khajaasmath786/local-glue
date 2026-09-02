# Project Name

This document outlines the management of dependencies and environment for the project using Poetry. It includes instructions on setting up your environment, adding dependencies, running tests, linting code, and building the project.

## Getting Started

Ensure you have Poetry installed on your system. If Poetry is not installed, follow the instructions on the [official Poetry website](https://python-poetry.org/docs/#installation).

## Installation

### Installing `pylint` with `pip`

If you need to install `pylint` globally on your system, you can use `pip`:

```bash
pip install pylint
```

### Installing Project Dependencies with Poetry
To install the project dependencies with Poetry, navigate to the project's root directory and run:
```bash
poetry install
```
This installs all the dependencies specified in `pyproject.toml`, setting up a virtual environment for the project.

### Adding Flask to Your Project

To add Flask to your project as a dependency, run:

```bash
poetry add flask
```
### Updating Dependencies
To update all dependencies to their latest versions within the constraints specified in pyproject.toml, run:
```bash
poetry update
```
### Version Management
To update the project version, run:
```bash
poetry version minor
```
This will increment the minor version number in pyproject.toml

### Testing and Linting
##### Running Tests
To run tests using pytest, execute:
```bash
poetry run pytest
```
##### Linting Code
To lint a specific directory of your project, use pylint:
```bash
poetry run pylint lib
```
For another directory, for instance pipelib:

```bash
poetry run pylint edh_pipelib
```

### Building the Project

To build your project as a distributable wheel, run:
```bash
poetry build --format wheel
```
This command will generate a .whl file in the dist directory.

### Update the Settings for VSCode

## For Git Bash (Unix-like Shell)
```sh
export PYTHONPATH="/home/glue_user/workspace/edh_data_reader:$PYTHONPATH"
cd /home/glue_user/workspace/edh_data_reader/lib/data_ingestion/spm
cd workspace/edh_data_reader/lib/jobs/dp_sop
cd /home/glue_user/workspace/edh_data_reader/lib/data_ingestion/gensight
python3 ingestion.py --env local
python3 run.py s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib/conf/dp_sop.json


./run_sop.sh s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib/configs/dp_apps/dp_sop.json

Run this from terminal. pwd and cd into utils folder. Also run specific program python3 secrets.py
```

## For PowerShell
```powershell
$env:PYTHONPATH = "C:\asmath_workspace\edh_devops\edh_data_reader;$env:PYTHONPATH"
```

## For Command Prompt (cmd)
```cmd
set PYTHONPATH=C:\asmath_workspace\edh_devops\edh_data_reader;%PYTHONPATH%
```