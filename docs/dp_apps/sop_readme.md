# EDH Framework JSON Document Construction Guide

## Overview

This README provides a detailed guide on constructing the SOP JSON document used in the EDH Framework. It explains the significance of the steps, the format requirements, different actions, and the use of views. Following this guide ensures that your workflow is correctly defined and can be executed seamlessly by the framework.

## JSON Document Structure

The JSON document consists of a series of steps that define the data processing workflow. Each step has a specific action and parameters necessary for executing the action. The structure is designed to ensure clarity, consistency, and ease of use.

### Example Structure

```json
{
  "sql": {
    "step1_create_orphan_records_table": {
      "action": "execute_query",
      "query": "CREATE TABLE ...",
      "engine": "spark"
    },
    "step2_copy_to_backup": {
      "action": "s3_copy",
      "path": "aws s3 cp ..."
    },
    "step3_read_orphan_records": {
      "action": "read",
      "path": "s3://...",
      "view": "temp_view_p_jde_addr"
    },
    "step4_select_data": {
      "action": "execute_query",
      "query": "SELECT * FROM temp_view_p_jde_addr",
      "view": "temp_view_p_jde_addr"
    },
    "step5_write_to_target": {
      "action": "write",
      "path": "s3://...",
      "partition_fields": "CORP_DATA_OWNR_CD",
      "view": "temp_view_p_jde_addr"
    },
    "step6_read_target_data": {
      "action": "read",
      "path": "s3://..."
    },
    "step7_compare_schemas": {
      "action": "compare_schema",
      "path1": "s3://...",
      "path2": "s3://..."
    }
  }
}
```

## Steps Explanation

### Step 1: Create Orphan Records Table
**Action:** `execute_query`  
**Description:** This step creates a new table to store orphan records, which are records not matched in a specified join operation. It uses Spark SQL to execute the query.
- **Parameters:**
  - `query`: The SQL query to execute.
  - `engine`: The query execution engine, typically `spark`.

### Step 2: Copy Data to Backup
**Action:** `s3_copy`  
**Description:** Copies existing data to a backup location in S3 for preservation and recovery purposes.
- **Parameters:**
  - `path`: The S3 copy command detailing the source and destination paths.

### Step 3: Read Orphan Records
**Action:** `read`  
**Description:** Reads the orphan records into a temporary view for further processing.
- **Parameters:**
  - `path`: The S3 path from where to read the data.
  - `view`: The name of the temporary view to create in Spark.

### Step 4: Select Data
**Action:** `execute_query`  
**Description:** Executes a query to select data from the temporary view created in Step 3.
- **Parameters:**
  - `query`: The SQL query to execute.
  - `view`: The name of the view to use.

### Step 5: Write Data to Target
**Action:** `write`  
**Description:** Writes the processed data back to the target S3 location, partitioned by specified fields for optimization.
- **Parameters:**
  - `path`: The S3 path where the data will be written.
  - `partition_fields`: Fields used to partition the data.
  - `view`: The name of the temporary view containing the data to write.

### Step 6: Read Target Data
**Action:** `read`  
**Description:** Reads the data from the target location to verify the write operation.
- **Parameters:**
  - `path`: The S3 path from where to read the data.

### Step 7: Compare Schemas
**Action:** `compare_schema`  
**Description:** Compares the schema of the original data with the updated data to ensure consistency.
- **Parameters:**
  - `path1`: The S3 path of the original data.
  - `path2`: The S3 path of the updated data.

## Actions Explained

### `execute_query`
This action executes an SQL query using a specified engine (typically Spark). It is used for data transformation and table creation.

### `s3_copy`
This action copies data from one S3 location to another. It is primarily used for creating backups.

### `read`
This action reads data from an S3 location and loads it into a temporary view in Spark. It is used for intermediate data processing.

### `write`
This action writes data from a temporary view in Spark to an S3 location. It can partition the data based on specified fields to optimize storage and retrieval.

### `compare_schema`
This action compares the schemas of two datasets to ensure they match. It is used for validation purposes.

### `run_crawler`
This action runs the crawler on the set of paths.
{
  "sql": {
    "step8_run_crawler": {
    	  "table_prefix": "test_crawler",
	      "action": "run_crawler",
		  "database_name": "enterpriseanalytics_edh_sprt",
		  "s3_target_path": "s3://baxaws-tst-enterpriseanalytics-edh-gabi-translate/backup/EMEA/finance/generalledger/p_jde_gl/202500207105800/p_jde_gl/",
	      "role": "baxaws-test-enterpriseanalytics-edh-glue-role",
	      "crawler_name": "baxaws_enterpriseanalytics_edh_sop_crawler"
  }
  }
}
Also make sure to have role name as test or prod in it. 

## Views Explained

### Temporary Views
Temporary views in Spark are in-memory representations of data that facilitate intermediate processing steps. They allow for efficient data manipulation and querying without persisting the data to storage. By using views, the framework can streamline data transformations and reduce I/O operations.

## Control M Scheduling
Run SOP from control M by passing the location of json file which has list of steps. Here is example
```bash
cd /home/hadoop/dp_scripts
./run_sop.sh s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib/configs/dp_apps/dp_sop.json

./run_sop.sh s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib/head-snapshot/master/configs/dp_apps/dp_sop.json

./run_sop.sh 
s3://baxaws-tst-enterpriseanalytics-edh-gabi-translate/LATAM/supplychain/dp_sop_LATAM_P_JDE_PO_RCPT_DTL_DEDUP_1112.json

s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/scripts/dp_sop_LATAM_P_JDE_PO_RCPT_DTL_DEDUP_11222024.json

./run_sop.sh  s3://baxaws-tst-enterpriseanalytics-edh-gabi-translate/LATAM/supplychain/dp_sop_crawler.json

```

## Future Improvements

Remove Drop privileage in Framework.
Add more validations for actions and engines.
Remove athena code.
Add all these validations before executing framework.
COntrol M will track users who kill this. Also add SNS notification or event driven for platform or L2 team to get notified.
Add --skip then it will not that particular step.


## Conclusion

Constructing the SOP JSON document for the EDH Framework involves defining a series of steps with specific actions and parameters. Each step plays a crucial role in the data processing workflow, ensuring data integrity, consistency, and efficiency. By following the structure and guidelines provided in this documentation, you can create robust workflows that leverage the full capabilities of the EDH Framework.