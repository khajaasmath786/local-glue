"""
Process CDC DATA from DMS S3 location into partitioned HUDI table.

Takes json string input with schema, table, primary key, and partition fields
 and writes output to consolidated partitioned hudi table.

 Command to Run in AWS EMR step :
 s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/scripts/baxaws-enterpriseanalytics-edh-consolidate-partition.sh s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/scripts/duplicate_partition_writer.py baxaws-prd-enterpriseanalytics-edh-jdeeu-inbound eu-central-1 baxaws-prd-enterpriseanalytics-edh-jde-inbound EUDTA F4311 PDDOCO,PDDCTO,PDKCOO,PDSFXO,PDLNID change_seq 5 6 6 "year(to_date(cast(((case when PDTRDJ=0 then 1 else PDTRDJ end) + 1900000) AS string), 'yyyyDDD'))" !

 Query:

 SELECT a.pdkcoo,
       a.pddoco,
       a.pddcto,
       a.pdsfxo,
       a.pdlnid,
       a.op,
       a.pdtrdj,
       a.partitionpath
FROM   enterpriseanalytics_edh_apdta.f4311 a
       JOIN (SELECT pdkcoo,
                    pddoco,
                    pddcto,
                    pdsfxo,
                    pdlnid,
                    op,
                    Count(*)
             FROM   enterpriseanalytics_edh_apdta.f4311
             GROUP  BY pdkcoo,
                       pddoco,
                       pddcto,
                       pdsfxo,
                       pdlnid,
                       op
             HAVING Count(*) > 1) b
         ON a.pdkcoo = b.pdkcoo
            AND a.pddoco = b.pddoco
            AND a.pddcto = b.pddcto
            AND a.pdsfxo = b.pdsfxo
            AND a.pdlnid = b.pdlnid
            AND a.op = b.op

SELECT Count(*)
FROM   enterpriseanalytics_edh_apdta.f594102a_adt
WHERE  iby59pc19 = '1'
       AND Trim(ibmcu) = '4013MAIN'

SELECT ibitm,
       ibmcu,
       Count(*)
FROM   enterpriseanalytics_edh_apdta.f594102a_adt
GROUP  BY ibitm,
          ibmcu
HAVING Count(*) > 1 
"""
import optparse
import os
import re
import subprocess
import sys
from functools import reduce

import boto3
import pyspark.sql.functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    count,
    current_date,
    desc,
    expr,
    lit,
    regexp_replace,
    row_number,
    when,
    year,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

if os.environ.get('LC_CTYPE', '') == 'UTF-8':
    os.environ['LC_CTYPE'] = 'en_US.UTF-8'


# General Constants
HUDI_FORMAT = "org.apache.hudi"
TABLE_NAME = "hoodie.table.name"
RECORDKEY_FIELD_OPT_KEY = "hoodie.datasource.write.recordkey.field"
PRECOMBINE_FIELD_OPT_KEY = "hoodie.datasource.write.precombine.field"
OPERATION_OPT_KEY = "hoodie.datasource.write.operation"
BULK_INSERT_OPERATION_OPT_VAL = "bulk_insert"
UPSERT_OPERATION_OPT_VAL = "upsert"
DELETE_OPERATION_OPT_VAL = "delete"
BULK_INSERT_PARALLELISM = "hoodie.bulkinsert.shuffle.parallelism"
UPSERT_PARALLELISM = "hoodie.upsert.shuffle.parallelism"
S3_CONSISTENCY_CHECK = "hoodie.consistency.check.enabled"
HUDI_CLEANER_POLICY = "hoodie.cleaner.policy"
KEEP_LATEST_COMMITS = "KEEP_LATEST_COMMITS"
HUDI_COMMITS_RETAINED = "hoodie.cleaner.commits.retained"
HUDI_MIN_COMMITS = "hoodie.keep.min.commits"
HUDI_MAX_COMMITS = "hoodie.keep.max.commits"
PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
EMPTY_PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload"
HUDI_TIMELINE_SERVER = "hoodie.embed.timeline.server"
HUDI_ROW_WRITER = "hoodie.datasource.write.row.writer.enable"
HUDI_FAIL_ARCHIVING = "hoodie.fail.on.timeline.archiving"
HUDI_METADATA_TABLE = "hoodie.metadata.enable"

# Hive Constants
HIVE_SYNC_ENABLED_OPT_KEY = "hoodie.datasource.hive_sync.enable"
HIVE_PARTITION_FIELDS_OPT_KEY = "hoodie.datasource.hive_sync.partition_fields"
HIVE_ASSUME_DATE_PARTITION_OPT_KEY = "hoodie.datasource.hive_sync.assume_date_partitioning"
HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = "hoodie.datasource.hive_sync.partition_extractor_class"
HIVE_TABLE_OPT_KEY = "hoodie.datasource.hive_sync.table"
HIVE_DATABASE_OPT_KEY = "hoodie.datasource.hive_sync.database"

# Partition Constants
NONPARTITION_EXTRACTOR_CLASS_OPT_VAL = "org.apache.hudi.hive.NonPartitionedExtractor"
MULTIPART_KEYS_EXTRACTOR_CLASS_OPT_VAL = "org.apache.hudi.hive.MultiPartKeysValueExtractor"
KEYGENERATOR_CLASS_OPT_KEY = "hoodie.datasource.write.keygenerator.class"
NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
COMPLEX_KEYGENERATOR_CLASS_OPT_VAL = "org.apache.hudi.keygen.ComplexKeyGenerator"
PARTITIONPATH_FIELD_OPT_KEY = "hoodie.datasource.write.partitionpath.field"

# Incremental Constants
VIEW_TYPE_OPT_KEY = "hoodie.datasource.view.type"
BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime"
VIEW_TYPE_INCREMENTAL_OPT_VAL = "incremental"
END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime"

bolHiveSync="true"

glue_db_prefix = "ENTERPRISEANALYTICS_EDH_"
schema_type = '_HIST'

parser = optparse.OptionParser()
parser.add_option("--sbucket", type=str, default="baxaws-prd-enterpriseanalytics-edh-jdeap-inbound",
                  help="input s3 source bucket name")
parser.add_option("--sregion", type=str, default="ap-southeast-1",
                  help="input s3 source bucket region")
parser.add_option("-b", "--bucket", type=str, default="baxaws-prd-enterpriseanalytics-edh-jde-inbound",
                  help="input s3 bucket name")
parser.add_option("-s", "--schema", type=str, default="EUDTA",
                  help="input schema name")
parser.add_option("-t", "--table", type=str, default="F42119",
                  help="input table name")
parser.add_option("--primary_key", type=str, default="SDDOCO,SDDCTO,SDKCOO,SDLNID",
                  help="input primary key. When using multiple columns as primary key, use comma separated notation")
parser.add_option("--sort_key", type=str, default="change_seq",
                  help="input sort key")
parser.add_option("-r", "--max_executors", type=str, default="5",
                  help="input a number for max_executors")
parser.add_option("-p", "--bulk_insert_parallelism", type=int, default=6,
                  help="input a number for insert parallelism")
parser.add_option("-u", "--upsert_parallelism", type=int, default=6,
                  help="input a number for upsert parallelism")
parser.add_option("--partition_key", type=str,
                  help="column used to partition data", default="year(to_date(cast(((case when PDTRDJ=0 then 1 else PDTRDJ end) + 1900000) AS string), 'yyyyDDD'))")
parser.add_option("--insert_hist", type=str, default="",
                  help="list of columns to retain initial insert history")

(options, args) = parser.parse_args()
config = {
    "sbucket": options.sbucket,
    "sregion": options.sregion,
    "bucket": options.bucket,
    "schema": options.schema,
    "table_name": options.table,
    "source": os.path.join("s3://", options.sbucket, "raw", options.schema, options.table),
    "processed-raw": os.path.join("s3://", options.sbucket, "raw-processed", options.schema, options.table),
    "target": os.path.join("s3://", options.bucket, "raw-consolidated", options.schema, options.table + '_PART'),
    "ihtarget": os.path.join("s3://", options.bucket, "raw-consolidated", options.schema + schema_type, options.table),
    "primary_key": options.primary_key,
    "sort_key": options.sort_key,
    "max_executors": options.max_executors,
    "bulk_insert_parallelism": options.bulk_insert_parallelism,
    "upsert_parallelism": options.upsert_parallelism,
    "partition_key": options.partition_key,
    "insert_hist": options.insert_hist}

print(config)

sbucket = config['sbucket']
sregion = config['sregion']
bucket = config['bucket']

source_prefix = "raw" + '/' + \
    config['schema'] + '/' + config['table_name'] + '/'
processed_prefix = "raw-processed" + '/' + \
    config['schema'] + '/' + config['table_name'] + '/'
target_prefix = "raw-consolidated" + '/' + \
    config['schema'] + '/' + config['table_name'] + '/'


def create_spark_session():
    conf = SparkConf()
    conf.set("spark.executor.instances", "1")
    conf.set("spark.dynamicAllocation.minExecutors", "1")
    conf.set("spark.dynamicAllocation.maxExecutors", config['max_executors'])
    conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    conf.set("spark.sql.avro.datetimeRebaseModeInWrite", "LEGACY")
    conf.set("spark.sql.parquet.writeLegacyFormat", "true")

    appname = config['schema'] + '-' + config['table_name'] + '-CDC'
    spark = SparkSession.builder.enableHiveSupport().appName(appname).config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")    
    return spark

def validate(spark,config):
    missing_records_df, still_exists_df= verify_data_integrity(spark=spark,config=config)

    print_dataframe_columns(spark=spark,config=config,df = missing_records_df)
    print_dataframe_columns(spark=spark,config=config,df = still_exists_df)

def process(config):
    """
    Start consolidation process.

    Gets file list based on criteria
    , filters the list
    , processes full load files
     limits the number of cdc files processed.
    """
    sbucket = config['sbucket']
    sregion = config['sregion']
    bucket = config['bucket']

    # select count(*) from enterpriseanalytics_edh_eudta.F4311 - 2740838
    spark=create_spark_session() 
    # insert_duplicates(spark=spark,config=config)    
    
    get_table_record_count(spark=spark,config=config)
    sys.exit(0)
    # partition_columns = get_partition_columns(spark, config)
    df,result_df = find_duplicates_with_rank(config, spark)  
        
    compare_df_schemas(spark,df)
    # df.show()
    # result_df.show() 
    register_table_in_glue(spark=spark,df=df,config=config)
    deleteRecordsInHudi(spark=spark,config=config,df=df)
    missing_records_df, still_exists_df= verify_data_integrity(spark=spark,config=config)

    print_dataframe_columns(spark=spark,config=config,df = missing_records_df)
    print_dataframe_columns(spark=spark,config=config,df = still_exists_df)

    get_table_record_count(spark=spark,config=config)
    
    #processCDCFiles(config=config)


import re

from pyspark.sql.types import *


def compare_df_schemas(spark,df):
    is_partitioned = False  # This should be dynamically determined based on actual table settings

    table_full_name = f"{glue_db_prefix.lower()}{config['schema'].lower()}.{config['table_name'].lower()}"
    glue_schema = fetch_schema_from_glue(spark, table_full_name, is_partitioned)
    compare_schemas(glue_schema, df.dtypes) 


def register_table_in_glue(spark, df, config):
    # Extract schema from DataFrame and form the SQL command
    schema = df.schema
    columns_sql = ', '.join(f"{field.name} {field.dataType.simpleString()}" for field in schema.fields)
    db_name=f"{glue_db_prefix.lower()}{config['schema'].lower()}" 
    table_name = f"{config['table_name']}_duplicates"
    parquet_path = f"s3://{config['bucket']}/raw-consolidated/{config['schema'].upper()}/{config['table_name'].upper()}_PART_BACKUP/"
    #parquet_path = f"s3://{config['bucket']}/path/to/backup/{table_name}/"

    # Save DataFrame to S3 as Parquet
    df.write.format("parquet").mode("overwrite").save(parquet_path)

    # Execute SQL to create external table in AWS Glue
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name} (
            {columns_sql}
        )
        STORED AS PARQUET
        LOCATION '{parquet_path}'
    """)

    print(f"Table {db_name}.{table_name} registered in AWS Glue Data Catalog.")

def print_dataframe_columns(spark, config, df):
    """
    Print specified columns from the DataFrame based on the configuration.
    
    :param spark: SparkSession instance
    :param config: Configuration dictionary containing primary_key and possibly partition_columns
    :param df: DataFrame to process
    """
    primary_keys = [key.strip() for key in config['primary_key'].split(",")]
    partition_columns = get_partition_columns(spark, config)
    
    # Select primary keys, hoodie commit time, and partition columns if they exist
    columns_to_select = [col(key) for key in primary_keys]
    columns_to_select += [col("_hoodie_commit_time")]
    
    if partition_columns:
        columns_to_select += [col(key) for key in partition_columns]
    
    # Select the desired columns from the dataframe
    result_df = df.select(*columns_to_select)
    
    # Show the resulting DataFrame with columns not truncated
    if config.get('debug', False):
        result_df.show(truncate=False)

def fetch_schema_from_glue(spark, table_name, is_partitioned=False):
    """
    Fetch the schema of a table from AWS Glue using Spark SQL, considering whether it's partitioned.

    Parameters:
    - spark: Spark session.
    - table_name: The name of the table in 'database.table' format.
    - is_partitioned: Boolean indicating if the table is partitioned.

    Returns:
    - A dictionary mapping column names to their data types.
    """
    schema_df = spark.sql(f"DESCRIBE FORMATTED {table_name}")
    schema_rows = schema_df.collect()
    schema = {}
    is_partition_section = False

    for row in schema_rows:
        # Toggle the reading if partition information starts or ends
        if "# Partition Information" in row['col_name']:
            is_partition_section = True
            continue
        if is_partition_section and row['col_name'] == "":
            break  # Stop if no more partition info is present

        # Read columns until partition information or end of relevant data
        if not is_partition_section and row['col_name'] and row['data_type']:
            schema[row['col_name']] = row['data_type']

    return schema

def compare_schemas(glue_schema, df_schema):
    """
    Compare two schemas and print out differences.

    Parameters:
    - glue_schema: Schema dictionary from Glue.
    - df_schema: Schema list from DataFrame (df.dtypes).
    """
    df_schema_dict = dict(df_schema)
    extra_columns = set(df_schema_dict.keys()) - set(glue_schema.keys())
    print("Extra Columns in DataFrame compared to Glue Table Schema:", extra_columns)

def get_partition_columns(spark, config):
    """
    Fetches the partition columns of a table from the AWS Glue Data Catalog.

    Parameters:
    - spark: SparkSession, the Spark session object.
    - database_name: str, the name of the database.
    - table_name: str, the name of the table.

    Returns:
    - List of partition column names if the table is partitioned, otherwise an empty list.
    """
    db_schema= glue_db_prefix.lower() + config['schema'].lower()    
    target_table = f"{db_schema}.{config['table_name'].lower()}"
    try:
        # Use DESCRIBE EXTENDED to get detailed metadata about the table
        df = spark.sql(f"DESCRIBE EXTENDED {target_table}")
        # Collect the dataframe into a list of rows
        rows = df.collect()

        # Find the starting index for partition information
        partition_info_index = next((index for index, row in enumerate(rows) if "Partition Information" in row['col_name']), None)

        if partition_info_index is not None:
            # Extract partition columns from the rows following the partition information header
            partition_columns = []
            for row in rows[partition_info_index + 2:]:  # Start from next to 'col_name data_type comment'
                if row['col_name'].strip() and row['data_type'].strip():  # Ensure there's valid data
                    partition_columns.append(row['col_name'].strip())
                else:
                    break  # Stop if there are no more partition columns
            return partition_columns
        
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    
def find_duplicates_with_rank(config,spark):

    partition_columns = get_partition_columns(spark, config)
    primary_keys = [key.strip() for key in config['primary_key'].split(",")]
    
    hudi_table_path = f"s3://{config['bucket']}/raw-consolidated/{config['schema'].upper()}/{config['table_name'].upper()}_PART/"
    
    if partition_columns:
        hudi_table_path = f"s3://{config['bucket']}/raw-consolidated/{config['schema'].upper()}/{config['table_name'].upper()}_PART/"
    else:
        hudi_table_path = f"s3://{config['bucket']}/raw-consolidated/{config['schema'].upper()}/{config['table_name'].upper()}/"
    # Group by primary keys and count duplicates
    df = spark.read.format("hudi").load(hudi_table_path + "/*").alias("df")
    duplicates = df.groupBy(*[col(key) for key in primary_keys]).agg(count("*").alias("count")).filter(col("count") > 1)

    # Alias the dataframes to resolve column ambiguity
    df_alias = df.alias("df")
    duplicates_alias = duplicates.alias("duplicates")

    # Join the original DataFrame with the duplicates to filter out non-duplicates
    join_conditions = [df_alias[key] == duplicates_alias[key] for key in primary_keys]
    duplicate_df = df_alias.join(duplicates_alias, on=join_conditions, how='inner')

    # Define a window specification ordered by hoodie_commit_time descending
    order_by_columns = [desc("df." + col) for col in partition_columns] + [desc("df._hoodie_commit_time")]
    windowSpec = Window.partitionBy([df_alias[key] for key in primary_keys]).orderBy(*order_by_columns)

    # Apply the row_number function to assign ranks within each group of duplicates
    ranked_df = duplicate_df.withColumn("rank", row_number().over(windowSpec))

    # Programmatically select columns, prefixing with the appropriate DataFrame alias
    selected_columns = ["df." + name for name in df.columns] + ["duplicates.count", "rank"]
    
    df = ranked_df.select(*selected_columns)
    df.printSchema()
    result_columns = [col("df." + key) for key in primary_keys] + [col("df." + key) for key in partition_columns]+ ["rank", "df._hoodie_commit_time", "duplicates.count"]
    result_df=df.select(*result_columns)
    if config.get('debug', False):
        result_df.show(truncate=False)          
    return df,result_df

def delete_records_partitioned_table(spark, config, df):
    if df.count() > 0:
        # Proceed with deletion
        (df.write.format(HUDI_FORMAT)
        .option(PRECOMBINE_FIELD_OPT_KEY, config["sort_key"])
        .option(RECORDKEY_FIELD_OPT_KEY, "primary_key")
        .option(TABLE_NAME, config['table_name'])
        .option(OPERATION_OPT_KEY, DELETE_OPERATION_OPT_VAL)
        .option(UPSERT_PARALLELISM, config['upsert_parallelism'])
        .option(HUDI_CLEANER_POLICY, KEEP_LATEST_COMMITS)
        .option(HUDI_MIN_COMMITS, 240)
        .option(HUDI_MAX_COMMITS, 300)
        .option(HUDI_COMMITS_RETAINED, 120)
        .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
        .option(HIVE_PARTITION_FIELDS_OPT_KEY, "partitionpath")
        .option(HIVE_TABLE_OPT_KEY, config['table_name'])
        .option(HIVE_SYNC_ENABLED_OPT_KEY, bolHiveSync)
        .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, MULTIPART_KEYS_EXTRACTOR_CLASS_OPT_VAL)
        .option(KEYGENERATOR_CLASS_OPT_KEY, COMPLEX_KEYGENERATOR_CLASS_OPT_VAL)
        .option(HIVE_DATABASE_OPT_KEY, glue_db_prefix.lower() + config['schema'].lower())
        .option(HUDI_METADATA_TABLE, "false")
        .option(HUDI_TIMELINE_SERVER, "false")
        .option(HUDI_ROW_WRITER, "false")
        .option(HUDI_FAIL_ARCHIVING, "false")
        .mode("Append")
        .save(config['target']))

        print("Deleted records successfully.")

    else:
        print("No records to delete based on the specified criteria.")

def delete_records_non_partitioned_table(spark, config, df):
    output_path = config['target']
    output_path = output_path.rstrip("_PART") if output_path.endswith("_PART") else output_path
    config['target'] = output_path
    if df.count() > 0:
        # Proceed with deletion
        (df.write.format(HUDI_FORMAT)
        .option(PRECOMBINE_FIELD_OPT_KEY, config["sort_key"])
        .option(RECORDKEY_FIELD_OPT_KEY, "primary_key")
        .option(TABLE_NAME, config['table_name'])
        .option(OPERATION_OPT_KEY, DELETE_OPERATION_OPT_VAL)
        .option(UPSERT_PARALLELISM, config['upsert_parallelism'])
        .option(HUDI_CLEANER_POLICY, KEEP_LATEST_COMMITS)
        .option(HUDI_MIN_COMMITS, 240)
        .option(HUDI_MAX_COMMITS, 300)
        .option(HUDI_COMMITS_RETAINED, 120)
        # .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
        # .option(HIVE_PARTITION_FIELDS_OPT_KEY, "partitionpath")
        .option(HIVE_TABLE_OPT_KEY, config['table_name'])
        .option(HIVE_SYNC_ENABLED_OPT_KEY, bolHiveSync)
        .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, NONPARTITION_EXTRACTOR_CLASS_OPT_VAL)
        .option(KEYGENERATOR_CLASS_OPT_KEY, NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL)
        .option(HIVE_DATABASE_OPT_KEY, glue_db_prefix.lower() + config['schema'].lower())
        .option(HUDI_METADATA_TABLE, "false")
        .option(HUDI_TIMELINE_SERVER, "false")
        .option(HUDI_ROW_WRITER, "false")
        .option(HUDI_FAIL_ARCHIVING, "false")
        .mode("Append")
        .save(config['target']))
        print("Deleted records successfully.")

    else:
        print("No records to delete based on the specified criteria.")

def deleteRecordsInHudi(spark,config, df):
    """
    Delete records from a Hudi table using the delete operation based on provided filters.
    """
    # Define the path to the Hudi table
    hudi_table_path = config['target']  # Ensure this points to the intended Hudi storage location

    
    # Filter the DataFrame to identify records to delete
    # df_to_delete.show()
    df_to_delete = df.filter("rank > 1")  # Example filter, adjust based on your criteria
    if config.get('debug', False):
        df_to_delete.show()
    df_to_delete=df_to_delete.drop("rank", "count")

    partition_columns = get_partition_columns(spark, config)
    if partition_columns:
        print("inside partitioned table deletion")        
        delete_records_partitioned_table(spark, config, df_to_delete)        
    else:
        print("inside non partitioned table deletion")
        delete_records_non_partitioned_table(spark, config, df_to_delete)
        
  
def get_table_record_count(spark, config):
    """
    Verify data integrity between the backup table of duplicates and the target table
    after operations. Optionally includes comparison of Hoodie commit times for accurate record matching.
    
    Parameters:
    - spark: Spark session object.
    - config: Configuration dictionary containing schema, table names, etc.
    """
    db_schema= glue_db_prefix.lower() + config['schema'].lower()
    backup_table_path = f"{db_schema}.{config['table_name'].lower()}_duplicates"
    target_table = f"{db_schema}.{config['table_name'].lower()}"
    target_df = spark.sql(f"SELECT * FROM {target_table}")
    print(f"Count of the table {target_table} is {target_df.count()}")

def insert_duplicates(spark, config):
    """
    Process CDC files generated by dms.

    This function appends cdc files to initial file using Hudi append
    """
    
    bolHiveSync = "true"
    

    colFilter = config['primary_key']
    partcols = config['partition_key']
    table_name=config['table_name'].lower() 
    db_schema= glue_db_prefix.lower() + config['schema'].lower()    
    target_table = f"{db_schema}.{config['table_name'].lower()}"
    df1 = spark.sql(f"SELECT * FROM {target_table} where partitionpath=2023 limit 5")
    df1.printSchema()
    if config.get('debug', False):
        df1.show()
    
 
    df1 = df1.withColumn("partitionpath", year(current_date()))
    columns_to_drop = [col for col in df1.columns if col.startswith("_hoodie")]
    df1 = df1.drop(*columns_to_drop)   
   

    # Use Hudi to append cdc files to initial file
    (df1.write.format(HUDI_FORMAT)
     .option(PRECOMBINE_FIELD_OPT_KEY, config["sort_key"])
     .option(RECORDKEY_FIELD_OPT_KEY, "primary_key")
     .option(TABLE_NAME, config['table_name'])
     .option(OPERATION_OPT_KEY, UPSERT_OPERATION_OPT_VAL)
     .option(UPSERT_PARALLELISM, config['upsert_parallelism'])
     .option(HUDI_CLEANER_POLICY, KEEP_LATEST_COMMITS)
     .option(HUDI_MIN_COMMITS, 240)
     .option(HUDI_MAX_COMMITS, 300)
     .option(HUDI_COMMITS_RETAINED, 120)
     .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
     .option(HIVE_PARTITION_FIELDS_OPT_KEY, "partitionpath")
     .option(HIVE_TABLE_OPT_KEY, config['table_name'])
     .option(HIVE_SYNC_ENABLED_OPT_KEY, bolHiveSync)
     .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, MULTIPART_KEYS_EXTRACTOR_CLASS_OPT_VAL)
     .option(KEYGENERATOR_CLASS_OPT_KEY, COMPLEX_KEYGENERATOR_CLASS_OPT_VAL)
     .option(HIVE_DATABASE_OPT_KEY, glue_db_prefix.lower() + config['schema'].lower())
     .option(HUDI_METADATA_TABLE, "false")
     .option(HUDI_TIMELINE_SERVER, "false")
     .option(HUDI_ROW_WRITER, "false")
     .option(HUDI_FAIL_ARCHIVING, "false")
     .mode("Append")
     .save(config['target']))

    print("FileList processed!\n")

def verify_data_integrity(spark, config):
    """
    Verifies data integrity by ensuring that all expected records (rank = 1) are still present in the target table
    post deletion and no records that were supposed to be deleted (rank > 1) remain.

    Parameters:
    - spark: SparkSession, the Spark session object.
    - config: dict, configuration dictionary containing schema, table names, etc.

    This function checks:
    1. Records with rank = 1 from the backup are still present in the target.
    2. No records with rank > 1 from the backup remain in the target.
    """
    db_schema = glue_db_prefix.lower() + config['schema'].lower()
    backup_table = f"{db_schema}.{config['table_name'].lower()}_duplicates"
    target_table = f"{db_schema}.{config['table_name'].lower()}"

    # Load duplicate and target data with proper aliasing
    duplicates_df = spark.sql(f"SELECT *, _hoodie_commit_time as dup_commit_time, primary_key as dup_primary_key FROM {backup_table}")
    target_df = spark.sql(f"SELECT *, _hoodie_commit_time as target_commit_time, primary_key as target_primary_key FROM {target_table}")

    # Check for records that should still exist (rank = 1)
    supposed_to_exist_df = duplicates_df.filter("rank = 1").alias('supposed')
    missing_records_df = supposed_to_exist_df.join(
        target_df.alias('target'),
        (col('supposed.dup_primary_key') == col('target.target_primary_key')) &
        (col('supposed.dup_commit_time') == col('target.target_commit_time')),
        'left_anti'
    ).select('supposed.*')

    if missing_records_df.count() > 0:
        print("Error: There are expected records missing from the target table.")
        if config.get('debug', False):
            missing_records_df.show(truncate=False)
    else:
        print("Verification successful: All expected records are present in the target table.")

    # Check for records that should not exist (rank > 1)
    not_supposed_to_exist_df = duplicates_df.filter("rank > 1").alias('not_supposed')
    still_exists_df = not_supposed_to_exist_df.join(
        target_df.alias('target'),
        (col('not_supposed.dup_primary_key') == col('target.target_primary_key')) &
        (col('not_supposed.dup_commit_time') == col('target.target_commit_time')),
        'inner'
    ).select('target.*')
    # Above statement is not working 
    qry =f"""
          SELECT a.*
          FROM   {target_table} a join {backup_table} b 
          ON a.primary_key = b.primary_key
          and a._hoodie_commit_time = b._hoodie_commit_time
          and b.rank = 1
        """
    still_exists_df = spark.sql(qry)    
    if config.get('debug', False):
        still_exists_df.show()

    if still_exists_df.count() > 0:
        print("Error: There are records that should have been deleted but still exist in the target table.")
        if config.get('debug', False):
            still_exists_df.show(truncate=False)
    else:
        print("Verification successful: No unintended records are in the target table.")

    return missing_records_df, still_exists_df

if __name__ == '__main__':
    process(config)
