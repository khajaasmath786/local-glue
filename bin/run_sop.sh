#!/bin/bash
set -e

# Export the Python environment variable
export PYSPARK_PYTHON=python3

# Define the AWS CLI profile name
AWS_PROFILE="saml"

# Function to display usage information
usage() {
  echo "Usage: $0 <json_file_location>"
  exit 1
}

# Function to determine the S3 bucket based on AWS account ID
determine_s3_bucket() {
  local account_id=$(aws sts get-caller-identity --query "Account" --output text )
  case $account_id in
    143049391535)
      echo "s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib/head-snapshot/master"
      ;;
    737965399985)
      echo "s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib/head-snapshot/master"
      ;;
    203058073716)
      echo "s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/scripts/edh_pipelib/head-snapshot/master"
      ;;
    *)
      echo "Unknown account ID: $account_id. Exiting."
      exit 1
      ;;
  esac
}

# Check if at least one argument is provided
if [ $# -lt 1 ]; then
  usage
fi

# Get the JSON file location from the first argument
json_file_location=$1

# Validate JSON file location argument
if [ -z "$json_file_location" ]; then
  usage
fi

# Determine the S3 bucket
s3_bucket=$(determine_s3_bucket)

# Echo the JSON file location
echo "JSON file location: $json_file_location"

# Define the driver script location
driver_script="${s3_bucket}/lib/jobs/dp_sop/run.py"

# Construct the spark-submit command
spark_submit_cmd="spark-submit --py-files ${s3_bucket}/dist/edh_pipelib.zip \
  --jars \"/usr/lib/spark/jars/httpclient-4.5.9.jar,/usr/lib/spark/jars/httpcore-4.4.11.jar,/usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar\" \
  --conf \"spark.serializer=org.apache.spark.serializer.KryoSerializer\" \
  --conf \"spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog\" \
  --conf \"spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension\" \
  --conf \"spark.port.maxRetries=600\" \
  --conf \"spark.driver.maxResultSize=2g\" \
  --conf \"spark.rdd.compress=true\" \
  --conf \"spark.kryoserializer.buffer.max=512m\" \
  --conf \"spark.shuffle.service.enabled=true\" \
  --conf \"spark.driver.memoryOverhead=1024\" \
  --conf \"spark.executor.memoryOverhead=3072\" \
  --executor-cores 5 \
  --executor-memory 8G \
  --driver-memory 8G \
  $driver_script $json_file_location"

# Execute the spark-submit command
eval $spark_submit_cmd
