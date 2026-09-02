#!/bin/bash
set -e

# Export the Python environment variable
export PYSPARK_PYTHON=python3


# Function to display usage information
usage() {
  echo "Usage: $0 --action <list|kill|grep> [--identifier <job_id|job_name>]"
  exit 1
}

# Function to determine the S3 bucket based on AWS account ID
determine_s3_bucket() {
  local account_id=$(aws sts get-caller-identity --query "Account" --output text)
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

# Parse command-line arguments
while [[ $# -gt 0 ]]
do
  key="$1"

  case $key in
    --action)
      ACTION="$2"
      shift # past argument
      shift # past value
      ;;
    --identifier)
      IDENTIFIER="$2"
      shift # past argument
      shift # past value
      ;;
    *)
      usage
      ;;
  esac
done

# Validate action argument
if [[ "$ACTION" != "list" && "$ACTION" != "kill" && "$ACTION" != "grep" ]]; then
  echo "Error: Invalid action. Valid actions are 'list', 'kill', 'grep'."
  usage
fi

# Determine the S3 bucket
s3_bucket=$(determine_s3_bucket)

# Define the driver script location
driver_script="${s3_bucket}/lib/jobs/dp_sop/yarn_job_manager.py"

# Construct the common part of the spark-submit command
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
  $driver_script"

# Add specific parameters based on the action
if [ "$ACTION" == "list" ]; then
  spark_submit_cmd="$spark_submit_cmd --list"
elif [ "$ACTION" == "grep" ]; then
  if [ -z "$IDENTIFIER" ]; then
    echo "Error: --identifier is required for action 'grep'."
    usage
  fi
  spark_submit_cmd="$spark_submit_cmd --grep $IDENTIFIER"
elif [ "$ACTION" == "kill" ]; then
  if [ -z "$IDENTIFIER" ]; then
    echo "Error: --identifier is required for action 'kill'."
    usage
  fi
  spark_submit_cmd="$spark_submit_cmd --kill $IDENTIFIER"
fi

# Execute the spark-submit command
eval $spark_submit_cmd
