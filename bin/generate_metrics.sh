#!/bin/bash
set -e

export PYSPARK_PYTHON=python3
AWS_PROFILE="saml"

determine_s3_bucket() {
  account_id=$(aws sts get-caller-identity --query "Account" --output text)
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
      echo "Unknown account ID: $account_id"
      exit 1
      ;;
  esac
}

run_spark_job() {
  s3_bucket=$(determine_s3_bucket)
  driver_script="${s3_bucket}/lib/jobs/dp_read/data_metrics.py"

  spark-submit --py-files "${s3_bucket}/dist/edh_pipelib.zip" \
    --jars "/usr/lib/spark/jars/httpclient-4.5.9.jar,/usr/lib/spark/jars/httpcore-4.4.11.jar,/usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
    --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
    --conf "spark.port.maxRetries=600" \
    --conf "spark.driver.maxResultSize=2g" \
    --conf "spark.rdd.compress=true" \
    --conf "spark.kryoserializer.buffer.max=512m" \
    --conf "spark.shuffle.service.enabled=true" \
    --conf "spark.driver.memoryOverhead=1024" \
    --conf "spark.executor.memoryOverhead=3072" \
    --executor-cores 5 \
    --executor-memory 8G \
    --driver-memory 8G \
    "$driver_script"
}

run_spark_job
