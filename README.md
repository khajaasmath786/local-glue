# local-glue



docker pull amazon/aws-glue-libs:glue_libs_3.0.0_image_01

$CREDENTIAL_LOCATION = "C:\Users\mohammk2\.aws"
$WORKSPACE_LOCATION = "C:\asmath_workspace\dp_workspace"
$AWS_PROFILE="saml"
$REGION="us-east-2"


docker run -itd -p 4040:4040 -p 18080:18080 -v ${CREDENTIAL_LOCATION}:/home/glue_user/.aws -v ${WORKSPACE_LOCATION}:/home/glue AWS_PROFILE=${AWS_PROFILE} -e AWS_REGION=${REGION} --name glue_pyspark amazon/aws-glue-libs:glue_libs_3.0.0_image_01 pyspark

docker run -itd -p 4040:4040 -p 18080:18080 -v ${CREDENTIAL_LOCATION}:/home/glue_user/.aws -v ${WORKSPACE_LOCATION}:/home/glue AWS_PROFILE=${AWS_PROFILE} -e AWS_REGION=${REGION} --name glue_pyspark public.ecr.aws/glue/aws-glue-libs:5 pyspark


Go to remote explorer in vscode, attach to this container in current window.
Then go to workspace folder of the container to see your files of your attach workspace ..

----

    docker stop glue_pyspark
    docker start glue_pyspark
    docker exec -t glue_pyspark /bin/bash -c "echo spark.jars.packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.12.0 >> $SPARK_CONF_DIR/spark-defaults.conf"
    docker exec -t glue_pyspark /bin/bash -c "echo spark.serializer=org.apache.spark.serializer.KryoSerializer >> $SPARK_CONF_DIR/spark-defaults.conf"
    docker exec -t glue_pyspark /bin/bash -c "echo spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension >> $SPARK_CONF_DIR/spark-defaults.conf"
    docker exec -t glue_pyspark /bin/bash -c "echo spark.driver.cores=18 >> $SPARK_CONF_DIR/spark-defaults.conf"
    docker exec -t glue_pyspark /bin/bash -c "echo spark.driver.memory=24g >> $SPARK_CONF_DIR/spark-defaults.conf"

---
DOnt connect to zscaler

To get to pyspark shell
docker exec -it glue_pyspark /bin/bash  (user it )  and then type pyspark in terminal




To get to pyspark shell docker exec -it glue_pyspark /bin/bash (user it ) and then type pyspark in terminal

spark = SparkSession .builder .appName("test") .getOrCreate() spark.sparkContext.setLogLevel("ERROR")

Set ERROR logging so you wont see any errors.
