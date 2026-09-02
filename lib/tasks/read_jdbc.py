from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
glueContext = GlueContext(sc)
sqlContext = SQLContext(sc)

# JDBC connection details based on the provided information
jdbc_url = "jdbc:oracle:thin:@dbsgmedev.aws.baxter.com:1521/GMEDEV"
connection_properties = {
    "user": "GME_DEV_MFG_PL_RO",
    "password": "Adoqetu273$",
    "driver": "oracle.jdbc.driver.OracleDriver"  # JDBC driver for Oracle
}

# Example query to test the connection and read data
dbtable = "MUTDTA.F0911"  # Replace with the actual table name you wish to query

df = sqlContext.read.jdbc(url=jdbc_url, table=dbtable, properties=connection_properties)

# Show the first few rows of the DataFrame to verify the connection and data retrieval
df.show()

# Remember to stop the Spark context when done
sc.stop()
