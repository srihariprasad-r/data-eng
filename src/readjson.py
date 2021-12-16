from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

sparkSession = SparkSession.builder.appName("test").getOrCreate()

multiline_df = sparkSession.read.option("multiline","true") \
    .json("inputFiles/jsoninput.json")
multiline_df.show()

