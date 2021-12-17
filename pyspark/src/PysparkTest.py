from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.appName("test").getOrCreate()

sparkSession.createDataFrame([(1, "value1"), (2, "value2")], ["id", "value"]).show()