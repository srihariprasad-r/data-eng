from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from urllib.request import urlopen

spark = SparkSession.builder.appName("test").getOrCreate()

# json file as input
multiline_df = spark.read.option("multiline","true") \
    .json("inputFiles/jsoninput.json")
# multiline_df.show()
# spark.read.json("inputFiles/jsoninput.json").explain()

# online json processing
urlJsonData = 'https://randomuser.me/api/0.8/?results=10'
httpData = urlopen(urlJsonData).read().decode('utf-8')
rdd = spark.sparkContext.parallelize([httpData])
df = spark.read.json(rdd)
# explode array and extract nested columns
derv_df = df.withColumn('expr_result', F.explode(df["results"])).select(F.col("expr_result.user.cell").alias('mobile phone'),
                                                              F.col("expr_result.user.location.city").alias('city'),
                                                              F.struct(F.col("expr_result.user.name.first"),
                                                                       F.col("expr_result.user.name.last")).alias('full_name'))
# create struct complex column and extract individual columns
derv_df.select(F.col('full_name').getField('last')).show()