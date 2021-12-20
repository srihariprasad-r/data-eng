from pyspark.sql import SparkSession, functions as F, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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
# add new column to dataframe using lit function
derv_df.withColumn('spark-user', F.lit(True)).show()
# not working, need to check below
derv_df.where(F.flatten(F.tranform(F.col('full_name'),'x->x.first')).equalTo('jennings')).show()

# Row lesson
myschema = StructType([
    StructField('col1', StringType(), True),
    StructField('col2', StringType(), False),
    StructField('col3', IntegerType(), False)
])
newRow = Row('Column','Value', 2)
newdf = spark.createDataFrame([newRow], myschema)

incrementRow = [Row('c1','v1', 3),
                Row('c2', 'v1', 4)]  # having col3 as None raises error as schema has nullable as False
incdf = spark.createDataFrame(incrementRow, myschema)
incdf.show()
# renaming the column using expr function
incdf.select(F.expr('col1 as column1')).show()
# simple expressions
incdf.selectExpr('*', "col1 != col2 as extended_column").show()
# aggregate function cannot be combined with any other column
incdf.selectExpr("avg(col3) as avg_col3", "count(col1) as column_cnt").show()

