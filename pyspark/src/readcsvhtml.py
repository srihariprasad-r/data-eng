from pyspark.sql import SparkSession
from pyspark import SparkFiles
csvurl = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv"
csvspark = SparkSession.builder.appName("testcsv").getOrCreate()

csvspark.sparkContext.addFile(csvurl)

csv_raw_df = csvspark.read \
    .csv("file:///" + SparkFiles.get("online-retail-dataset.csv"), header=True, inferSchema=True).show()