from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import expr, col
from pyspark import SparkFiles
csvurl = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv"
csvspark = SparkSession.builder.appName("testcsv").getOrCreate()

csvspark.sparkContext.addFile(csvurl)

csv_raw_df = csvspark.read \
    .csv("file:///" + SparkFiles.get("online-retail-dataset.csv"), header=True, inferSchema=True)

csv_raw_df.show()

hasMetal = F.instr(F.col('Description'), 'METAL') > 0
hasCupid = F.instr(F.col('Description'), 'CUPID') > 0
# filter only rows which does not have METAL/CUPID in description
csv_raw_df.withColumn('hasMetalCupid', hasMetal | hasCupid) \
                        .where('hasMetalCupid==False') \
                        .select('StockCode', 'hasMetalCupid').show()
# filter only rows which have METAL/CUPID in description
csv_raw_df.withColumn('hasMetalCupid', hasMetal | hasCupid) \
    .where('hasMetalCupid') \
    .select('StockCode', 'hasMetalCupid').show()
# filter DOT stocks and include rows with UnitPrice < 500 and < 650
only_dot_stocks_df = csv_raw_df.where(F.col('StockCode').isin("DOT"))
unitpriceLess500 = F.col('UnitPrice') > 500
unitpriceLess650 = F.col('UnitPrice') < 650
only_dot_stocks_df.where(unitpriceLess500 & unitpriceLess650) \
    .select('InvoiceNo', 'StockCode', 'UnitPrice') \
    .show()
# same check using expr function
only_dot_stocks_df.where(F.expr('UnitPrice > 500') & F.expr('UnitPrice < 650')) \
    .select('InvoiceNo', 'StockCode', 'UnitPrice') \
    .show()
# conditions in expr is passed as raw string
only_dot_stocks_df.where(expr('InvoiceNo != 536365') & expr('UnitPrice > 500')).show()
# conditions in col function should be prepped before(if its more than one)
condtn_invoice = col('InvoiceNo') != 536365
condtn_unitprice = col('UnitPrice') > 500
only_dot_stocks_df.where( condtn_invoice & condtn_unitprice ).show()