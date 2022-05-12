from pyspark.sql import SparkSession

from delta import *

spark = (SparkSession.builder
         .getOrCreate())

df = spark.range(10)

df.show()

spark.sql("CREATE dbx_test_table(id INT) USING DELTA")