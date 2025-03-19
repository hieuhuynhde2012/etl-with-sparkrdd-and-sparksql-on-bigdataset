import random

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

spark =  SparkSession.builder \
    .appName("SparSql") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# rdd = spark.sparkContext.parallelize(range(1, 11)) \
#     .map(lambda x: (x, random.randint(0, 99) * x ))
# # print(rdd.collect())
# dataFrame = spark.createDataFrame(rdd, ["key", "value"])
# dataFrame.show()

hotTiktok = spark.sparkContext.parallelize([
    Row(1, "Le Tuan Khang", "men"),
    Row(2, "Son Tung Mtp", "men"),
    Row(3, "Amee", "women"),
    Row(4, "Phap Kieu", "LGBT"),
])

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("gender", StringType(), True),
])

df = spark.createDataFrame(hotTiktok, schema).show()