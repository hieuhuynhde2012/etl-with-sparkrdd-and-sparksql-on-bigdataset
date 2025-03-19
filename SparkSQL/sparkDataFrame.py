from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder \
    .appName("Spark DataFrame") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

data = [
    Row(
        name= "Tan Canh",
        age = 27,
        id = 1,
        salary = 2000.1,
        bonus = 200.0,
        is_activate=True,
        scores = [1, 9, 10],
        attributes = {"leader": "team 20", "role": "manager"},
        hire_date = datetime.strptime("2025-01-10", "%Y-%m-%d").date(),
        last_login = datetime.strptime("2025-03-16 22:30:15", "%Y-%m-%d %H:%M:%S"),
        tax_rate = 0.9
    ),
    Row(
        name="Tan Phong",
        age=25,
        id=2,
        salary=1000.2,
        bonus=100.0,
        is_activate=True,
        scores=[1, 8, 10],
        attributes={"leader": "team 10", "role": "full stack"},
        hire_date=datetime.strptime("2025-03-10", "%Y-%m-%d").date(),
        last_login=datetime.strptime("2025-03-16 23:30:15", "%Y-%m-%d %H:%M:%S"),
        tax_rate=0.9
    ),
    Row(
        name="Trung Tinh",
        age=27,
        id=3,
        salary=1500.3,
        bonus=200.1,
        is_activate=True,
        scores=[2, 9, 10],
        attributes={"leader": "team 5", "role": "Devop"},
        hire_date=datetime.strptime("2025-09-2", "%Y-%m-%d").date(),
        last_login=datetime.strptime("2025-03-10 22:30:15", "%Y-%m-%d %H:%M:%S"),
        tax_rate=0.9
    ),
]

schemaPeoPle= StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("salary", FloatType(), True),
    StructField("bonus", FloatType(), True),
    StructField("is_activate", BooleanType(), True),
    StructField("scores", ArrayType(IntegerType(), True), True),
    StructField("attributes", MapType(StringType(), StringType()), True),
    StructField("hire_date", DateType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("tax_rate", FloatType(), True),
])

df = spark.createDataFrame(data, schemaPeoPle)

df.show()
df.printSchema()