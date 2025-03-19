from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf).getOrCreate()

fileRdd = sc.textFile(r"C:\Users\PC\PycharmProjects\PythonProject2\rdd\Data\text.txt")

print(fileRdd.getNumPartitions())
print(fileRdd.glom().collect())
print(fileRdd.collect())