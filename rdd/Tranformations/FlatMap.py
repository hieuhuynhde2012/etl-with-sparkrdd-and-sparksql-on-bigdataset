from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

rdd = sc.textFile(r"C:\Users\PC\PycharmProjects\PythonProject2\rdd\Data\text.txt")

wordRdd = rdd.flatMap(lambda line: line.split(" "))
print(wordRdd.collect())

for line in wordRdd.collect():
    print(line)