
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap").setMaster("local[*]").set("spark.executor.memory", "6g")
sc = SparkContext(conf=conf)

word = sc.parallelize(["Du cHo tAn The vaN yeu Em Luon bEn eM duNg hONg aI giAnh lAy aNh khOng bOng anh khOng buonG"]) \
    .flatMap(lambda w: w.split(" ")) \
    .map(lambda e: e.lower())


stopWords = sc.parallelize(["luon ben em"]) \
    .flatMap(lambda w: w.split(" "))

finalWords = word.subtract(stopWords)
print(stopWords.collect())
print(word.collect())
print(finalWords.collect())