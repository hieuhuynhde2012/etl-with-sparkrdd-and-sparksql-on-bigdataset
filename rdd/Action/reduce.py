from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("reduce")

sc = SparkContext(conf=conf)

numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

def sum(v1: int, v2: int) -> int:
    print(f"v1: {v1}, v2: {v2} => ({v1 + v2})")
    return v1 + v2

print(numbers.getNumPartitions())
print(numbers.reduce(sum))