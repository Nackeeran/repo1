from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

data = ["hello world", "hello spark", "hello Jenkins"]
rdd = spark.sparkContext.parallelize(data)
words = rdd.flatMap(lambda line: line.split(" "))
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
for word, count in word_counts.collect():
    print(f"{word}: {count}")

spark.stop()
