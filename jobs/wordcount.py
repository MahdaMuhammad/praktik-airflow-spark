from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

text = "Hello Spark Hello Airflow Hello Python Hello Docker And Hello Mahda"

words = spark.sparkContext.parallelize(text.split(" "))

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

for wc in wordCounts.collect():
    print(wc[0], wc[1])

spark.stop()
