import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

#spark = SparkSession.builder \
#    .appName("helloworld") \
#    .getOrCreate()

#get the second argument passed to spark-submit (the first is the python app)
#logFile = sys.argv[1]

#read file
#logData = spark.sparkContext.textFile(logFile).cache()

#numAs = logData.filter(lambda s: 'a' in s).count()

#numBs = logData.filter(lambda s: 'b' in s).count()

#print("Lines with a: {}, lines with b: {}".format(numAs, numBs))

print("Hello World")
