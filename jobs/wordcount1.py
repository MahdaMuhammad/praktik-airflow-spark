from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("wordcount") \
    .getOrCreate()

# Menentukan jalur lengkap ke file book.txt
file_path = "book.txt"

# Baca teks dari file
lines = spark.sparkContext.textFile(file_path)

# Hitung jumlah kemunculan setiap kata
word_counts = lines.flatMap(lambda line: line.split(" ")) \
                   .map(lambda word: (word, 1)) \
                   .reduceByKey(lambda x, y: x + y)

# Tampilkan hasil
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Stop SparkSession
spark.stop()
