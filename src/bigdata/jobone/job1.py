from pyspark.sql import SparkSession

if __name__ == '__main__':
	spark = SparkSession.builder.appName("py1").getOrCreate()

	lines = spark.read.csv("hdfs:///home/proch92@gmail_com/data/london.csv", header=False)

	print lines.count()