from pyspark.sql import SparkSession

if __name__ == '__main__':
	spark = SparkSession.builder.appName("py1").getOrCreate()

	lines = spark.read.csv("data/london.csv", header=False).cache()
	output = lines.map()

	print lines.count()
