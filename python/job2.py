from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import desc, col, rank
from pyspark.sql.window import Window

# per ogni anno determinare i 3 quartieri con la media di crimini al giorno più alta

if __name__ == '__main__':
	spark = SparkSession.builder.appName("py1").getOrCreate()

	schema = StructType([
			StructField("code", StringType()),
			StructField("neigh", StringType()),
			StructField("crime", StringType()),
			StructField("subcrime", StringType()),
			StructField("occ", DoubleType()),
			StructField("year", IntegerType()),
			StructField("month", IntegerType())
		])

	df = spark.read.csv("/user/proch92/data/london.csv", header=False, schema=schema).cache()

	window = Window.partitionBy("year").orderBy("sum(occ)")

	temp1 = df.select(["year", "neigh", "occ"]) \
				.groupby("year", "neigh") \
				.sum("occ") \
				.withColumn("rank", rank().over(window)) \
				.filter("rank <= 3") \
				.show()