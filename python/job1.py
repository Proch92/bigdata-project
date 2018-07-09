from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import desc

# lista ordinata dei quartieri di londra per occorrenze di un determinato crimine negli ultimi 5 anni

if __name__ == '__main__':
	spark = SparkSession.builder.appName("py1").getOrCreate()

	schema = StructType([
			StructField("code", StringType()),
			StructField("neigh", StringType()),
			StructField("crime", StringType()),
			StructField("subcrime", StringType()),
			StructField("occ", IntegerType()),
			StructField("year", IntegerType()),
			StructField("month", IntegerType())
		])

	df = spark.read.csv("/user/proch92/data/london.csv", header=False, schema=schema).cache()

	results = df.filter(df.year >= 2013) \
				.filter(df.crime == "Robbery") \
				.select(["neigh", "crime", "occ"]) \
				.groupby("neigh") \
				.sum("occ") \
				.sort(desc("sum(occ)")) \
				.show()
