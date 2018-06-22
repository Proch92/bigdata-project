from pyspark.sql import SparkSession
from pyspark.sql.typer import StructType, StructField, IntegerType, StringType

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

	df = spark.read.csv("data/london.csv", header=False, schema=schema).cache()

	print df.columns
