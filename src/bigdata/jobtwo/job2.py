from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import desc, col

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

	df = spark.read.csv("data/london.csv", header=False, schema=schema).cache()

	temp1 = df.select(["year", "neigh", "occ"]) \
				.groupby("year", "neigh") \
				.sum("occ") \
				#.withColumn("sum(occ)", col("sum(occ)") / 365) \
				.sort(desc("year"), desc("sum(occ)")) \
				.show()
