from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import desc, col, rank
from pyspark.sql.window import Window


if __name__ == '__main__':
	spark = SparkSession.builder.appName("py2").getOrCreate()

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

	window = Window.partitionBy("year").orderBy(desc("sum(occ)"))

	# select su anno, quartiere e occorrenze
	# aggregazione primaria sull'anno e secondaria sul quartiere
	# utilizzo la window per rankare i quartieri in base alle occorrenze
	# filtro solo i quartieri con rank 1,2,3 (3 per ogni anno)
	# select per scartare la colonna rank
	# divisione column-wise per calcolare la media annua

	temp1 = df.select(["year", "neigh", "occ"]) \
				.groupby("year", "neigh") \
				.sum("occ") \
				.withColumn("rank", rank().over(window)) \
				.filter("rank <= 3") \
				.sort("year", "rank") \
				.select(["year", "neigh", "sum(occ)"]) \
				.withColumn("sum(occ)", col("sum(occ)") / 365) \
				.write.csv("/user/proch92/outpy2")