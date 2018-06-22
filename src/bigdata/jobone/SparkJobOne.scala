package bigdata.jobone

import org.apache.spark.sql.SparkSession

object SparkJobOne {
	def main(args: Array[String]) {
		val spark = SparkSession
			.builder
			.appName("SparkJobOne")
			.getOrCreate()

		val lines = spark.read.format("csv").option("header", "false").load("hdfs:///home/proch92_gmail_com/data/london.csv")

		spark.stop()
	}
}