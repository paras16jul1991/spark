package com.scala.test.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppDetailMaster {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\hadoop");
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL Example").config("spark.master", "local")
				.getOrCreate();

		Dataset<Row> df = spark.read()
				.option("mode", "DROPMALFORMED")
				.option("sep", ",")
				.option("header", true)
				.option("inferSchema", true).csv(args[0]);
		df.show();
		df.printSchema();
		Dataset<Row> filter = df.filter(df.col("Reviews").gt(25000));
		filter.show();
		
		filter.explain();
				
		filter.createOrReplaceTempView("application_detail");

		Dataset<Row> sqlResult = spark.sql("SELECT count(*)" + " FROM application_detail");
		sqlResult.explain();

		sqlResult.show();
	}
}
