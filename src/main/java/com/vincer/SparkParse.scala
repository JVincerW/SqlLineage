package com.vincer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
	* @ClassPath com.vincer.SparkParse
	* @Description TODO
	* @Date 2021/11/4 11:01
	* @Created by Vincer
	* */
object SparkParse {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
			.appName("OrdersRealtimeApp")
			.config("spark.sql.shuffle.partitions", 10)
			.config("spark.debug.maxToStringFields", "10000")
			.master("local[*]")
			.getOrCreate()
		println(spark.sessionState.sqlParser.parsePlan("select * from test").inputSet)
		spark.stop()
	}
}
