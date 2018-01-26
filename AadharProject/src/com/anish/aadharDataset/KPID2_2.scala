package com.anish.aadharDataset

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem

object KPID2_2 {
  def main(args: Array[String]){
    
  //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
   //Find the number of states, districts in each state and sub-districts in each district.
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		val n = 25
		val spark = SparkSession
				.builder
				.appName("KPI2_2")
				.master("local")
				.getOrCreate()
			import spark.implicits._
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz").as[AadharData]
    
    // Grouping based on state, district
    val result1 = data.groupBy("_c3","_c4").count()
    
    // grouping based on district , sub district
    val result2 = data.groupBy("_c4","_c5").count()
    
    // saving result in 1 partition
    result1.rdd.coalesce(1, true).saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp22_21outDS")
    
    result2.rdd.coalesce(1, true).saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp22_22outDS")
    
    spark.stop()
  }
}