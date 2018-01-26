package com.anish.aadharDataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem

object KPI2_2 {
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
			
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz")
    
    data.registerTempTable("tab1")
    
    // Grouping based on state, district
    val query1 = "select _c3,_c4, count(*) from tab1 group by _c3,_c4"
    
    val result1 = spark.sql(query1)
    
    // grouping based on district , sub district
    val query2 = "select _c4,_c5,count(*) from tab1 group by _c4,_c5"
    
    val result2 = spark.sql(query2)
    
    // saving result in 1 partition
    result1.rdd.coalesce(1, true).saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp22_21outDF")
    
    result2.rdd.coalesce(1, true).saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp22_22outDF")
    
    spark.stop()
  }
}