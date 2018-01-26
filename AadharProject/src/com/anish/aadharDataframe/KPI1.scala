package com.anish.aadharDataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.{desc,row_number,rank}
import org.apache.spark.sql.expressions.Window


object KPI1 {
  def main(args: Array[String]){
    
  //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
   //View/result of the top 25 rows from each individual store
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		val n = 25
		val spark = SparkSession
				.builder
				.appName("top25indvstore")
				.master("local")
				.getOrCreate()
			
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz")
    
    data.registerTempTable("tab1")
    
    val query = "SELECT _c0,_c1,_c2,_c3,_c4,_c5,_c6,_c7,_c8,_c9,_c10,_c11,_c12 FROM (SELECT _c0,_c1,_c2,_c3,_c4,_c5,_c6,_c7,_c8,_c9,_c10,_c11,_c12,row_number() OVER (PARTITION BY _c2 ORDER BY _c8 DESC) as rank FROM tab1) tmp WHERE rank <= 25"
    
    val dfTop =spark.sql(query)
    
    
    // save output
     dfTop.rdd.saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp1outDF")
    
    spark.stop()
  }
  
}