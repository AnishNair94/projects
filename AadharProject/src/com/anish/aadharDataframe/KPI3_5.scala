package com.anish.aadharDataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.desc

object KPI3_5 {
  def main(args: Array[String]){
    
    //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
    
    //Find the no. of Aadhaar cards generated in each state?
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		val n = 25
		val spark = SparkSession
				.builder
				.appName("KPI3_5")
				.master("local")
				.getOrCreate()
			
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz")
    
    data.registerTempTable("Tab1")
        
    // sum of aadhar generated group by state
    val result = spark.sql("select _c3, sum(_c9) aadhar_sum from Tab1 group by _c3")
    
    val fresult = result.orderBy("_c3")
    
    fresult.rdd.coalesce(1,true)saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp35outDF")
    
    spark.stop()
    
   }
}