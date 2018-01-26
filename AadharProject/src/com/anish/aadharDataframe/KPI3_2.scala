package com.anish.aadharDataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.desc

object KPI3_2 {
  def main(args: Array[String]){
    
    //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
    
    //Find top 3 private agencies generating the most number of Aadhar cards?
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("KPI3_2")
				.master("local")
				.getOrCreate()
			
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz")
    
    data.registerTempTable("Tab1")
    // Sum of Aadhar generated group by Agency
    val result = spark.sql("select _c2, sum(_c9) aadhar_sum from Tab1 group by _c2")
    
    val fresult = result.orderBy(desc("aadhar_sum")).limit(3)
    
    fresult.rdd.saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp32outDF")
    spark.stop()
    
   }
}