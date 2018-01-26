package com.anish.aadharDataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.desc

object KPI3_4 {
  def main(args: Array[String]){
    
    //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
    
    //Find top 3 districts where enrolment numbers are maximum?
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		val n = 25
		val spark = SparkSession
				.builder
				.appName("KPI3_4")
				.master("local")
				.getOrCreate()
			
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz")
    
    data.registerTempTable("Tab1")
     
    // Sum of Aadhar generated and Aadhar rejected group by districts
    val result = spark.sql("select _c4, sum(_c9+_c10) aadhar_sum from Tab1 group by _c4")
    
    val fresult = result.orderBy(desc("aadhar_sum")).limit(3)
    
    fresult.rdd.saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp34outDF")
    
    spark.stop()
    
   }
}