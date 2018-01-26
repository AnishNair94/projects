package com.anish.aadharDataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.{desc,row_number,dense_rank}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object KPI5_5 {
  def main(args: Array[String]){
    
    //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
    
   // The summary of the acceptance percentage of all the Aadhaar cards applications by bucketing
  //  the age group into 10 buckets.
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		
		val interval  = 20
		
		val spark = SparkSession
				.builder
				.appName("KPI5_5")
				.master("local")
				.getOrCreate()
			
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz")
    
    val df1 = data.withColumn("range",col("_c8") - (col("_c8") % interval))
                  .withColumn("range", concat(col("range"), lit(" - "),col("range") + interval))
    
    df1.registerTempTable("Tab1")
    
    val res1 = spark.sql("select range , sum(_c9+_c10) total_enrol, sum(_c9) total_gen from Tab1 group by range")
    
    res1.registerTempTable("Tab2")
    
    val res2 = spark.sql("select range, ((total_gen/total_enrol)*100) Acceptance_percentage from Tab2")
    
    res2.rdd.coalesce(1, true)saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp55outDF")
    
    spark.stop()
    
   }
}