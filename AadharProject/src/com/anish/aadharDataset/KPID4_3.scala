package com.anish.aadharDataset

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.{desc,sum}

object KPID4_3 {
  def main(args: Array[String]){
    
    //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
    
    // Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("KPI4_3")
				.master("local")
				.getOrCreate()
		import spark.implicits._	
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz").as[AadharData]
    
    val res1 = data.where("_c3 = \"Uttar Pradesh\" OR _c3 =\"Maharashtra\"")
    
    val res2 = res1.groupBy("_c3").agg(sum("_c10").alias("rsum")).select("_c3","rsum")
    
    
    
    res2.rdd.coalesce(1,true)saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp43outDS")
    
    spark.stop()
    
   }
}