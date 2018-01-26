package com.anish.aadharDataset

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem

object KPID2_4 {
  def main(args: Array[String]){
    
  //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
   //Find out the names of private agencies for each state
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		val n = 25
		val spark = SparkSession
				.builder
				.appName("KPI2_4")
				.master("local")
				.getOrCreate()
	  
	 import spark.implicits._
	  // Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz").as[AadharData]
    
    
    val fresult = data.select("_c2", "_c3").groupBy("_c3","_c2").count().orderBy("_c3")
    fresult.rdd.coalesce(1, true).saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp24outDS")
    
    spark.stop()
   }
}