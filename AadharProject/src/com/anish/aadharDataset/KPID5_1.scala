package com.anish.aadharDataset

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.{desc,sum}

object KPID5_1 {
  def main(args: Array[String]){
    
    //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
    
    // The top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("KPI5_1")
				.master("local")
				.getOrCreate()
		import spark.implicits._	
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz").as[AadharData]
    
    val res1 = data.where("_c7=\"M\"")
    
    //val res = spark.sql("select _c3,sum(_c8) a_sum from Tab1 where _c7='M' group by _c3")
    val res2 = res1.groupBy("_c3").agg(sum("_c8").alias("asum")).orderBy(desc("asum")).limit(3).select("_c3")
    
    
    res2.rdd.coalesce(1, true)saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp51outDS")
    
    spark.stop()
    
   }
}