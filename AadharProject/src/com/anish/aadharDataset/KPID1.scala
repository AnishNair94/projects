package com.anish.aadharDataset

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.{desc,row_number,rank}
import org.apache.spark.sql.expressions.Window

case class AadharData(_c0: String,_c1: String, _c2: String, _c3: String, _c4: String, _c5: String, _c6: String,_c7: String,_c8: String,_c9: String,_c10: String,_c11: String,_c12: String)

object KPID1 {
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
			
	 import spark.implicits._
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz").as[AadharData]
    
    // Windows are partitioned by Store
    // Each window has each store ordered by aadhar generated in descending order
    val w = Window.partitionBy("_c2").orderBy(desc("_c8"))

    // In a window, provide rank to each row using row_number
    // limit row_number <= 25 
   
    val dfTop = data.withColumn("rank", row_number.over(w)).where("rank<=25").drop("rank") 
   
    
    // save output
    dfTop.rdd.saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp1outDS")
    
    spark.stop()
  }
  
}