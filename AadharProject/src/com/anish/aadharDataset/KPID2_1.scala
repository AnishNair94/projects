package com.anish.aadharDataset


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem

//case class AadharData(_c0: String,_c1: String, _c2: String, _c3: String, _c4: String, _c5: String, _c6: String,_c7: String,_c8: String,_c9: String,_c10: String,_c11: String,_c12: String)

object KPID2_1 {
  def main(args: Array[String]){
    
  //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
   //Find the count and names of registrars in the table.
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		val n = 25
		val spark = SparkSession
				.builder
				.appName("Registrar CountName")
				.master("local")
				.getOrCreate()
		
	  import spark.implicits._
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz").as[AadharData]
    
    // Grouping Based on Registrar 
    val result = data.groupBy("_c1").count()
    
    // Saving into 1 output file 
   result.rdd.coalesce(1, true).saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp21outDS")
    
    spark.stop()
    
   }
}