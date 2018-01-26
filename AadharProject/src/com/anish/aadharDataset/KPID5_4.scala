package com.anish.aadharDataset

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.{desc,row_number,dense_rank,sum}
import org.apache.spark.sql.expressions.Window

object KPID5_4 {
  def main(args: Array[String]){
    
    //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
     
    // The top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
    // In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards
    // being rejected for males is the highest.
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("KPI5_4")
				.master("local")
				.getOrCreate()
		import spark.implicits._	
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz").as[AadharData]
    
   
    val res = data.where("_c7=\"F\"").groupBy("_c3").agg(sum("_c8").alias("a_sum")).orderBy(desc("a_sum")).limit(3)
    
    val res1 = data.join(res,"_c3").where("_c7=\"M\"").groupBy("_c3", "_c4").agg(sum("_c9").alias("r_sum"))
   
    
    val w = Window.partitionBy("_c3").orderBy(desc("r_sum"))
    
    val res2 = res1.withColumn("rank", row_number().over(w)).where("rank<=3")
    
    
    res2.rdd.coalesce(1, true)saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp54outDS")
    
    spark.stop()
    
   }
}