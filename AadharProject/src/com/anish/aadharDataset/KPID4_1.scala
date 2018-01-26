package com.anish.aadharDataset

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.{desc,sum}

object KPID4_1 {
  def main(args: Array[String]){
    
    //20150420,Allahabad Bank,A-Onerealtors Pvt Ltd,Delhi,South Delhi,Defence Colony,110025,F,49,1,0,0,1
    
    
    // Write a command to see the correlation between “age” and “mobile_number”?
    // For each Age Group find total mobile numbers given out of total Applicants
    
    System.setProperty("hadoop.home.dir", "/home/hduser/hadoop-2.5.0-cdh5.3.2")
		System.setProperty("spark.sql.warehouse.dir", "/home/hduser/spark-warehouse")
		
		val spark = SparkSession
				.builder
				.appName("KPI4_1")
				.master("local")
				.getOrCreate()
			import spark.implicits._
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz").as[AadharData]
    
    /*
    15 5 1 4 = 15 6 4
    15 5 0 4 = 15 5 4 = 15 11 8 = (8/11)*100
    
    18 5 0 5 = 18 5 5
    18 6 1 5 = 18 7 5 = 18 12 10 = (10/12)*100
    * 
    */
    
    val res1 = data.groupBy("_c8").agg(sum("_c9").alias("asum"),sum("_c10").alias("rsum"),sum("_c12").alias("tmob"))
        
    val res2 = res1.withColumn("t_enrol",res1.col("asum")+res1.col("rsum")).withColumn("t_mob", res1.col("tmob"))
    
    val res3 = res2.withColumn("has_mobile", (res2.col("t_mob")/res2.col("t_enrol")*100))
    
    val fresult = res3.select("_c8","has_mobile")
    
    fresult.rdd.coalesce(1,true)saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp41outDS")
    
    spark.stop()
    
   }
  
}