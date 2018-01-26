package com.anish.aadharDataframe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.sql.functions.{desc,row_number,dense_rank}
import org.apache.spark.sql.expressions.Window

object KPI5_4 {
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
			
				// Reading CSV data
		val options= Map("sep" -> ",")
    val data   = spark.read.options(options).csv("/home/hduser/eclipse-workspace/aadhaar_data.csv.gz")
    
    data.registerTempTable("Tab1")

    val res = spark.sql("select _c3,sum(_c8) a_sum from Tab1 where _c7='F' group by _c3")
        
    val res2 = res.orderBy(desc("a_sum")).limit(3).createTempView("Tab2")
    
    val res3 = spark.sql("select t1._c3 state ,t1._c4 district, sum(t1._c9) r_sum from Tab1 t1, Tab2 t2 where t1._c3 = t2._c3 and t1._c7='M' group by t1._c3,t1._c4")
    res3.createTempView("tab3")
    
    val query = "select state,district from (SELECT state,district,r_sum,dense_rank() OVER (PARTITION BY state ORDER BY r_sum DESC) as rank FROM tab3) tmp where rank<=3"
    
    val res4 = spark.sql(query)
    
    res4.rdd.coalesce(1, true)saveAsTextFile("/home/hduser/eclipse-workspace/AadharProject/kp54outDF")
    
    spark.stop()
    
   }
}