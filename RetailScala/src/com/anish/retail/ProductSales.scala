package com.anish.retail

import org.apache.spark.sql.SparkSession

object ProductSales extends{
  //2012-01-01	09:00	San Jose	Men's Clothing	214.05	Amex
  def main(args: Array[String]){
    if(args.length < 2){
      System.err.println("Usage: Retail Product Sales <Input-File> <Output-File>");
      System.exit(1);
    }
   
    val sp = SparkSession.builder().appName("Product Sales").getOrCreate()
    
    val data  = sp.read.textFile(args(0)).rdd
    
    val mapresult = data.map{
      line =>
      val tokens = line.split("\t")
      (tokens(3),tokens(4).toFloat)
    }
   
    val result = mapresult.reduceByKey(_+_)
    
    
    result.saveAsTextFile(args(1))
     
    sp.stop
    }
  }
