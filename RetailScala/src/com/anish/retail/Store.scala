package com.anish.retail
import org.apache.spark.sql.SparkSession

object Store {
  def main(args: Array[String]){
    //2012-01-01	09:00	San Jose	Men's Clothing	214.05	Amex
     if(args.length < 2){
      System.err.println("Usage: Retail Store Sales <Input-File> <Output-File>");
      System.exit(1);
    }
   
    val sp = SparkSession.builder().appName("Store Sales").getOrCreate()
    
    val data  = sp.read.textFile(args(0)).rdd
    
    val mapresult = data.map{
      line =>
      val tokens = line.split("\t")
      (tokens(2),tokens(4).toFloat)
    }
   
    val result = mapresult.reduceByKey(_+_)
    
    
    result.saveAsTextFile(args(1))
     
    sp.stop
  }
}