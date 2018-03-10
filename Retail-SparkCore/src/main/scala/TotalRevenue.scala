import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

object TotalRevenue {
  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().appName("Total_Revenue").master("yarn").getOrCreate()

    val inputpath = args(0)
    val outputpath = args(1)


    val fs = FileSystem.get(sc.sparkContext.hadoopConfiguration)

    if(args.length != 2){
      print("Usage Inputpath Outputpath")
      System.exit(1)
    }


    if(!fs.exists(new Path(inputpath))){
      print(inputpath+" doesnt exists")
      System.exit(1)
    }

    val op = new Path(outputpath)
    if(fs.exists(op)){
      fs.delete(op,true)
    }


    val orders_data = sc.read.textFile(inputpath + "/orders").rdd

    val order_items_data = sc.read.textFile(inputpath+"/order_items").rdd

    val orders_filtered = orders_data.filter(line => (
      line.split(",")(3).equalsIgnoreCase("complete") ||  line.split(",")(3).equalsIgnoreCase("closed")
      ))

    val order_maps = orders_filtered.map(line => (
      (line.split(",")(0).toInt, line.split(",")(1).toString)
      ))

    val order_item_maps = order_items_data.map(line => (
      (line.split(",")(1).toInt, line.split(",")(4).toFloat)
      ))

    val order_item_join = order_maps.join(order_item_maps)


    val oi_join_map = order_item_join.map(line => (
      line._2
      ))

    val res_gbk = oi_join_map.reduceByKey(_+_)

    res_gbk.map(line => line.productIterator.mkString("|")).saveAsTextFile(inputpath+"/"+outputpath)

    sc.stop()


  }
}
