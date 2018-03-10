import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object DailyRevPerDepartment {
  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().appName("Dialy_Revenue").master("yarn").getOrCreate()

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

    val product_data = sc.read.textFile(inputpath+"/products").rdd

    val category_data = sc.read.textFile(inputpath+"/categories").rdd

    val department_data = sc.read.textFile(inputpath+"/departments").rdd



    val orders_f = orders_data.filter(line => (
      line.split(",")(3).equalsIgnoreCase("complete")
      ))

    // Orders  - Order_id, Order_Date
    val order_map = orders_f.map(line => (
      (line.split(",")(0).toInt, line.split(",")(1).toString)
      ))
    val order_i_map = order_items_data.map(line => {
       (line.split(",")(1).toInt, (line.split(",")(2).toInt,line.split(",")(4).toFloat))})

    val order_item_j = order_map.join(order_i_map)

    // order_id ,(day, (product_id, revenue))
    //(Int, (String, (Int, Float))) = (65722,(2014-05-23 00:00:00.0,(365,119.98)))
    val product_map = product_data.map(line => {
       (line.split(",")(0).toInt, line.split(",")(1).toInt)})

    val oi_j_map = order_item_j.map(line =>{
       (line._2._2._1.toInt, (line._2._1, line._2._2._2.toFloat))})

//    (Int, (String, Float)) = (365,(2014-05-23 00:00:00.0,119.98))

    val p_oi_j = product_map.join(oi_j_map)

      // product_id, (category_id, (day, revenue))
   // (Int, (Int, (String, Float))) = (226,(11,(2013-08-16 00:00:00.0,599.99)))

    val poij_map = p_oi_j.map(line =>{
      (line._2._1.toInt, (line._2._2._1, line._2._2._2.toFloat))})
   // (Int, (String, Float)) = (11,(2013-08-16 00:00:00.0,599.99))

    val cat_map = category_data.map(line =>{
      (line.split(",")(0).toInt, line.split(",")(1).toInt)})

    val poij_cat_j = cat_map.join(poij_map)
    // category_id, (dept_id,(day, revenue))
   // (Int, (Int, (String, Float))) = (13,(3,(2013-09-22 00:00:00.0,127.96)))

    val poijcat_map = poij_cat_j.map(line => {
      (line._2._1.toInt, (line._2._2._1, line._2._2._2.toFloat))})
   // (Int, (String, Float)) = (3,(2013-09-22 00:00:00.0,127.96))

    val dept_map = department_data.map(line => {
      (line.split(",")(0).toInt, line.split(",")(1))})

    val poijcat_dept_j = poijcat_map.join(dept_map)
    // dept_id, ((day, revenue), dept_name)
   // (Int, ((String, Float), String)) = (4,((2014-05-23 00:00:00.0,119.98),Apparel))

    val pcd_map = poijcat_dept_j.map(line=> {
      ((line._2._2, line._2._1._1), line._2._1._2.toFloat)})
  //  ((String, String), Float) = ((Apparel,2014-05-23 00:00:00.0),119.98)

    val pcd_gbk = pcd_map.reduceByKey(_+_)
    pcd_gbk.map(line =>
      (line._1._1, (line._1._2+" | "+line._2)))
      .sortByKey()
      .map(line=>
        line._1+" | "+line._2)
        .saveAsTextFile(inputpath+"/"+outputpath)

    sc.stop()


  }
}
