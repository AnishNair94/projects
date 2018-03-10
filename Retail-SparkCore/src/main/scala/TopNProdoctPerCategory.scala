import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc,dense_rank}


object TopNProdoctPerCategory {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("Top N Product Per Department").master("yarn").getOrCreate()

    val inputpath = args(0)
    val outputpath = args(1)

    val N = args(2).toInt

    val fs = FileSystem.get(sc.sparkContext.hadoopConfiguration)

    if(args.length != 3){
      print("Usage Inputpath Outputpath N")
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

    val product_data = sc.read.textFile(inputpath+"/products").rdd

    val category_data = sc.read.textFile(inputpath+"/categories").rdd

    val product_filter = product_data.filter(line=>{
      (!(line.split(",")(1).isEmpty) && !(line.split(",")(4).isEmpty))
    })

    val cat_filter = category_data.filter(line=>{
      !(line.split(",")(0).isEmpty)
    })

    val product_map = product_filter.map(line=> {
      (line.split(",")(1).toInt, (line.split(",")(2),line.split(",")(4).toFloat))
    })


    val cat_map = cat_filter.map(line=> {
      (line.split(",")(0).toInt, line.split(",")(2))
    })

    val pc_join = product_map.join(cat_map)

    val pcj_map = pc_join.map(line=>{
      (line._2._2 , line._2._1)
    })

    val pcj_toData = pcj_map.map(line=>{
      (line._1,line._2._1,line._2._2)})

    import  sc.implicits._


    val ds = pcj_toData.toDS()

    val w = Window.partitionBy("_1").orderBy(desc("_3"))


    val topn = ds.withColumn("rank",dense_rank.over(w)).where("rank<="+N).drop("rank").rdd.saveAsTextFile(inputpath+"/"+outputpath)

    sc.stop()
  }
}
