package org.shirdrn.spark.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GrouppingUserEventByArea {

  def main(args: Array[String]): Unit = {
    if(args.length <= 1) {
      println("org.shirdrn.spark.analytics.GrouppingUserEventByArea <hdfs_file> <output_file>")
      return
    }
    val file = args(0)
    val writeToFile = args(1)
    val conf = new SparkConf().setMaster("local[8]").setAppName("GrouppingUserEventByArea")
    val sc = new SparkContext(conf)
    
    val rdd = sc.textFile(file)
    rdd.flatMap(line => {
      val a = line.split("\t")
      val eventCode = a(1)
      val udid = a(2)
      val areaCode = a(34)
      Some((areaCode, eventCode),  udid)
    }).distinct()
    .map(triple => (triple._1, 1)).reduceByKey(_ + _).sortBy(x => x._2, false)
    .saveAsTextFile(writeToFile)
    
//    val conf = new SparkConf().setMaster("local[1]").setAppName("GrouppingUserEventByArea")
//    val sc = new SparkContext(conf)
//    val data = Array((("a", "b"), "u001"), (("a", "b"), "u001"), (("a", "b"), "u001"), (("a", "b"), "u002"))
//    val rdd = sc.parallelize(data, 2).distinct()
//    rdd.foreach(println(_))
    
    
  }
}