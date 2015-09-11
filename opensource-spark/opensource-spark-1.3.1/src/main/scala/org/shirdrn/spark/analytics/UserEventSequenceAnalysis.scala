package org.shirdrn.spark.analytics

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

/**
 * Compute user operation sequences, including event order, page order, and linger times.<br>
 *
 * Input HDFS files:
 * <pre>
 *   [spark@hadoop6 ~]$ hdfs dfs -ls /test/shiyj/basis_event/
 *   Found 4 items
 *   -rw-r--r--   2 spark supergroup  496126927 2015-09-11 11:51 /test/shiyj/basis_event/2015-09-07.txt
 *   -rw-r--r--   2 spark supergroup  498087644 2015-09-09 18:42 /test/shiyj/basis_event/2015-09-08.txt
 *   -rw-r--r--   2 spark supergroup  225197783 2015-09-11 11:26 /test/shiyj/basis_event/2015-09-09.txt
 *   -rw-r--r--   2 spark supergroup  168884237 2015-09-11 11:28 /test/shiyj/basis_event/2015-09-10.txt
 * </pre>
 *
 * Input file line content contains 11 fields:
 * <table border="1">
 *   <tr>
 *     <td>01</td><td>02</td><td>03</td><td>04</td><td>05</td><td>06</td><td>07</td><td>08</td><td>09</td><td>10</td><td>11</td>
 *   </tr>
 *   <tr>
 *     <td>id</td><td>event_code</td><td>user_id</td><td>device_type</td><td>version</td><td>sessionid</td><td>album_id</td><td>page_no</td><td>event_time</td><td>action_index</td><td>server_time</td>
 *   </tr>
 * </table>
 * Submit spark application:
 * <p>
 *   bin/spark-submit --master spark://hadoop1:7077 --class org.shirdrn.spark.analytics.UserEventSequenceAnalysis ~/opensource-spark-1.3.1_2.10-1.0.jar /test/shiyj/basis_event/`*`.txt /test/shiyj/spark/results
 * </p>
 *
 * Output HDFS files:
 * <pre>
 *   [spark@hadoop6 ~]$ hdfs dfs -ls /test/shiyj/spark/results/
 *   Found 11 items
 *   -rw-r--r--   2 spark supergroup          0 2015-09-11 12:05 /test/shiyj/spark/results/_SUCCESS
 *   -rw-r--r--   2 spark supergroup   23791686 2015-09-11 12:05 /test/shiyj/spark/results/part-00000
 *   -rw-r--r--   2 spark supergroup   23942923 2015-09-11 12:05 /test/shiyj/spark/results/part-00001
 *   -rw-r--r--   2 spark supergroup   23199570 2015-09-11 12:05 /test/shiyj/spark/results/part-00002
 *   -rw-r--r--   2 spark supergroup   22477013 2015-09-11 12:05 /test/shiyj/spark/results/part-00003
 *   -rw-r--r--   2 spark supergroup   22676061 2015-09-11 12:05 /test/shiyj/spark/results/part-00004
 *   -rw-r--r--   2 spark supergroup   22397693 2015-09-11 12:05 /test/shiyj/spark/results/part-00005
 *   -rw-r--r--   2 spark supergroup   23197003 2015-09-11 12:05 /test/shiyj/spark/results/part-00006
 *   -rw-r--r--   2 spark supergroup   23077222 2015-09-11 12:05 /test/shiyj/spark/results/part-00007
 *   -rw-r--r--   2 spark supergroup   22828026 2015-09-11 12:05 /test/shiyj/spark/results/part-00008
 *   -rw-r--r--   2 spark supergroup   23161814 2015-09-11 12:05 /test/shiyj/spark/results/part-00009
 * </pre>
 *
 * Output file content sample:
 * <pre>
 *   [spark@hadoop6 ~]$ hdfs dfs -text /test/shiyj/spark/results/part-00000 | head -10
 *   236e7950137225759ce5b390654b9702	101011	101011	1441630855	985	0
 *   29b855cab4d7e48f04fe901eb7c33568	101011	101011	1441630848	12	0
 *   f54cc3d57666805d8dbebf6b56b6f960	300021	200002	1441604934	1200	0
 *   f54cc3d57666805d8dbebf6b56b6f960	300021	200002	1441614034	1227	9100
 *   f54cc3d57666805d8dbebf6b56b6f960	300021	200002	1441614104	1228	70
 *   f54cc3d57666805d8dbebf6b56b6f960	300021	200002	1441614116	1229	12
 *   f54cc3d57666805d8dbebf6b56b6f960	300021	200002	1441616303	1233	2187
 *   f54cc3d57666805d8dbebf6b56b6f960	101011	101011	1441630858	1265	14555
 *   f54cc3d57666805d8dbebf6b56b6f960	300021	200002	1441692414	1351	61556
 *   f54cc3d57666805d8dbebf6b56b6f960	300021	200002	1441704866	1374	12452
 * </pre>
 */
object UserEventSequenceAnalysis {

  def main (args: Array[String]) {
    if(args.length != 2) {
      sys.exit(-1)
    }

    val input = new Path(args(0))
    val output = args(1)

    val sc = new SparkContext()
    val fs = input.getFileSystem(sc.hadoopConfiguration)

    // Union all files which are matched the given file path glob
    val rdd = fs.globStatus(input)
      .map(file => sc.textFile(file.getPath.toString))
      .reduce(_ ++ _)
    fs.close

    // Core computation logic
    val resultRDD = rdd.map { record =>
      val values = record.split("\t").toSeq
      // (user_id, (event_code, page_no, event_time, action_index))
      (values(2), (values(1), values(7).toLong, values(8).toLong, values(9).toLong))
    }.groupByKey(10).map { pair =>
      val userId = pair._1
      val values = pair._2.toList.sortWith(_._3 < _._3)
      var previous = values(0)
      var firstProcessed = false
      values.map { current =>
        if(previous == current && !firstProcessed) {
          firstProcessed = true
          Seq(userId, current._1, current._2, current._3, current._4, 0).mkString("\t")
        } else {
          val old = previous
          previous = current
          Seq(userId, current._1, current._2, current._3, current._4, current._3 - old._3).mkString("\t")
        }
      }
    }.flatMap(_.toSeq)

    // save final computation result
    resultRDD.saveAsTextFile(output)
  }
}
