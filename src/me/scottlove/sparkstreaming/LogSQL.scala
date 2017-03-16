
package me.scottlove.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

/** Illustrates using SparkSQL with Spark Streaming, to issue queries on 
 *  Apache log data extracted from a stream on port 9999.
 */
object LogSQL {
  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LogSQL").setMaster("local[*]").set("spark.sql.warehouse.dir", "file:///C:/tmp")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    setupLogging()
    
    val pattern = apacheLogPattern()

    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[error]"
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })
 
    requests.foreachRDD((rdd, time) => {

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()

      requestsDataFrame.createOrReplaceTempView("requests")

      val wordCountsDataFrame =
        sqlContext.sql("select agent, count(*) as total from requests group by agent")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
      
      // If you want to dump data into an external database instead, check out the
      // org.apache.spark.sql.DataFrameWriter class! It can write dataframes via
      // jdbc and many other formats! You can use the "append" save mode to keep
      // adding data from each batch.
    })
    
    // Kick it off
    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

/** Case class for converting RDD to DataFrame */
case class Record(url: String, status: Int, agent: String)

/** Lazily instantiated singleton instance of SQLContext 
 *  (Straight from included examples in Spark)  */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}


