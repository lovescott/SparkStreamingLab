
package me.scottlove.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

/** Maintains top URL's visited over a 5 minute window, from a stream
 *  of Apache access logs on port 9999.
 */
object LogParser {
 
  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
    setupLogging()
    
    val pattern = apacheLogPattern()
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})

    val agents = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(9)})

    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})

    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    val agentNames  = agents.map {x => val arr = x.toString.split(" "); arr(1)}

    val agentCount = agents.map( x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    agentNames.print()

    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
//    sortedResults.print()
    
    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

