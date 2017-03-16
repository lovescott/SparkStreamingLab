
package me.scottlove.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import java.util.concurrent._
import java.util.concurrent.atomic._

/** Monitors a stream of Apache access logs on port 9999, and prints an alarm
 *  if an excessive ratio of errors is encountered.
 */
object LogAlarmer {
  
  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "LogAlarmer", Seconds(1))
    
    setupLogging()
    val pattern = apacheLogPattern()

    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    val statuses = lines.map(x => {
          val matcher:Matcher = pattern.matcher(x); 
          if (matcher.matches()) matcher.group(6) else "[error]"
        }
    )
    
    val successFailure = statuses.map(mapFunc = x => {
      val statusCode = util.Try(x.toInt) getOrElse 0

      statusCode match {
        case x if 200 to 299 contains x => "Success"
        case x if 500 to 599 contains x => "Failure"
        case _ => "Other"
      }
    })
    
    val statusCounts = successFailure.countByValueAndWindow(Seconds(300), Seconds(1))
    
    statusCounts.foreachRDD((rdd, time) => {
      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect()
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {
            totalSuccess += count
          }
          if (result == "Failure") {
            totalError += count
          }
        }
      }

      println("Total success: " + totalSuccess + " Total failure: " + totalError)
      
      if (totalError + totalSuccess > 100) {
        val ratio:Double = util.Try( totalError.toDouble / totalSuccess.toDouble ) getOrElse 1.0
        if (ratio > 0.5) {
          println("Wake somebody up! Something is horribly wrong.")
        } else {
          println("All systems go.")
        }
      }
    })
    

    // Kick it off
    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

