package me.scottlove.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

/** Listens to a stream of tweets and saves them to disk. */
object SaveTweets {
  
  def main(args: Array[String]) {

    setupTwitter()
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(status => status.getText())
    
    //statuses.saveAsTextFiles("Tweets", "txt")
    
    var totalTweets:Long = 0
        
    statuses.foreachRDD((rdd, time) => {
      if (rdd.count() > 0) {
        val repartitionedRDD = rdd.repartition(1).cache()

        repartitionedRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString)

        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })
    
//    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
