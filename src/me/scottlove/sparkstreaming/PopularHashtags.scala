package me.scottlove.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object PopularHashtags {
  
  def main(args: Array[String]) {

    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)
    
    val statuses = tweets.map { status => status.getText }

    val tweetwords = statuses.flatMap { tweetText => tweetText.split(" ") }
    
    val hashtags = tweetwords.filter { word => word.startsWith("#") }
    
    val hashtagKeyValues = hashtags.map {hashtag => (hashtag, 1) }

    val wordKeyValues = tweetwords.map { word => (word, 1) }
    
    val hashtagCounts = hashtagKeyValues reduceByKeyAndWindow( _ + _, _ -_, Seconds(1000), Seconds(1))

    val wordCounts = wordKeyValues reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    val sortedResults = wordCounts transform(rdd => rdd.sortBy(x => x._2, false))
    
    sortedResults.print
    
    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
