

package   me.scottlove.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
 
  def main(args: Array[String]) {

    setupTwitter()

    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)
    
    val statuses = tweets.map(status => status.getText())
    
    // Print out the first ten
    statuses.print()
    
    ssc.start()
    ssc.awaitTermination()
  }  
}