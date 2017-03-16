package me.scottlove.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans
import Utilities._

/** Example of using streaming K-Means clustering to cluster people by income and age into
 *  5 clusters.
 */
object StreamingKMeans {
  
  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "StreamingKMeans", Seconds(1))
    
    setupLogging()
    
    val trainingLines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    val testingLines = ssc.socketTextStream("127.0.0.1", 7777, StorageLevel.MEMORY_AND_DISK_SER)
    
    val trainingData = trainingLines.map(Vectors.parse).cache()
    val testData = testingLines.map(LabeledPoint.parse)
    
    trainingData.print()
    
    val model = new StreamingKMeans()
      .setK(5)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)
    
    model.trainOn(trainingData)
    
    model.predictOnValues(testData.map(lp => (lp.label.toInt, lp.features))).print()
    
    // Kick it off
    ssc.checkpoint("checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
  
}