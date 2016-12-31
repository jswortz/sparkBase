package org.apache.spark.examples.streaming

import scala.collection.mutable.Queue

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QueueStream extends App{

    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))


    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[String]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.flatMap(x => (x.split(" "))).map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()
    ssc.start()

    // Create and push some RDDs into rddQueue
    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.textFile("/users/Renae/Desktop/testBook.txt")
      }
      Thread.sleep(1000)
    }
    ssc.stop()
}