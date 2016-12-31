package org.apache.spark.examples.streaming

import org.apache.hadoop.io.LongWritable

import scala.collection.mutable.Queue
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}


object QueueStream extends App{

  Logger.getLogger("org").setLevel(Level.WARN)
  val conf = new SparkConf().setAppName("QueueStream").setMaster("local[3]")
  // Create the context
  val ssc = new StreamingContext(conf, Seconds(1))
  //val sparkContext = new SparkContext(conf)
  val rdd  = ssc.sparkContext.textFile("/Users/Renae/Desktop/stream/*")
  val rddQueue: Queue[RDD[String]] = Queue()
  val dstream = ssc.queueStream(rddQueue)
  val stuff = dstream.map((_,1))
  stuff.print()
  rddQueue += rdd


  ssc.start()
  ssc.awaitTermination()
}