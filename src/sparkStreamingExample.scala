import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._

/**
  * Created by Renae on 12/30/16.
  */
object sparkStreamingExample extends App {

  Logger.getLogger("org").setLevel(Level.WARN)

  val conf = new SparkConf().setMaster("local").setAppName("StreamingExample")
  //val sc = new SparkContext(conf)
  val ssc = new StreamingContext(conf, Seconds(1))

  val lines  = ssc.textFileStream("/users/Renae/Desktop/stream")
  val words = lines.map(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
  println(wordCounts.print())
  ssc.start()
  ssc.awaitTermination()
}
