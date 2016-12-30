import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}


/**
  * Created by Renae on 12/30/16.
  */


object sparkHello extends App{

  Logger.getLogger("org").setLevel(Level.WARN)

  val conf = new SparkConf().setMaster("local").setAppName("spark-play")
  val sc = new SparkContext(conf)
  val data = sc.parallelize(1 to 10000000).collect().filter(_ < 1000)
  data.foreach(println)
  sc.stop()
}
