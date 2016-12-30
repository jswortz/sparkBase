import org.apache.spark.{SparkConf, SparkContext}
val conf = new SparkConf().setMaster("local").setAppName("spark-play")
//
//sc.stop()
Array(1,2,3)
val sc = new SparkContext(conf)


