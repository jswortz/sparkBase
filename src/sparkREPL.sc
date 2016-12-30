import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf().setMaster("local").setAppName("spark-play")

val sc = new SparkContext(conf)

val data = sc.parallelize(1 to 10).collect().filter(_ < 1000)
data.foreach(println)
sc.stop()



