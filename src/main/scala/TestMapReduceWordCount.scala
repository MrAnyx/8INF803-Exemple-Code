import org.apache.spark.{SparkConf, SparkContext}

object TestMapReduceWordCount extends App {
  val conf = new SparkConf().setAppName("Test en classe").setMaster("local[4]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val data=sc.textFile("input.txt")
  data.collect.foreach(println)

  val splitdata = data.flatMap(line => line.split(" "))
  splitdata.collect.foreach(println)

  val mapdata = splitdata.map(word => (word,1))
  mapdata.collect.foreach(println)

  val reducedata = mapdata.reduceByKey(_ + _)
  reducedata.collect.foreach(println)

}
