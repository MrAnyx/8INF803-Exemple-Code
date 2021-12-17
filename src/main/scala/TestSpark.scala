import org.apache.spark.sql.SparkSession

object TestSpark extends App {

  val spark = SparkSession
    .builder()
    .appName("D&D spells")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val path = "spells.json"
  val spellsDF = spark.read
    .option("multiline", "true")
    .json(path)

  spellsDF.printSchema()

  spellsDF.createOrReplaceTempView("spells")

  val spellsLevel4 = spark.sql("SELECT * FROM spells WHERE level >= 4")
  spellsLevel4.show()
}
