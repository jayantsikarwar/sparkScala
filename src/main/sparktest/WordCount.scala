import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
  val ss = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .master("local")
    .getOrCreate()
  val sqlContext = ss.sqlContext
  val linesDF = ss.read.textFile("file.txt").toDF("line")
  val wordsDF = linesDF.explode("line", "word")((line: String) => line.split(" "))
  val wordCountDF = wordsDF.groupBy("word").count()

  val joinedDF = wordCountDF.join(wordsDF, testDF("PassengerId") === genmodDF("PassengerId"), "inner")
}
