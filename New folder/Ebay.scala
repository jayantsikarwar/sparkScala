// Data Location:
  ----------------
//  spark/ebay/ebay.csv




// Spark Code:
// -----------
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object EBayMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ebayData")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //Load the input file as RDD
    val textFile = sc.textFile("/user/cloudera/spark/ebay/ebay.csv")

    //Return the first element
    println(textFile.first())
    import sqlContext.implicits._

    //Use a Scala case class to define the Auction schema
    val ebayDF = textFile.map(row => row.split(",")).map(fields => EBaySchema(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6).toDouble, fields(7), fields(8))).toDF()

    //Find the overall no of auctions happened
    println("Total number of Acutions " + ebayDF.count())

    //Display the 20 rows of DataFrame
    ebayDF.show(20)

    //Find how many auctions were held?
    println("Number of distinct Auctions  " + ebayDF.select($"auctionId").distinct().count())

    //How many bids were happened per item?
    ebayDF.groupBy($"item").count().show()

    //What's the minimum, maximum and average number of bids per item?
    ebayDF.groupBy($"item").agg(min($"bid"), max($"bid"), avg($"bid")).show()

    //Get the auctions with closing price > 100 in tabular format
    ebayDF.filter($"price" > 100).show()

    //Register main DataFrame as a temp table
    ebayDF.registerTempTable("ebayTable")

    //How many bids per auction?
    sqlContext.sql("select auctionId, count(bid) from ebayTable group by auctionId").show()

    //Display maximum price of each auction
    val maxPrice = sqlContext.sql("select auctionId, max(price) from ebayTable group by auctionId")
    maxPrice.show()

    //Generate the explain plan of the above result set
    maxPrice.explain(true)

    //Cache the temporary table
    sqlContext.cacheTable("ebayTable")
  }
}

