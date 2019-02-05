DAta Location:
  --------------
// /user/cloudera/spark/criminal_data/wash_dc_crime_incidents_2013.csv
// /user/cloudera/spark/criminal_data/crime.json

Spark Schema Class:
  --------------------
case class CriminalDataSchema (CCN: String, REPORTDATETIME: String, SHIFT: String, OFFENSE: String, METHOD: String, LASTMODIFIEDDATE: String,
                               BLOCKSITEADDRESS: String, BLOCKXCOORD: String, BLOCKYCOORD: String, WARD: String, ANC: String, DISTRICT: String, PSA: String,
                               NEIGHBORHOODCLUSTER: String, BUSINESSIMPROVEMENTDISTRICT: String, BLOCK_GROUP: String, CENSUS_TRACT: String, VOTING_PRECINCT: String,
                               START_DATE: String,  END_DATE: String)

Spark Code:
  -----------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object CriminalDataMain {
  def main(args: Array[String]): Unit = {

    //Spark Conf
    val sparkConf = new SparkConf().setAppName("CriminalData").setMaster("local")

    //spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //loading criminal data
    val criminalData = sc.textFile("/user/cloudera/spark/criminal_data/wash_dc_crime_incidents_2013.csv")

    //filter header
    val header = criminalData.first()
    import sqlContext.implicits._
    val DataFiltered = criminalData.filter(row => row != header)

    //Applying Schema
    val criminalDataFrame = DataFiltered.map(record => record.split(",")).map(list => CriminalDataSchema(list(0), list(1), list(2), list(3), list(4),list(5), list(6), list(7), list(8), list(9), list(10), list(11), list(12), list(13), list(14), list(15), list(16), list(17), list(18), list(18))).toDF()

    //Extract Required data
    val staggedDF = criminalDataFrame.select($"CCN", $"REPORTDATETIME", $"SHIFT", $"OFFENSE", $"METHOD")

    //Analysis (Show the results line by line)

    //Find the no of crimes happen based on murder weapon
    staggedDF.groupBy($"METHOD").count().show()

    //Get all the crimes related to robbery
    val robberies = staggedDF.filter($"OFFENSE" === "ROBBERY")
    robberies.show()

    //How many robberies happened?
    println(robberies.count())

    //List the robberies happened in the day time
    robberies.filter($"SHIFT" === "DAY").show()

    //List all the offenses along with the no of occurrences
    staggedDF.groupBy($"OFFENSE").count().show()

    //Check the no of RDD partitions
    println("Number partitions of RDD is " + criminalData.getNumPartitions)

    //During which police shift did the most crimes occur in 2013, show along with count of crimes
    println(staggedDF.groupBy($"SHIFT").count().sort().first())

    //Display the number of homicides by weapon
    staggedDF.filter($"OFFENSE" === "HOMICIDE").groupBy($"METHOD").count().show()

    //Group the data by type of crime
    staggedDF.groupBy($"METHOD").count().write.save("/user/cloudera/spark/criminal_data/output")

    //Add a month column to the dataset. (Display full month name)
    val months = staggedDF.withColumn("MONTH", (date_format(from_unixtime(unix_timestamp($"REPORTDATETIME", "MM/dd/yyyy"), "yyyy-MM-dd"), "MMMMM")))

    //List all the crimes per month
    months.groupBy($"MONTH").count().show()

    //Load json file, Show the ccn and area of the cop & Flatten the cop data
    val copData = sqlContext.read.json("/user/cloudera/spark/criminal_data/crime.json").select($"ccn", $"cop.area" as "area", $"cop.name" as "name")

    //join the json file with the csv dataset
    val joinedData = staggedDF.join(copData, staggedDF("CCN") === copData("ccn")).select($"SHIFT", $"OFFENSE", $"area", $"name")

    //Save the result as parquet file in cluster
    joinedData.write.format("parquet").save("/user/cloudera/spark/criminal_data/crime_output")
  }
}