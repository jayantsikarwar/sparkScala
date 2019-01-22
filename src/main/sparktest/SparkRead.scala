import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.spark_project.dmg.pmml.True
//case class one(CCN: String, REPORTDATETIME: String, SHIFT: String, OFFENSE: String, METHODname: String)
case class one(CCN: String,
REPORTDATETIME: String,
SHIFT: String,
OFFENSE: String,
METHOD: String,
LASTMODIFIEDDATE: String,
BLOCKSITEADDRESS: String,
BLOCKXCOORD: String,
BLOCKYCOORD: String,
WARD: String,
ANC: String,
DISTRICT: String,
PSA: String,
NEIGHBORHOODCLUSTER: String,
BUSINESSIMPROVEMENTDISTRICT: String,
BLOCK_GROUP: String,
CENSUS_TRACT: String,
VOTING_PRECINCT: String,
START_DATE: String,
END_DATE: String)

object SparkRead extends App {
  val ss = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
   /* .config("hive.metastore.warehouse.dir", "user/hive/warehouse")
    .enableHiveSupport() */
    .master("local")
    .getOrCreate()

  val sqlContext = ss.sqlContext

  import sqlContext.implicits._

  val df = ss.read.json("C:\\Users\\Mayank\\Desktop\\crime.json")
 //Displays the content of the DataFrame to stdout
 df.show()
 val ds = ss.read
    .option("header", "true")
    .option("charset", "UTF8")
    .option("delimiter", ",")
    .csv("C:\\Users\\Mayank\\Desktop\\wash_dc_crime_incidents_2013.csv")
    .as[one]
  ds.show()
  //val a =new Inc(ss)
 ds.createOrReplaceTempView("xyz")
 //val a = ss.sql("SELECT METHOD ,count(METHOD) FROM xyz group by METHOD").show()
// val a = ss.sql("SELECT CCN,SHIFT,OFFENSE FROM xyz where OFFENSE='ROBBERY' and SHIFT='DAY'").show()
// val a = ss.sql("SELECT OFFENSE ,count(OFFENSE)  FROM xyz where OFFENSE='ROBBERY' group by OFFENSE").show()
//val a = ss.sql("SELECT OFFENSE,count(OFFENSE) FROM xyz group by OFFENSE ").show()
}