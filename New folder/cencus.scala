/*MySql Table Creating:
  ---------------------
create table population (Zip_Code varchar(100), Total_Population varchar(100), Median_Age varchar(100), Total_Males varchar(100), Total_Females varchar(100), Total_Households varchar(100), Average_Household_Size varchar(100))

Data Location:
  --------------

/user/cloudera/spark/census/2010_Census_Populations_by_Zip_Code.csv

Sqoop command:
  ---------------
sqoop import --connect jdbc:mysql://localhost:3306/spark --username root --password cloudera --table population --target-dir "/user/cloudera/spark/census/sqooped_data" -m 1

Hive Query:
  ------------
create external table population (zip_code String, total_population String, madian_age String, total_males String, total_females String, total_households String, average_household_size String) row format delimited fields terminated by "," location "/user/cloudera/spark/census/hive"


Scala Schema:
  ----------------*/
package com.hcl.tech.census

case class CensusSchema (Zip_Code: String, Total_Population: String, Median_Age: String, Total_Males: String,
                         Total_Females: String, Total_Households: String, Average_Household_Size: String)

/*spark code to load data to mysql
---------------------------------*/
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties

object CensusMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("census").master("local").getOrCreate()
    import spark.implicits._
    val rawData = spark.read.option("header", "true").option("inferschema", "false").csv("/user/cloudera/spark/census/2010_Census_Populations_by_Zip_Code.csv")
    val properties = new Properties()

    properties.put("driver","com.mysql.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "jayant123")

    rawData.write.jdbc("jdbc:mysql://localhost:3306/demo", "population", properties)
  }
}

/*Spark Processing:
  ----------------------*/
package com.hcl.tech.census

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CensusProcession {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("census").master("local").getOrCreate()

    val textFiles = spark.sparkContext.wholeTextFiles("/user/cloudera/spark/census/sqooped_data/part-00000")

    val value = textFiles.map(data => data._2).flatMap(line => line.split("\n"))

    val header = value.first()

    val filtered =value.filter(row => row != header)
    import spark.implicits._
    val df = filtered.map(line => line.split(",")).map(rec => CensusSchema(rec(0), rec(1), rec(2), rec(3), rec(4), rec(5), rec(5))).toDF()

    df.show()

    println("the number of partitions are " + df.rdd.getNumPartitions)
    df.repartition(2)
    df.show(10)
    df.registerTempTable("population")

    spark.sql("select Zip_Code, Total_Population, Median_Age, Total_Males, Total_Females, Total_Households, Average_Household_Size from population where Zip_Code not in ('90068','90201')").
      write.csv("/user/cloudera/spark/censusoutput")

    df.rdd.map(line => line.mkString(",").replace("[", "").replace("]", "")).saveAsTextFile("/user/cloudera/spark/census/textFile")

    df.rdd.map(line => ("null", line.mkString(","))).saveAsSequenceFile(/user/cloudera/spark/census/sequenceFile")


      }
      }
