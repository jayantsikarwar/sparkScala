
/*Data Location:
  --------------
/user/cloudera/spark/facebook/dataset_Facebook.csv

MY Sql Table create statement:
  -------------------------------
create table facebook(Page_total_likes varchar(100), Type varchar(100), Category varchar(100), Post_Month varchar(100), Post_Weekday varchar(100), Post_Hour varchar(100), Paid varchar(100), Lifetime_Post_Total_Reach varchar(100), Lifetime_Post_Total_Impressions varchar(100), Lifetime_Engaged_Users varchar(100), Lifetime_Post_Consumers varchar(100), Lifetime_Post_Consumptions varchar(100), Lifetime_Post_Impressions_by_people_who_have_liked_your_Page varchar(100), Lifetime_Post_reach_by_people_who_like_our_Page varchar(100), Lifetime_People_who_have_liked_Page_and_engaged_with_your_post varchar(100), comment varchar(100), likes varchar(100), shares varchar(100), Total_Interactions varchar(100));

Sqoop Import Statement:
  -------------------------
sqoop import --connect jdbc:mysql://localhost:3306/spark --username root --password cloudera --table facebook --target-dir /user/cloudera/spark/facebook/sqooped_data -m 1

Hive Table create statement:
  ----------------------------
create external table facebook (Page_total_likes String, Type String, Category String, Post_Month String, Post_Weekday String, Post_Hour String, Paid String, Lifetime_Post_Total_Reach String, Lifetime_Post_Total_Impressions String, Lifetime_Engaged_Users String, Lifetime_Post_Consumers String, Lifetime_Post_Consumptions String, Lifetime_Post_Impressions_by_people_who_have_liked_your_Page String, Lifetime_Post_reach_by_people_who_like_our_Page String, Lifetime_People_who_have_liked_Page_and_engaged_with_your_post String, comment String, like String, share String, Total_Interactions String) row format delimited fields terminated by "," location "/user/cloudera/spark/facebook/sqooped_data";

HIve intermediate table:
  ----------------------------
create table intermediate(post_month String, post_week String, type String, category String, comment String, like int, share int)

insert into table intermediate select post_month, post_weekday,type, category, comment, cast(like as int), cast(share as int) from facebook;

Hive DMLs:
  ---------------
select max(like) from intermediate where post_month= '12';

select a.category, a.type, a.share from (select category, type,share, row_number() over (order by share desc) as row_number from intermediate order by share desc limit 5) a where a.row_number='3'

select category, max(like) from intermediate group by category


Spark Code To insert data into MYSQL:
  --------------------------------------*/
package com.hcl.tech.facebook

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties
import org.apache.spark.sql.SaveMode

object FacebookDataLoad {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("facebook").setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val facebookRdd = sc.textFile("/user/cloudera/spark/facebook/dataset_Facebook.csv")

    val header = facebookRdd.first()
    val filteredRdd = facebookRdd.filter(record => record != header).map(record => record.split(";"))

    import sqlContext.implicits._
    val facebookDF = filteredRdd.map(line => FacebookSchema(line(0), line(1), line(2), line(3), line(4), line(5),line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17), line(18))).toDF()

    val properties = new Properties()

    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "cloudera")

    facebookDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark", "facebook", properties)
  }
}

/*Spark Code to repeate HIve operations:
  --------------------------------------*/
val intermediateDF = facebookDF.select($"Post Month" as "post_month", $"Post Weekday" as "post_weekday", $"Type", $"Category", $"comment", $"like", $"share").
  withColumn("like", $"like".cast(org.apache.spark.sql.types.IntegerType)).withColumn("share", $"share".cast(org.apache.spark.sql.types.IntegerType))
intermediateDF.registerTempTable("intermediate")

sqlContext.sql("select max(like) from intermediate where post_month = 12").show()
sqlContext.sql("select a.category, a.type, a.share from (select category, type, share, row_number() over (order by share desc) as row_number from intermediate order by share desc) a where a.row_number='3'" ).show()
sqlContext.sql("select category, max(like) from intermediate group by category").show()


/*Case Class:
  ----------------*/
package com.hcl.tech.facebook

case class FacebookSchema (Page_total_likes:String, Type:String, Category:String, Post_Month:String, Post_Weekday:String, Post_Hour:String, Paid:String,
                           Lifetime_Post_Total_Reach:String, Lifetime_Post_Total_Impressions:String, Lifetime_Engaged_Users:String, Lifetime_Post_Consumers:String,
                           Lifetime_Post_Consumptions:String, Lifetime_Post_Impressions_by_people_who_have_liked_your_Page:String,
                           Lifetime_Post_reach_by_people_who_like_our_Page:String, Lifetime_People_who_have_liked_Page_and_engaged_with_your_post:String,
                           comment:String, likes:String, shares:String, Total_Interactions:String)


