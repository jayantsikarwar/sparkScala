/*HIve Tables:
  -----------
create table movies(movieId String, movieName String, genre String) row format delimited fields terminated by ",";

create table ratings(userId String, movieId String, rating String) row format delimited fields terminated by ",";

Spark Case class for schema:
  ----------------------------
case class MovieSchema (movieId: String, movieName: String, genre: String)

case class RatingsSchemaa(userId: String, movieId: String, rating: Int)

Data Location:
  ------------------
/user/cloudera/spark/movie_data/movie.dat

/user/cloudera/spark/movie_data/ratings.dat

Spark Code:
  -----------*/
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object MovieDataMain {
  def main(args: Array[String]): Unit = {

    //Spark Context
    val sparkConf = new SparkConf().setMaster("local").setAppName("movieData")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //Reading Input files
    val movieData = sc.textFile("/user/cloudera/spark/movie_data/movies.dat")
    val ratingData = sc.textFile("/user/cloudera/spark/movie_data/ratings.dat")

    //applying Schema
    import sqlContext.implicits._
    val movieDF = movieData.map(row => row.split("::")).map(list => MovieSchema(list(0), list(1), list(2))).toDF()
    val ratingsDF = ratingData.map(row => row.split("::")).map(line => RatingsSchemaa(line(0), line(1), line(2).toInt)).toDF()

    //Displaying top 20 records of the Data Frame
    movieDF.show(20)
    ratingsDF.show(20)

    //Number of Unique users who Rated
    println("Unique users who are rated to Movies" + ratingsDF.select($"userId").distinct().count())

    //Most Rated movies
    val joinedDF = movieDF.join(ratingsDF, movieDF("movieId") === ratingsDF("movieId")).select($"movieName", $"rating").groupBy($"movieName")
    joinedDF.count().orderBy($"count".desc).show()

    //Top 10 high rated movies
    val avgRating = joinedDF.avg("rating")
    val top10Rated = avgRating.filter($"avg(rating)" > 4).orderBy($"avg(rating)".desc).limit(10)
    top10Rated.show()

    //Average rating of top 10 high rated movies
    println("average rating of top 10 rated movies" + top10Rated.select(avg($"avg(rating)")).show())

    //Top 20 worst rated movies
    val last20Rated = avgRating.filter($"avg(rating)" < 2).orderBy($"avg(rating)".asc).show(20)
  }
}


Write data to Hive tables from spark:
  ------------------------------
movieDF.write.mode("overwrite").saveAsTable("spark.movies")

ratingsDF.write.mode("overwrite").saveAsTable("spark.ratings")
