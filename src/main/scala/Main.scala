import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Main extends App {
  val spark = SparkSession.builder
                          .appName("Spark MovieLens")
                          .master("local")
                          .getOrCreate()

  import spark.implicits._

  val sc                = spark.sparkContext
  val movies            = sc.textFile("data/movies.dat").map(Parsers.parseMovieLine).toDF()
  val ratings           = sc.textFile("data/ratings.dat").map(Parsers.parseRatingLine).toDF()
  val aggregatedRatings = ratings.groupBy("movieId")
                                 .agg(
                                   min("rating").as("minRating"),
                                   max("rating").as("maxRating"),
                                   avg("rating").as("avgRating")
                                 )
  val movieRatings      = movies.join(aggregatedRatings, "movieId")

  movieRatings.write.mode(SaveMode.Overwrite).parquet("data/movie-ratings.parquet")

  sc.stop()
}

case class Movie(movieId: String, title: String, genres: Seq[String])
case class Rating(userId: String, movieId: String, rating: Int, timestamp: Int)

object Parsers {
  def parseMovieLine(line: String): Movie = {
    val parts = line.split("::")
    Movie(
      movieId = parts(0),
      title   = parts(1),
      genres  = parts(2).split('|').toSeq
    )
  }
  def parseRatingLine(line: String): Rating = {
    val parts = line.split("::")
    Rating(
      userId    = parts(0),
      movieId   = parts(1),
      rating    = parts(2).toInt,
      timestamp = parts(3).toInt
    )
  }
}