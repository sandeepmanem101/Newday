import org.junit.{Before, Test}
import com.newday.Runner
import com.newday.SchemaLoader
import com.newday.MoviesWithRatings
import com.newday.UsersFavouriteMovies
import com.newday.MainSparkApp
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.text.{DecimalFormat, ParseException}
import java.util
import java.util.function.Consumer
import java.util.{List, Map}
import com.newday.SchemaLoader
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD


@Test
class MoviesRatingTest extends MoviesRatingTestData  {

  var spark: SparkSession = _
  var tempRowRDD: RDD[Row] = _
  var tempRowRDD1: RDD[Row] = _
  var src_DF_Ratings: DataFrame = _
  var src_DF_Movies: DataFrame = _
  val sLogLevel: String = "WARN"
  val moviesPath: String = "/dataset/movies.dat"
  val ratingsPath: String = "/dataset/ratings.dat"
  val sinkPath: String = "/dataset/assignment"
  val loadIndicator: String = "2"


@Before
  def setup(): Unit = {

  spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
  tempRowRDD = spark.sparkContext.parallelize(moviesTestData).map(r => Row.fromSeq(r))
  tempRowRDD.foreach(println)
  tempRowRDD1 = spark.sparkContext.parallelize(ratingsTestData).map(r => Row.fromSeq(r))
  tempRowRDD1.foreach(println)
  src_DF_Movies = spark.createDataFrame(tempRowRDD, movieSchema)
  src_DF_Ratings = spark.createDataFrame(tempRowRDD1, ratingSchema)
  src_DF_Movies.show(10)
  src_DF_Ratings.show(10)
}
@Test def test() {
  val args=new Array[String](5)
  args(0) = "WARN"
  args(1)  = "/dataset/movies.dat"
  args(2)  = "/dataset/ratings.dat"
  args(3)  = "/dataset/assignment"
  args(4)  = "2"

  val sLogLevel: String = "WARN"
  val moviesPath: String = "/dataset/movies.dat"
  val ratingsPath: String = "/dataset/ratings.dat"
  val sinkPath: String = "/dataset/assignment"
  val loadIndicator: String = "2"

  new MoviesWithRatings().run(src_DF_Movies, src_DF_Ratings).show(10)
  new UsersFavouriteMovies().run(src_DF_Movies, src_DF_Ratings).show(10)
}


}