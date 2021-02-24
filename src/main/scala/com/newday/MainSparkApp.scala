package com.newday

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.text.{DecimalFormat, ParseException}
import java.util.function.Consumer
import com.newday.SchemaLoader
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType


object MainSparkApp {

    private val SPARK_APP_NAME = "NewDaySparkAssignment"
    private val MOVIES_WITH_RATINGS = "\\movieswithratings"
    private val USERS_TOP_MOVIES = "\\userstopmovies"
    var noPartitions = 1

  /**
    * Main job starts from here.
    */

    def main(args: Array[String])= {

      var i: Int = 0
      for (i <- 0 to (args.length - 1)) {
        println("args[" + i + "] :" + args(i))
      }
      if (args.length == 0) {
        println("You didn't passed any Params - Pass the arguments correctly!  Param1 is Loglevel,Param2 is MoviedataFileLocation,Param3 is RatingsDataFileLocation,Param4 is SinkLocation,Param5 is LoadIndicator")
      } else if (args.length > 5) {
        println("You passed more or less Params - Pass the arguments correctly!  Param1 is Loglevel,Param2 is MoviedataFileLocation,Param3 is RatingsDataFileLocation,Param4 is SinkLocation,Param5 is LoadIndicator")
      } else {
        try {
          val sLogLevel: String = args(0)
          val moviesPath: String = args(1)
          val ratingsPath: String = args(2)
          val sinkPath: String = args(3)
          val loadIndicator: String = args(4)
          println("You All Params Set..job Starting.....")
          run(sLogLevel,moviesPath,ratingsPath,sinkPath,loadIndicator)

          println("Job Ended Successfully.....")
        } catch {
          case e: ParseException => println("Error! Job Failed")
        }
      }
    }


  /**
    * Run and get the spark.
    */
  def run(sLogLevel: String,movieIn: String,ratingIn:String,sinkOut:String,mId:String): Unit = {
    // Build spark session
    val spark = SparkSession
      .builder.master("local[*]")
      .appName(SPARK_APP_NAME)
      .getOrCreate

    spark.sparkContext.setLogLevel(sLogLevel)

    // Read initial datasets as non-streaming DataFrames
    val schemaLoader = new SchemaLoader()
    val moviesDataset = readCsvIntoDataframe(spark, movieIn, schemaLoader.getMovieSchema)
    val ratingsDataset = readCsvIntoDataframe(spark, ratingIn, schemaLoader.getRatingSchema)

    val startTime = System.nanoTime
    val colRows = runMetric(moviesDataset, ratingsDataset, mId)
    partitionCheck(colRows)
    writter(colRows,writterId(sinkOut,mId))
    val endTime = System.nanoTime
    printRows(startTime, endTime)
    spark.stop()
  }

  /**
    * Instantiate specified metrics solver class and trigger the evaluation.
    *
    * @param moviesDataset  Dataset containing the movies information.
    * @param ratingsDataset Dataset containing the ratings information.
    * @param sMetric        The metric ID to evaluate.
    * @return Dataset collected into a List.
    */
  private def runMetric(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row], sMetric: String) = {
    val metricResult = null
    sMetric.trim match {
      case "1" =>
        System.out.println("\nMovies Along With Ratings")
        new MoviesWithRatings().run(moviesDataset, ratingsDataset)
      case "2" =>
        System.out.println("\nUsers With Favourite Movies")
        new UsersFavouriteMovies().run(moviesDataset, ratingsDataset)
      case _ =>
        val sExceptionMessage = "Tried to obtain an unavailable metric: " + sMetric.trim
        System.out.println(sExceptionMessage)
        throw new RuntimeException(sExceptionMessage)
    }
  }

  /**
    * Get bytes to calculate no of partitions
    */

  def getByte(value:Any):Long={
    val stream: ByteArrayOutputStream=new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray.length
  }

  /**
    * Calculate no of partitions
    */

  def partitionCheck(partitionCheck: Dataset[Row]) = {
    val count = partitionCheck.count()
    if(count!=0) {
      val rowCount = count
      val rowSize = getByte(partitionCheck.head)
      val partitionSize = 268435456 //million bytes in MB
      noPartitions = (rowSize * rowCount / partitionSize).toInt
      if (noPartitions <= 0) {
        noPartitions = 1
      } else {
        noPartitions = noPartitions
      }
    }
    else
    {
      println("There are No Records in Source")
    }
  }


  /**
    * Get which sink path to load the data
    */

   def writterId(sinkPath:String,sMetric: String) = {
    val metricResult = null
    sMetric.trim match {
      case "1" => sinkPath+MOVIES_WITH_RATINGS
      case "2" => sinkPath+USERS_TOP_MOVIES
      case _ =>
        val sExceptionMessage = "Tried to obtain an unavailable metric: " + sMetric.trim
        System.out.println(sExceptionMessage)
        throw new RuntimeException(sExceptionMessage)
    }
  }

  /**
    * writter to load the data
    */
  def writter(writingData: Dataset[Row],wId: String) = {
    val sinkPath = wId
    println(wId)
    writingData.coalesce(noPartitions).write.mode(SaveMode.Overwrite)
      .option("header","true")
      .option("quote","\u0000")//To avoid double-quotes
      .csv(sinkPath)
  }

  /**
    * Read file into dataframe using an existing schema.
    *
    * @param spark        SparkSession.
    * @param filename path to the file.
    * @param schema   dataframe schema.
    */
  private def readCsvIntoDataframe(spark: SparkSession, filename: String, schema: StructType) = {
    val fullPath = filename
     spark.read
      .format("csv")
      .option("delimiter","::")
      .schema(schema).load(fullPath)
    }

  private def printRows(startTime: Long, endTime: Long): Unit = {
    val duration = ((endTime - startTime) / 1000000000.toDouble).toDouble
    println("Results obtained in " + new DecimalFormat("#.##").format(duration) + "s\n")

  }


}



