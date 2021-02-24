package com.newday

import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * users Top Movies
  */

class UsersFavouriteMovies extends Runner {

  override def run(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]): Dataset[Row] = {
    // First find the top movies movies based on ratings per user.#

    val windowSpec = Window.partitionBy("userId").orderBy(org.apache.spark.sql.functions.col("rating").desc)

    val ratingMovies = ratingsDataset.withColumn("user_top_movies", row_number.over(windowSpec))
      .filter(col("user_top_movies") <= 3)
      .select("movieId", "userId", "rating", "user_top_movies")


    val userTopMovies = moviesDataset
      .join(ratingMovies, moviesDataset.col("movieId").equalTo(ratingMovies.col("movieId")))
      .drop(ratingMovies.col("movieId"))
      .select("userId", "movieId", "title", "rating", "user_top_movies")

    userTopMovies
  }
}

