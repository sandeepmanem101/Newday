package com.newday

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

/**
  * Movies details along with the ratings Min/Max/Avg
  */

class MoviesWithRatings extends Runner {

  override def run(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]): Dataset[Row] = {
    // First find the ratings min/max/avg for the movies based on movieid.#
    val windowSpec  = Window.partitionBy("movieId")
    val ratingMovies = ratingsDataset
      .groupBy("movieId")
      .agg(avg("rating").alias("avg_rating"),max("rating").alias("max_rating"),min("rating").alias("min_rating"))
      .select("movieId","max_rating","min_rating","avg_rating")


    val movieRatings = moviesDataset
      .join(ratingMovies, moviesDataset.col("movieId").equalTo(ratingMovies.col("movieId")))
      .drop(ratingMovies.col("movieId"))
      .select("movieId","title","genres","max_rating","min_rating","avg_rating")

    movieRatings
  }


}
