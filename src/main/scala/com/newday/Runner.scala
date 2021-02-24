package com.newday
import org.apache.spark.sql.{Dataset, Row}

trait Runner {

  def run(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]): Dataset[Row]


}
