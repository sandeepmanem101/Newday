import org.apache.spark.sql.types.{DataTypes, StringType, StructType}

trait MoviesRatingTestData {

   val movieSchema:StructType = new StructType()
    .add("movieId", DataTypes.IntegerType, false)
    .add("title", DataTypes.StringType, false)
    .add("genres", DataTypes.StringType, false)

   val ratingSchema:StructType = new StructType()
    .add("userId", DataTypes.IntegerType, false)
    .add("movieId", DataTypes.IntegerType, false)
    .add("rating", DataTypes.IntegerType, false)
    .add("timestamp", DataTypes.IntegerType, false)

  val moviesTestData:List[List[Any]]=List(
    List(1,"Toy Story (1995)","Animation|Children's|Comedy"),
    List(2,"Jumanji (1995)","Adventure|Children's|Fantasy"),
    List(3,"Grumpier Old Men (1995)","Comedy|Romance"),
    List(4,"Waiting to Exhale (1995)","Comedy|Drama"),
    List(5,"Father of the Bride Part II (1995)","Comedy"),
    List(6,"Heat (1995)","Action|Crime|Thriller"),
    List(7,"Sabrina (1995)","Comedy|Romance"),
    List(8,"Tom and Huck (1995)","Adventure|Children's"),
    List(9,"Sudden Death (1995)","Action"),
    List(10,"GoldenEye (1995)","Action|Adventure|Thriller")
  )

  val ratingsTestData:List[List[Any]]=List(
    List(1,1,5,978300760),
    List(2,1,3,978302109),
    List(1,2,3,978301968),
    List(3,1,4,978300275),
    List(1,3,5,978824291),
    List(1,2,3,978302268),
    List(5,5,5,978302039),
    List(7,10,5,978300719),
    List(7,5,4,978302268),
    List(7,7,4,978301368),
    List(1,6,5,978824268),
    List(1,4,4,978301752),
    List(6,4,4,978302281),
    List(6,4,4,978302124),
    List(6,1,5,978301753),
    List(1,2,4,978302188),
    List(1,3,3,978824268),
    List(2,4,4,978301777),
    List(2,9,5,978301713),
    List(5,8,4,978302039)
  )

}
