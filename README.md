# newday


Spar Submit Command 

spark-submit --master local[*] --class CLASS_NAME JAR_LOCATION_WITH_JARNAME PARAM1 PARAM2 PARAM3 PARAM4 PARAM5

PARAM1 --> valid Log levels are <WARN or ERROR or INFO>
PARAM2 --> valid location of movies data
PARAM3 --> valid location of ratings data
PARAM4 --> valid location to sink
PARAM5 --> Indicator for data loading <1 for Movieswithrating or 2 for UserTopMovies>
  
Example:

//To load UsersTopMovies://

spark-submit --master local[*] --class com.newday.MainSparkApp D:\Productivity\newday\target\scala-2.12\newday_2.12-0.1.jar WARN D:\Productivity\ml-1m\movies.dat D:\Productivity\ml-1m\ratings.dat D:\Productivity\ml-1m\assignment 2

//To load Movieswithrating://

spark-submit --master local[*] --class com.newday.MainSparkApp D:\Productivity\newday\target\scala-2.12\newday_2.12-0.1.jar WARN D:\Productivity\ml-1m\movies.dat D:\Productivity\ml-1m\ratings.dat D:\Productivity\ml-1m\assignment 1
