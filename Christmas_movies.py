from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

spark = SparkSession.builder.appName("ChristmasMovies").getOrCreate()

df1 = spark.read.format("csv").option("header", True).load("data/ratings.csv")
df2 = spark.read.format("csv").option("header", True).load("data/links.csv")
df3 = spark.read.format("csv").option("header", True).load("data/movies.csv")

df11 = df1.groupBy("movieId").agg(avg("rating").alias("avg_rating"))

df_movie = df11.join(df2, df11.movieId == df2.movieId, "inner") \
               .join(df3, df11.movieId == df3.movieId, "inner") \
               .where((col("avg_rating") > 3) & (col("title").like("%Christmas%"))) \
               .select(df11.movieId, round(df11.avg_rating, 2).alias("avg_rating"), df2.imdbId, df3.title)
df_movie.printSchema()

df_movie2 = df_movie.withColumn("movieId", df_movie.movieId.cast("int"))
df_movie2.printSchema()

df_movie3 = df_movie2.orderBy("movieId")
df_movie3.show(10, truncate = False)

df_movie3.write.format("csv").option("header", "true").save("df_movie")
