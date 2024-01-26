import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def average_rating(movies,ratings):

    #calcualte avg, max , min of ratings per movie
    kpi_df=ratings.groupBy(F.col('movie_id'))\
                   .agg(F.avg('rating').alias('avg_rating'),
                        F.max('rating').alias('max_rating'),
                        F.min('rating').alias('min_rating'))

    #round avg rating to 4 decimal points
    kpi_df=kpi_df.withColumn('avg_rating',F.round('avg_rating',4))

    #add new calculated columns to  movies df
    added_values_df = movies.join(kpi_df, on=['movie_id'], how='left')

    return added_values_df


def top_ranked(ratings,movies):

    #obtain readable date column from unix timestamp by convertig datatype (use when condition in case invalid unix timestap)
    ratings_with_date=ratings.withColumn("date",
                                         F.when(ratings['timestamp'].isNotNull(),ratings["timestamp"].cast("timestamp"))
                                         .otherwise(F.lit(None)))

    #create window partioned by user and ordered by date of rating
    # (in case more than 3 movies are rated 5 then it chooses the top 3 ones with the latest rating)
    window_spec = Window.partitionBy("user_id").orderBy(F.col("rating").desc(),F.col('date').desc())

    #rank every movie by user using the window partition and enumerate using row num
    ranked_df = ratings_with_date.withColumn("rank", F.row_number().over(window_spec))

    #filter top 3 per user
    top_3_movies_per_user = ranked_df.filter(F.col("rank") <= 3)

    #get names of movies
    top_3_movies_per_user=top_3_movies_per_user.join(movies.select(['movie_id','title']), on=['movie_id'], how='left')

    #another prsesentation creating a column of arrays with each movie (name or id)
    top_3_movies_per_user_array=top_3_movies_per_user.groupBy(F.col('user_id')).agg(F.collect_set('title').alias('top3'))

    #another presentation with ranked order (only relevant fileds)
    top_3_movies_per_user=top_3_movies_per_user.select(['user_id', 'title', 'rating','date','rank'])

    return top_3_movies_per_user, top_3_movies_per_user_array


def run(spark, ingest_path, transformation_path):

    #data dict for scalability in case more tables are ingested
    data_dict={}

    #for every ingested parquet file
    for data_file in os.listdir(ingest_path):
        #read ingested parquet
        curr = spark.read.parquet(ingest_path + f"/{data_file}")
        #add to data dict with name as key
        data_dict[data_file]=curr

        #copy original data to final transformed folder as instructions say
        curr.write.mode("overwrite").parquet(transformation_path + '/' + data_file)


    #run task1
    task_1=average_rating(data_dict['movies'], data_dict['ratings'])

    #run task2
    task_2_option1,task_2_option2=top_ranked(data_dict['ratings'],data_dict['movies'])


    #write final results to tranformed folder
    task_1.write.mode("overwrite").parquet(transformation_path + '/' + 'movies_kpis')
    task_2_option1.write.mode("overwrite").parquet(transformation_path + '/' + 'user_top_three_option1')
    task_2_option2.write.mode("overwrite").parquet(transformation_path + '/' + 'user_top_three_option2')
