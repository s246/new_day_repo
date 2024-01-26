import os
import tempfile
from typing import Tuple

from data_transformations import movies_transform
from tests.integration import SPARK

from data_transformations import movies_ingest

from pyspark.sql import functions as F


#SAMPLE DATA FOR TESTS
ratings_data= [
    (1, 1, 5, 1643667623),
    (2, 2, 4, 1643667624),
    (2, 3, 3, 1643667625),
    (3, 2, 2, 1643667626),
    (1, 3, 4, 1643667627),
]

movies_data = [
    (1, "Movie 1", "Genre 1|Genre 2|Genre 5"),
    (2, "Movie 2", "Genre 3|Genre 4"),
    (3, "Movie 3", "Genre 1|Genre 5"),
    (4, "Movie 4", "Genre 2|Genre 3"),
]

users_data = [
    (1, "M", 25, 10, 12345),
    (2, "F", 30, 15, 23456),
    (3, "M", 35, 20, 34567),
]

# dict of col names based on data documentation (README FILE)
col_names = {'movies': ['MovieID', 'Title', 'Genres'],
             'users': ['UserID', 'Gender', 'Age', 'Occupation', 'Zip-code'],
             'ratings': ['UserID', 'MovieID', 'Rating', 'Timestamp']}


#@pytest.mark.skip
def test_should_add_kpi_cols() -> None:

    #create ingestion and tranform folders for tests on /temp/xxx on each test run
    given_ingest_folder, given_transform_folder = __create_ingest_and_transform_folders()

    #run job with sample data and temp paths
    movies_transform.run(SPARK, given_ingest_folder, given_transform_folder)

    #read result
    actual_dataframe = SPARK.read.parquet(given_transform_folder+ '/' + 'movies_kpis')

    #EXPECTED DF
    expected_dataframe = SPARK.createDataFrame(
        [
            list(movies_data[0]) + [5.0, 5, 5],
            list(movies_data[1]) + [3.0, 4, 2],
            list(movies_data[2]) + [3.5, 4, 3],
            list(movies_data[3]) + [None, None, None],
        ],
        movies_ingest.sanitize_columns(col_names.get('movies')) + ['avg_rating', 'max_rating', 'min_rating']
    )

    #order to compare results
    expected_dataframe=expected_dataframe.orderBy(F.col('avg_rating').asc())
    actual_dataframe=actual_dataframe.orderBy(F.col('avg_rating').asc())

    #TEST
    assert expected_dataframe.collect() == actual_dataframe.collect()


def __create_ingest_and_transform_folders() -> Tuple[str, str]:

    #CREATE temp folders
    base_path = tempfile.mkdtemp()
    ingest_folder = "%s%singest" % (base_path, os.path.sep)
    transform_folder = "%s%stransform" % (base_path, os.path.sep)

    #save sample data as parquet file
    ingest_dataframe1 = SPARK.createDataFrame(ratings_data, movies_ingest.sanitize_columns(col_names.get('ratings')))
    ingest_dataframe1.write.parquet(ingest_folder+'/ratings', mode='overwrite')

    ingest_dataframe2 = SPARK.createDataFrame(movies_data, movies_ingest.sanitize_columns(col_names.get('movies')))
    ingest_dataframe2.write.parquet(ingest_folder+'/movies', mode='overwrite')

    ingest_dataframe3 = SPARK.createDataFrame(users_data, movies_ingest.sanitize_columns(col_names.get('users')))
    ingest_dataframe3.write.parquet(ingest_folder+'/users', mode='overwrite')

    return ingest_folder, transform_folder