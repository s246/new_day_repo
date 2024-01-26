import logging
import os
from typing import List


#standard column name cleaning
def sanitize_columns(columns: List[str]) -> List[str]:

    return [column.replace(" ", "_")\
                .replace('-','_')\
                .replace('ID','_id').lower() for column in columns]


#spark job
def run(spark, ingest_path, transformation_path):

    #dict of col names based on data documentation (README FILE)
    col_names={'movies':['MovieID','Title','Genres'],
                'users':['UserID','Gender','Age','Occupation','Zip-code'],
                'ratings':['UserID','MovieID','Rating','Timestamp']}

    #escalable for every .dat file present on data source folder
    for data_file in os.listdir(ingest_path):
        #get .dat files only
        if data_file.endswith(".dat"):
            logging.info(f"Reading {data_file} file from: {ingest_path}")

            #read as csv with proper sep (inferschema for velocity)
            input_df = spark.read.format("org.apache.spark.csv").option("header", False)\
                                                                 .option("inferSchema", True)\
                                                                 .option("delimiter", "::")\
                                                                 .csv(ingest_path+'/'+data_file)

            #get table name form filename
            curr_data_name=data_file.split('.')[0]

            #apply appropiate columns
            df_with_cols = input_df.toDF(*col_names.get(curr_data_name))

            #sanitize col names
            renamed_columns = sanitize_columns(df_with_cols.columns)
            ingested_df = input_df.toDF(*renamed_columns)

            #check ingestion and save to ingested folder
            ingested_df.printSchema()
            ingested_df.show()
            ingested_df.write.mode("overwrite").parquet(transformation_path + '/' + curr_data_name)