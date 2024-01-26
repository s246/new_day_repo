import logging

import sys
from pyspark.sql import SparkSession

from data_transformations import movies_transform

LOG_FILENAME = 'project.log'
APP_NAME = "New Day Pipeline: Transform"

#main job func
if __name__ == '__main__':

    #write logs
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    #check if enoug args were send in job submission (2 paths)
    if len(sys.argv) != 3:
        logging.warning("Input source and output path are required")
        sys.exit(1)

    # create spark app ref an context
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)

    # get paths from arguments
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    #FOR DEBUG
    # input_path='../resources/ingest/'
    # output_path='../resources/transform/'

    # run spark job
    logging.info("Application Initialized: " + spark.sparkContext.appName)
    movies_transform.run(spark, input_path, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)

    spark.stop()
