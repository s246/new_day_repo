# Project Name

Solution to the exercise for the application process at New Day

This project executes two spark jobs movies_ingest_job and movies_transform_job 

A test module is also included to follow best practices

## Table of Contents

- [Usage](#usage)
- [Usage](#Testing)


## Usage

It is assumed that necessary libs are locally installed (i.e spark, pyspark...)

First run to ingest data:
spark-submit --master local jobs/movies_ingest_job.py resources/ml-1m/ resources/ingest/

Second run to transform data:
spark-submit --master local jobs/movies_transform_job.py resources/ingest/ resources/transform/

## Testing
It is assumed that necessary libs are installed (i.e make)
To run the test module:

make tests
