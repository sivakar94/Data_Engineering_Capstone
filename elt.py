import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.functions import col, hour, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import monotonically_increasing_id

# Importing helper functions
from qualityTests import (check_data_type, check_greater_that_zero,
                          check_unique_keys, ensure_no_nulls)

import etl_functions

config = configparser.ConfigParser()
config.read('credentials.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def process_immigration_data(spark, input_data, output_data, file_name, temperature_file, mapping_file):
    """Process the immigration data input file and creates fact table and calendar, visa_type and country dimension tables.
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    file_name (string): immigration input file name
    mapping_file (pandas dataframe): dataframe that maps country codes to country names
    temperature_file (string): global temperatures input file name
    """
    # get the file path to the immigration data
    immigration_file = input_data + file_name

    # read immigration data file
    immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_file)

    # clean immigration spark dataframe
    immigration_df = preprocess_functions.clean_spark_immigration_data(immigration_df)

    # create visa_type dimension table
    visa_df = preprocess_functions.create_visa_dimension_table(immigration_df, output_data,visa_code_file)

    # create calendar dimension table
    calendar_df = etl_functions.create_immigration_calendar_dimension(immigration_df, output_data)

    # create country dimension table
    country_df = etl_functions.create_country_dimension_table(spark, immigration_df, temperature_df, output_data, country_code_file)

    temperature_df= etl_functions.create_temperature_dimension_table(temperature_file_name,country_code_file)
    
    # create immigration fact table
    fact_df = etl_functions.create_immigration_fact_table(spark, immigration_df, output_data)
    
    
        


def main():
    spark = create_spark_session()
    input_data = "s3://capstoneprojectsiva/"
    output_data = "s3://capstoneprojectsiva/"

    immigration_file_name = 'i94_apr16_sub.sas7bdat'
    temperature_file_name = 'GlobalLandTemperaturesByCity.csv'
    happiness_development_file_name = 'happiness_and_development.csv'

    country_code_file = input_data + "i94cit.csv"
    visa_code_file = input_data + "i94visa.csv"
    
    # load the i94res to country mapping data
    country_code = spark.read.csv(country_code_file, header=True, inferSchema=True)
    country_code = spark.read.csv(visa_code_file, header=True, inferSchema=True)
    
    process_immigration_data(spark, input_data, output_data, immigration_file_name, temperature_file_name, mapping_file)

    process_temp_data(spark, input_data, output_data, usa_demographics_file_name)


if __name__ == "__main__":
    main()