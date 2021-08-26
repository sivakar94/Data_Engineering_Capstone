import pandas as pd
import os
import configparser
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import desc, when, lower, isnull
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
import datetime as dt
from datetime import timedelta, datetime


def clean_spark_immigration_data(df):
    """Clean immigration dataframe
    :param df: spark dataframe with monthly immigration data
    :return: clean dataframe
    """
    total_records_count = df.count()
    print(f'Total records in dataframe: {total_records_count:,}')
    
    # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them
    drop_columns = ["visapost", "occup", "entdepu", "insnum","count", "entdepa", "entdepd", "matflag", "dtaddto", "biryear", "admnum","fltno", "airline"]

    df = df.drop(*drop_columns)
    
    # drop rows where all elements are missing
    df = df.dropna(how='all')
    new_total_records = df.count()
    print(f'Total records after cleaning: {new_total_records:,}')
    
    return df

def process_temperature_data():
    
    temp_df= spark.read.csv("../../data2/GlobalLandTemperaturesByCity.csv",header=True, inferSchema=True)
    temp_df = temp_df.groupby(["Country"])
                            .agg({"AverageTemperature": "avg", "Latitude": "first", "Longitude": "first"})\
                            .withColumnRenamed('avg(AverageTemperature)', 'Temperature')\
                            .withColumnRenamed('first(Latitude)', 'Latitude')\
                            .withColumnRenamed('first(Longitude)', 'Longitude')
        
    temp_df = temp_df.withColumn('country', lower(temp_df.Country))
    temp_df.show(5)
    return temp_df    


def create_visa_dimension_table(immigration_df, output_data,visa_code_file):
    """
    This Function Creates visa dimension table
    
    :param df: spark dataframe of the immigration data
    :output: path to write visa dimension table
    :return: Visa dataframe
    """
    visa_code = spark.read.csv(visa_code_file, header=True, inferSchema=True)
    # Create Visa code view
    visa_code.createOrReplaceTempView("visa_code_view")
    # Create Immiogration view
    immigration_df.createOrReplaceTempView("immigration_view")
    
    # create country dimension using SQL
    visa=spark.sql(
        """
        SELECT 
        v.Stay_Purpose,
        i.visatype,
        i.Stay_period
        from 
        immigration_view as i
        JOIN visa_code_view as v
        on i.i94visa = v.code
        """
    ).distinct()
        
    visa_df = visa.withColumn('visa_key', monotonically_increasing_id())
    
    # write dimension to parquet file
    #visa_df.write.parquet(output_data + "visatype", mode="overwrite")

    return visa_df
    

def create_immigration_calendar_dimension(immigration_df, output_data):
     """
     This Function Creates visa dimension table
    
    :param df: spark dataframe of the immigration data
    :output: path to write visa dimension table
    :return: Visa dataframe
    """
        
    # The date format string preferred to our work here: YYYY-MM-DD
    date_format = "%Y-%m-%d"   
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # create initial calendar df from arrdate column
    calendar_df = df.select(['arrdate']).withColumn("arrdate", get_datetime(df.arrdate)).distinct()

    # expand df by adding other calendar columns
    calendar_df = calendar_df.withColumn('arrival_day', dayofmonth('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_week', weekofyear('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_month', month('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_year', year('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_weekday', dayofweek('arrdate'))

    # create an id field in calendar df
    calendar_df = calendar_df.withColumn('id_key', monotonically_increasing_id())
    
    # write dimension to parquet file
    #calendar_df.write.parquet(output_data + "visatype", mode="overwrite")
    
    return calendar_df



def create_country_dimension_table(spark, immigration_df, temperature_df, output_data, country_code_file):
    
    immigration_df.createOrReplaceTempView("immigration_view")
    
    file = input_data + country_code_file
    country_code = spark.read.csv(file, header=True, inferSchema=True)

    combined = country_code.join(temp_df, country_code.country == temperature_df.country, how="left")
    
    combined.createOrReplaceTempView("country_view")
    
    # load the i94res to country mapping data
    happiness_development = spark.read.csv("Lookup/happiness_and_development.csv", header=True, inferSchema=True)
    
    h_d.createOrReplaceTempView("H_D")
    
    # create country dimension using SQL
    Final_country_df=spark.sql(
        """
        SELECT 
        t.*,
        h.*
        from country_view as t
        LEFT JOIN H_D as h
        on t.country=h.country
        """
    ).distinct()

    # write the dimension to a parquet file
    Final_country_df.write.parquet(output_data + "country", mode="overwrite")

    return country_df

def create_immigration_fact_table(spark, immigration_df, output_data):
    """
     This Function Creates visa dimension table
    
    :param df: spark dataframe of the immigration data
    :output: path to write visa dimension table
    :return: Visa dataframe
    """
    date_format = "%Y-%m-%d"
    
    '''
    # rename columns to align with data model
    df = df.withColumnRenamed('cicid','record_id') \
            .withColumnRenamed('i94res', 'country_residence_code') \
    '''

    #User defined functions using Spark udf wrapper function to convert SAS dates into string dates in the format YYYY-MM-DD
    convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))
    
    df=df.withColumn("Arrival_date", convert_sas_udf(col("arrdate"))) 
    df=df.withColumn("Departure_date", convert_sas_udf(col("depdate")))
    
    date_diff_udf = udf(date_diff)
    
    df = df.withColumn('Stay_period', date_diff_udf(col('Arrival_date'), col('Departure_date')))
    
    df.write.parquet(output_data + "Immigration_Fact", mode="overwrite")
    
    return df
    
    
    
def date_diff(date1, date2):
    '''
    Calculates the difference in days between two dates
    '''
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, date_format)
        b = datetime.strptime(date2, date_format)
        delta = b - a
        return delta.days