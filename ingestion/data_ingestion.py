# -*- coding: utf-8 -*-
"""
Created on Sun Dec 26 20:47:31 2021

@author: Poonam Dixit
"""

from pyspark.sql.functions import col, explode
import datetime


def load_source_data(spark, input_path):
    """

    Load source json data from open library using defined spark session 
    with path
    ----------
    spark : spark session
    Input_path : source json path
    """
    return spark.read.json(input_path)

def clean_source_data(spark, source_data, preprocessed_path):
    """
        clean source data to perfomr the right processing using below checks:
            1. publish date should be greater than 1950
            2. number of pages should be greater than 20
            3. title should not be empty or null
            4. genres should not be empty or null
            5. selecting required set of columns to address end user's queries
    ----------
    spark : spark session
    source_data : source_data to be cleaned up
    preprocessed_path : outputlocation for cleaned data
    """
    
    #filter records with given required filter checks
    clean_data =source_data.withColumn("author", explode("authors.key"))
    
    #populating current year to clean unwanted data on the basis of publish year
    now_time = datetime.datetime.now()
    current_year = now_time.year
    filter_str ="(title is not null or title != '') \
                                    and (author is not null or author != '') \
                                    and number_of_pages > 20 and \
                                    publish_date > 1950 and publish_date < {0} \
                                        and genres is not null".\
                                        format(current_year)
    filtered_data = clean_data.filter(filter_str)
    filtered_clean_data = filtered_data.select(col("title"), \
                                               col("author"), \
                                               col("publish_date").cast("Integer"), \
                                               col("number_of_pages"),\
                                                   col("genres")
                                               ).\
        withColumnRenamed("publish_date","publish_year")
    filtered_clean_data.coalesce(4).write.parquet(preprocessed_path)
    return filtered_clean_data
