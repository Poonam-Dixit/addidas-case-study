# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 13:52:12 2021
main.py

main program to run all use cases for addidas case study
@author: Poonam Dixit
"""

from pyspark.sql import SparkSession
from ingestion import data_ingestion
from transformation import data_transformations

if __name__ == "__main__":
    # defining path variables
    input_path = "file:///Addidas/input/ol_cdump.json"
    preprocessed_path = "file:///Addidas/preprocessed"
    output_path = "file:///Addidas/output/"
    
    #creating spark session
    print("### spark session created ####")
    spark=SparkSession.builder.appName("Addidas_case_study_solutions").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partition",4)
    
    #reading source data from json
    print("### reading source data ####")
    source_data = data_ingestion.load_source_data(spark, input_path)
    
    # cleaning / preprocessing source data 
    print("### cleaning source data ####")
    cleaned_data = data_ingestion.clean_source_data(spark, source_data, preprocessed_path)
    
    #performing data transformations as per the usecasess
    #getting harry potter book data
    print("### Addidas case study usecase solutions started ####")
    data_transformations.harry_potter_books(spark, cleaned_data, output_path)
    
    #getting book with most pages
    data_transformations.book_with_most_pages(spark,cleaned_data)
    
    #getting  top 5 authors with most written books
    data_transformations.authors_most_written_books(spark,cleaned_data)
    
    #getting  top 5 genres with most books
    data_transformations.genres_most_books(spark,cleaned_data)
    
    #getting  average number of pages
    data_transformations.avg_number_pages(spark,cleaned_data)
    
    #number of authors who published atleast one book in one publishing year
    data_transformations.authors_published_yearly(spark,cleaned_data, output_path)
    
