# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 13:23:03 2021
transformation.py
Transformations on preprocessed Addidas case study data
@author: Poonam Dixit
"""

from pyspark.sql.functions import col,explode,avg


def harry_potter_books(spark, cleaned_data, output_path):
    """

    This will select all harry potter books from cleaned data
    ----------
    spark : spark session
    cleaned_data : source path for cleaned/preprocessed data
    output_path : traansformed data is written
    """
    harry_potter_book = cleaned_data.filter("title like '%Harry Potter%'")
    print("####### Harry Potter Book Data ######")
    harry_potter_book.show(truncate=False)
    write_path = output_path + "harry_potter_book"
    harry_potter_book.coalesce(1).write.parquet(write_path)
    

def book_with_most_pages(spark, cleaned_data):
    """

    This will select book with most pages
    ----------
    spark : spark session
    cleaned_data : source path for cleaned/preprocessed data
    """
    print("####### Book with Most Pages ######")
    cleaned_data.orderBy(col("number_of_pages").desc()).limit(1).show(truncate=False)
    
def authors_most_written_books(spark, cleaned_data):
    """

    This will select top 5authors with most written books
    ----------
    spark : spark session
    cleaned_data : source path for cleaned/preprocessed data
    """
    print("####### top 5 authors with Most written books ######")
    cleaned_data.groupBy("author").count().orderBy(col("count").desc())\
    .limit(5).show(truncate=False)
   
def genres_most_books(spark, cleaned_data):
    """

    This will select top 5 geners with most books
    ----------
    spark : spark session
    cleaned_data : source path for cleaned/preprocessed data
    """
    print("####### top 5 geners with Most books ######")
    cleaned_data.withColumn("genre", explode("genres")).drop("genres").\
        groupBy("genre").count().orderBy(col("count").desc()).limit(5).\
            show(truncate=False)
        
def avg_number_pages(spark, cleaned_data):
    """

    This will give average number of pages
    ----------
    spark : spark session
    cleaned_data : source path for cleaned/preprocessed data
    """
    print("####### average number of pages ######")
    cleaned_data.agg(avg("number_of_pages").alias("Average_pages")).show()
    
def authors_published_yearly(spark, cleaned_data, output_path):
    """

    This will give author number who published atleast one book per year
    ----------
    spark : spark session
    cleaned_data : source path for cleaned/preprocessed data
    """
    print("##### author number who published atleast one book per year ####")
    authors_published = cleaned_data.groupBy("publish_year", "author").count().\
        filter("count >= 1").drop("count").groupBy("publish_year").count().\
            orderBy(col("publish_year").desc()).\
                withColumnRenamed("count","number_of_authors")
    authors_published.show(10,truncate=False)           
    write_path = output_path + "authors_published_yearly"
    authors_published.coalesce(2).write.parquet(write_path)
    