# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 16:10:41 2023

@author: arnol
"""

#importing packages.

from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local")\
        .appName("test")\
            .getOrCreate()
            
data = [
        ('James', '', 'Smith', '1991-04-01','M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert','','Williams','1978-09-05','M',4000)
        ]         

columns = ["firstname", "middlename", "lastname","dob","gender","salary"]

df = spark.createDataFrame(data=data, schema=columns)   

print(df.printSchema())