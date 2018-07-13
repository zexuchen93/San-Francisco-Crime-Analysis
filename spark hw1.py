# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 01:43:24 2018

@author: zexu chen
"""

# explore the dataset
sf_data = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/sf_data.csv')

sf_data.take(10)

from pyspark.sql.functions import *
from pyspark.sql.types import *
sf_data.printSchema()
sf_data.show()

#Q1: number of crimes for different category
sf_data.groupBY("Category").count().show()

#Q2: number of crimes for different district
sf_data.groupBy("PdDistrict").count().show()

#Q3: number of crimes each sunday at SF downtown
#register the dataframe as a SQL temporary view
sf_data.createOrReplaceTempView("SD")
answer1 = spark.sql("SELECT * FROM SD WHERE DayOfWeek = 'Sunday' AND X BETWEEN -122.44 AND -122.42 AND Y BETWEEN 37.74 AND 37.76")
answer1.count()




