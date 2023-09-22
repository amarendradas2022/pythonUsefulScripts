import builtins
import os
import sys
import logging
import yaml
import pyspark
import glob
from pyspark.sql import SQLContext,Row
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit,col,lower,trim,coalesce,upper,when

table_list=['fire_rating_record','homeowners_rating_rec']
for i in table_list:
    print(i)
    path = "/dbfs/mnt/cleanMount/gain/gain/"+i+"/20230921"
    newdir = path.replace("/dbfs", "")
    qdf= spark.read.parquet(newdir)
    qdf.printSchema()