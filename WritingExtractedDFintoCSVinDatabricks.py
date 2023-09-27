#exporting the data extract to a CSV file - 26 August Version
#This script will write Pandas Dataframe into a CSV file in Databricks

import traceback
import json
import sys
import logging
import yaml
from datetime import datetime,date
from datetime import timedelta
import os
# Custom Library
sys.path.insert(0,'/dbfs/dataintegration/process-files-dataflow_deltaLake/')
sys.path.insert(0,'/dbfs/dataintegration/process-files-common/')
import utilsShared

try:
  # Logger function
  logger = utilsShared.getFormattedLogger(__name__)
  
  # Read the env Config Properties
  envConfig = utilsShared.getEnvConf(logger,dbutils)


  spark.conf.set("spark.sql.crossJoin.enabled","true")
    
  extractDF = sql("select * from Majesco_Loss_Sentive_Policy")

  currentDateTime = datetime.now()

#extract storage local dbfs path
  extractLocalFolder = "/dbfs/mnt/extractMount/data/"
  extractdbfsFolder = "dbfs:/mnt/extractMount/data/"
  
  #<sourcename>_<LOB>_Quote/Policy_<yyyymmdd>.csv
  extractFilename = "majesco_loss_sensitive_policy_" + str(currentDateTime.strftime("%Y%m%d")) + ".csv" 
 
  pathWithFileName = extractLocalFolder + extractFilename 

  logger.info("Data Extract Local file path: " + pathWithFileName)
  
  logger.info("Writing the data in csv file: " + pathWithFileName)

  if extractDF.count() > 0 : 
    extractDF.toPandas().to_csv(pathWithFileName,index=False,header=True,sep=str(","),encoding="utf-8")
    logger.info("Data Extract completed... DBFS file path: " + extractdbfsFolder + extractFilename)
    #dbutils.notebook.exit(extractdbfsFolder + extractFilename)
    dbutils.notebook.exit(json.dumps({"status": "success", "file_name": extractFilename, "extractPathFolder": "", "columnDelimiter":","}))

except Exception as e:
  logger.error(str(traceback.print_exc()))
  status = "Dataextract Failed in majesco_loss_sensitive_policy -- "
  failure_reason = str(e)
  logger.error(status + failure_reason)
  traceback.print_exc(status)
  dbutils.notebook.exit(status + failure_reason)