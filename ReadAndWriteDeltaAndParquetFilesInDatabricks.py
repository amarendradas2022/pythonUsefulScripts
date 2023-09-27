## Read and Write Parquet and Delta tables in Databricks
import time
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
#from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pyspark.sql.functions import lit,col,lower,trim,coalesce,upper,when
from pyspark.sql.functions import max
import hashlib
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from delta.tables import *
import logging
import yaml
import os

# Default Logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.DEBUG)
delta_path = "/mnt/mountdeltalake"


# REad the application config file
def getAppConf(obj=0):
	appConfPath= "/dbfs/dataintegration/conf/appConfig.yaml"   
	logger.info("Reading Application Config from: " + appConfPath)
	if os.path.isfile(appConfPath):
		appConfig= yaml.load(open(appConfPath,'r'),Loader=yaml.FullLoader)	
	else:
		logger.info("Application Config is not availabe " + str(appConfPath) + ", Please place the config file in order to execute the job")
		appConfig = {}
		return appConfig
	
	if obj == 0:
		return appConfig
	
	if "use-case" in obj and obj["use-case"] in ["DataMart","Datamart","datamart" ]:
		if "DataMart" in appConfig:
			appConfig.update(appConfig["DataMart"])
	elif "use-case" in obj and obj["use-case"] in ["DataVault","Data-Vault","datavault"] :
		if "DataVault" in appConfig:
			appConfig.update(appConfig["DataVault"])
	return appConfig
	
# Function to read from the sql server of datamart
def read_datamart(spark,tableName,obj,sourceDict=0):
	# Check if the additional filter is provided
	if isinstance(sourceDict,dict) and "read-type" in sourceDict:
		readType = str(sourceDict["read-type"])
	elif "read-type" in obj:
		readType = str(obj["read-type"])
	else:
		readType = "full"
	
	if "source-filter" in obj :
		sourceName = str(obj["source-filter"])
	elif isinstance(sourceDict,dict) and "source-filter" in sourceDict :
		sourceName = str(sourceDict["source-filter"])
		
	if "source-filter-col" in obj :
		sourceFilterCol = str(obj["source-filter-col"])
	elif isinstance(sourceDict,dict) and "source-filter-col" in sourceDict :
		sourceFilterCol = str(sourceDict["source-filter-col"])
	else:
		sourceFilterCol = "RECORD_SOURCE_NAME"
		
	if "load-end-key" in obj:
		loadEndKey = str(obj["load-end-key"])
	elif isinstance(sourceDict,dict) and "load-end-key" in sourceDict:
		loadEndKey = str(sourceDict["load-end-key"])
	else:
		loadEndKey = "RECORD_END_DATE"
	

	tableSpltList = str(tableName).split(".")
	if len(tableSpltList) > 1 :
		tableName1 =  tableSpltList[1].lower()
	else:
		tableName1 = tableName	
	
	if "dm_schema" in obj:
		schema = str(obj["dm_schema"])
		logger.debug("Using schema : "+str(schema))
		tableName1 = schema + "."  + tableName1
	elif len(tableSpltList) > 1 :
		schema = str(tableName).split(".")[0]
		tableName1 = tableName #schema + "."  + tableName1
	else:
		schema = ''		
	excludeSchemaList=["dbo","bau","abc","anlt","rdm","rheatvadb","sys"]
	# Skip the tables which are not for mart to be processed via new driver
	if not (tableSpltList[1].lower().startswith("fact_") or tableSpltList[1].lower().startswith("dim_") or tableSpltList[1].lower().startswith("flf_")) :
		schema = 'dbo'

	if schema.lower() in excludeSchemaList:
		logger.info("Reading Mart table without partition method")
		if readType == "rec-source-filtered":
			sourceDF = spark.read.format("jdbc") \
			.option("url", str(str(obj['sqlserver']['connection-string']))) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
			.load().where((col(sourceFilterCol)==str(sourceName)))

		elif readType == "rec-source-filtered-active":  # Reac source filteed with filter of active record
			sourceDF = spark.read.format("jdbc") \
			.option("url", str(str(obj['sqlserver']['connection-string']))) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
			.load().where((col(sourceFilterCol)==str(sourceName)) & (col(loadEndKey).isNull()))
	
		elif readType == "active-filtered":  # Reac source filteed with filter of active record
			sourceDF = spark.read.format("jdbc") \
			.option("url", str(str(obj['sqlserver']['connection-string']))) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
			.load().where(col(loadEndKey).isNull())
		
		else: # Full load
			sourceDF = spark.read.format("jdbc") \
			.option("url", str(str(obj['sqlserver']['connection-string']))) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()		
		
	else:
		logger.info("Reading Mart table using partition method")
		tableSplt = str(tableName).split(".")
		if len(tableSplt) > 1 :
			part_col_name =  tableSplt[1].lower()
		else:
			part_col_name = tableName

		part_col = getPartitionCol_DM(part_col_name)	
		logger.info("Reading DM table " + part_col_name)
		logger.debug("Partition column : "+part_col)
	
		if readType == "rec-source-filtered":
			sourceDF = spark.read.format("jdbc") \
			.option("url", str(str(obj['sqlserver']['connection-string']))) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
			.option("numPartitions", "20").option("partitionColumn", part_col) \
			.option("lowerBound", "0") \
			.option("upperBound", "10000000") \
			.load().where((col(sourceFilterCol)==str(sourceName)))

		elif readType == "rec-source-filtered-active":  # Reac source filteed with filter of active record
			sourceDF = spark.read.format("jdbc") \
			.option("url", str(str(obj['sqlserver']['connection-string']))) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
			.option("numPartitions", "20").option("partitionColumn", part_col) \
			.option("lowerBound", "0") \
			.option("upperBound", "10000000") \
			.load().where((col(sourceFilterCol)==str(sourceName)) & (col(loadEndKey).isNull()))
	
		elif readType == "active-filtered":  # Reac source filteed with filter of active record
			sourceDF = spark.read.format("jdbc") \
			.option("url", str(str(obj['sqlserver']['connection-string']))) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
			.option("numPartitions", "20").option("partitionColumn", part_col) \
			.option("lowerBound", "0") \
			.option("upperBound", "10000000") \
			.load().where(col(loadEndKey).isNull())
		
		else : # Full load
			sourceDF = spark.read.format("jdbc") \
			.option("url", str(str(obj['sqlserver']['connection-string']))) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
			.option("numPartitions", "20").option("partitionColumn", part_col) \
			.option("lowerBound", "0") \
			.option("upperBound", "10000000") \
			.load()
		
	return sourceDF
	
# Function to write the dataframe to SQL server in datamart
def write_datamart(hashDF,tableName,envConfig,mode="append"):
	if str(mode) != "append" or str(mode) != "overwrite":
		mode = "append"
		
	tableSpltList = str(tableName).split(".")
	if len(tableSpltList) > 1 :
		tableName1 =  tableSpltList[1].lower()
	else:
		tableName1 = tableName
		
	if "dm_schema" in envConfig:
		schema = str(envConfig["dm_schema"])
		logger.debug("Using schema : "+str(schema))
		tableName1 = schema + "."  + tableName1
	elif len(tableSpltList) > 1 :
		schema = str(tableName).split(".")[0]
		tableName1 = tableName #schema + "."  + tableName1
	else:
		schema = ''

   # Skip the tables which are not for mart to be processed via new driver
	if not (tableSpltList[1].lower().startswith("fact_") or tableSpltList[1].lower().startswith("dim_") or tableSpltList[1].lower().startswith("flf_")) :
		schema = 'dbo'
        
	logger.info("Writing table " + tableName1 )
	excludeSchemaList=["dbo","bau","abc","anlt","rdm","rheatvadb","sys"]
	if schema.lower() in excludeSchemaList:
		hashDF.write \
			.format("jdbc") \
			.option("url", str(envConfig['sqlserver']['connection-string'])) \
			.option("dbtable", str(tableName1)) \
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
			.option("numPartitions",10) \
			.mode(str(mode)) \
			.save()
	else:
		hashDF.write \
			.format("com.microsoft.sqlserver.jdbc.spark") \
			.mode("append") \
			.option("url", str(envConfig['sqlserver']['connection-string'])) \
			.option("dbtable", str(tableName1)) \
			.option("batchsize", 100000) \
			.option("mssqlIsolationLevel", "READ_UNCOMMITTED") \
			.option("tableLock","true") \
			.option("reliabilityLevel","BEST_EFFORT") \
			.option("schemaCheckEnabled","false") \
			.save()
		
# Read data from atomic table
def read_raw(spark,obj,logger,sourceDict=0):

	if "infer-schema-flag" in sourceDict:
		inferSchemaFlag = str(sourceDict["infer-schema-flag"])
	else:
		inferSchemaFlag = "true"
	logger.debug("Infer schema flag is  :" + inferSchemaFlag)	
	if str(obj["fileName"])=="":
		dynamicInputPath = str(obj['azure']['raw-base-path']) + "/" + str(obj["sourceName"]) + "/" +  str(obj["schemaName"]) + "/" + str(obj["tableName"]) + "/" +  str(obj["tableName"]) + "_"	+ obj["batchDate"] + "." + str(sourceDict["file-format"])	
	else:
		dynamicInputPath = str(obj['azure']['raw-base-path']) + "/" + str(obj["sourceName"]) + "/" +  str(obj["schemaName"]) + "/" + str(obj["tableName"]) + "/" +  str(obj["fileName"])
		
	logger.debug("Dynamic Input Path is: " + str(dynamicInputPath))
	if str(sourceDict["file-format"]) == "csv":
		inputDF = spark.read.format("csv").option("inferSchema", inferSchemaFlag) \
				.option("header",sourceDict["header"]) \
				.option("delimiter",sourceDict['delimiter']) \
				.load(dynamicInputPath)
	elif str(sourceDict["file-format"]) == "xml":
		# Storage read from Blob storage
		inputDF = spark.read.format("com.databricks.spark.xml").option("rowTag", str(sourceDict['xmlRowTag'])) \
			.option("valueTag", "ValueTagElement") \
			.load(dynamicInputPath)
	# Updating the Column names to remove special character not allowed in parquet
	inputDF = inputDF.toDF(*(re.sub(r'[\,\s;\n\t\={}()]+', '_', c) for c in inputDF.columns))
	
	return inputDF


# Write the data in clean zone
def write_clean(HASH_TABLE,spark,obj,logger):
	
	# Write mode identification
	if "mode-of-write" in obj:
		writeMode = str(obj["mode-of-write"])
	else:
		writeMode = "overwrite"
		
	# Target Format identification
	if "target-format" in obj:
		writeFormat = str(obj['target-format'])
	else:
		writeFormat = "parquet"
	
	output = str(obj['azure']['clean-base-path']) + "/" + str(obj["sourceName"]) + "/" +  str(obj["schemaName"]) + "/" + str(obj["tableName"]) +"/"+ obj["batchDate"]
	logger.debug("Writing file at :" + output + " with mode " + writeMode + " in format :" + writeFormat)
	HASH_TABLE.write.mode(str(writeMode)).save(output, format=writeFormat)

# Read data from atomic table
def read_atomic(spark,tableName,obj,sourceDict=0,exportFlag='false'):

	# Check if the additional filter is provided
	if isinstance(sourceDict,dict) and "read-type" in sourceDict:
		readType = str(sourceDict["read-type"])
	elif "read-type" in obj:
		readType = str(obj["read-type"])
	else:
		readType = "full"

	if "historicalFlag" in obj and obj["historicalFlag"] == "true":
		readType = "historical"

	if "source-filter" in obj :
		sourceName = str(obj["source-filter"])
	elif isinstance(sourceDict,dict) and "source-filter" in sourceDict :
		sourceName = str(sourceDict["source-filter"])
	else:
		sourceName = "NA"
		
	if "source-filter-col" in obj :
		sourceFilterCol = str(obj["source-filter-col"])
	elif isinstance(sourceDict,dict) and "source-filter-col" in sourceDict :
		sourceFilterCol = str(sourceDict["source-filter-col"])
	else:
		sourceFilterCol = "REC_SRC_NM"
		
	if "load-end-key" in obj:
		loadEndKey = str(obj["load-end-key"])
	elif isinstance(sourceDict,dict) and "load-end-key" in sourceDict:
		loadEndKey = str(sourceDict["load-end-key"])
	else:
		loadEndKey = "LD_END_DT"
	
	appConf = getAppConf(obj)
	if "rec-source-list" in appConf:
		recSourceList = appConf["rec-source-list"]
	else:
		recSourceList = []
	
	if sourceName != "NA" and (readType == "rec-source-filtered" or readType == "historical" or exportFlag == "true"):
		sourceDF = spark.read \
				.format("org.apache.phoenix.spark") \
				.option("table", str(tableName)) \
				.option("zkUrl", str(obj["hdi"]["zkurl"])) \
				.load().where(col(sourceFilterCol)==str(sourceName))

	elif readType == "rec-source-filtered-active":  # Reac source filteed with filter of active record
		sourceDF = spark.read \
				.format("org.apache.phoenix.spark") \
				.option("table", str(tableName)) \
				.option("zkUrl", str(obj["hdi"]["zkurl"])) \
				.load().where((col(sourceFilterCol)==str(sourceName)) & (col(loadEndKey).isNull()))
				
	elif readType == "rec-source-filtered-list":
		sourceDF = spark.read \
				.format("org.apache.phoenix.spark") \
				.option("table", str(tableName)) \
				.option("zkUrl", str(obj["hdi"]["zkurl"])) \
				.load().where(col(sourceFilterCol).isin(recSourceList))

	elif readType == "rec-source-filtered-list-active":  # Reac source filteed with filter of active record
		sourceDF = spark.read \
				.format("org.apache.phoenix.spark") \
				.option("table", str(tableName)) \
				.option("zkUrl", str(obj["hdi"]["zkurl"])) \
				.load().where((col(sourceFilterCol).isin(recSourceList)) & (col(loadEndKey).isNull()))
	
	elif readType == "active-filtered":  # Reac source filteed with filter of active record
		sourceDF = spark.read \
				.format("org.apache.phoenix.spark") \
				.option("table", str(tableName)) \
				.option("zkUrl", str(obj["hdi"]["zkurl"])) \
				.load().where((col(loadEndKey).isNull()))
	else : # Full load
		sourceDF = spark.read \
				.format("org.apache.phoenix.spark") \
				.option("table", str(tableName)) \
				.option("zkUrl", str(obj["hdi"]["zkurl"])) \
				.load()	
												
	return sourceDF	

# Read data from atomic table
def read_delta(spark,tableName,obj,sourceDict=0,exportFlag='false'):

	# Check if the additional filter is provided
	logger.info("Reading delta atomic table")
	if isinstance(sourceDict,dict) and "read-type" in sourceDict:
		readType = str(sourceDict["read-type"])
	elif "read-type" in obj:
		readType = str(obj["read-type"])
	else:
		readType = "full"

	if "historicalFlag" in obj and obj["historicalFlag"] == "true":
		readType = "historical"

	if "source-filter" in obj :
		sourceName = str(obj["source-filter"])
	elif isinstance(sourceDict,dict) and "source-filter" in sourceDict :
		sourceName = str(sourceDict["source-filter"])
	else:
		sourceName = "NA"
		
	if "source-filter-col" in obj :
		sourceFilterCol = str(obj["source-filter-col"])
	elif isinstance(sourceDict,dict) and "source-filter-col" in sourceDict :
		sourceFilterCol = str(sourceDict["source-filter-col"])
	else:
		sourceFilterCol = "REC_SRC_NM"
		
	if "load-end-key" in obj:
		loadEndKey = str(obj["load-end-key"])
	elif isinstance(sourceDict,dict) and "load-end-key" in sourceDict:
		loadEndKey = str(sourceDict["load-end-key"])
	else:
		loadEndKey = "LD_END_DT"
	
	appConf = getAppConf(obj)
	if "rec-source-list" in appConf:
		recSourceList = appConf["rec-source-list"]
	else:
		recSourceList = []

	#below params added for mart inc load	
	if "num_of_days" in obj and obj["num_of_days"] != "" and obj["num_of_days"] != "NA":
		num_of_days = int(obj["num_of_days"])
		start_dt = (datetime.now() - timedelta(days=num_of_days)).strftime('%Y-%m-%d')
		end_dt = (datetime.now()).strftime('%Y-%m-%d')
		#logger.debug("Running for : "+str(start_dt) +" and "+str(end_dt))		
	elif "start_dt" in obj and obj["start_dt"] != "" and obj["start_dt"] != "NA" and "end_dt" in obj and obj["end_dt"] != "" and obj["end_dt"] != "NA":
		start_dt = str(obj["start_dt"])
		end_dt = str(obj["end_dt"])
		#logger.debug("Running for : "+str(start_dt) +" and "+str(end_dt))
	else:
		start_dt = "NA"		 
		end_dt = "NA"
		#logger.debug("Running for : "+str(start_dt) +" and "+str(end_dt))
	
	if "adv_schema" in obj:
		schema = str(obj["adv_schema"])
		logger.debug("Using schema : "+str(schema))
	else:
		schema = str(tableName).split(".")[0]

	# logger.info("USING SCHEMA : "+schema)
	# delta_path = "/mnt/mountdeltalake/"
	# schema = str(tableName).split(".")[0]
	tableSpltList = str(tableName).split(".")
	if len(tableSpltList) > 1 :
		table = tableSpltList[1].lower()
	else:
		table = tableName
	
	logger.info("Reading table " + schema + "." + table)
	delta_table_path = delta_path + "/" + schema.upper() + "/" + table
	
	if sourceName != "NA" and (readType == "rec-source-filtered" or readType == "historical" or exportFlag == "true"):
		logger.info("Reading target table with source filter")
		sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where(col(sourceFilterCol)==str(sourceName))

	elif readType == "rec-source-filtered-active":  # Reac source filteed with filter of active record
		sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(sourceFilterCol)==str(sourceName)) & (col(loadEndKey).isNull()))
				
	elif readType == "rec-source-filtered-list":
		sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where(col(sourceFilterCol).isin(recSourceList))

	elif readType == "rec-source-filtered-list-active":  # Reac source filteed with filter of active record
		sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(sourceFilterCol).isin(recSourceList)) & (col(loadEndKey).isNull()))
	
	elif readType == "active-filtered":  # Reac source filteed with filter of active record
		sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(loadEndKey).isNull()))	
	#below read type added for mart inc load
	elif readType == "active-filtered-with-loaddate":
		if start_dt == "NA" and end_dt == "NA":
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(loadEndKey).isNull()))
		else :
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(loadEndKey).isNull()) & ((col("LD_DT").cast("date")>=start_dt) & (col("LD_DT").cast("date")<=end_dt)))			
	elif readType == "loaddate-filtered":
		if start_dt == "NA" and end_dt == "NA":
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path)
		else :
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where(((col("LD_DT").cast("date")>=start_dt) & (col("LD_DT").cast("date")<=end_dt)))
	elif readType == "rec-source-filtered-with-loaddate":  
		if start_dt == "NA" and end_dt == "NA":
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where(col(sourceFilterCol)==str(sourceName))
		else :
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(sourceFilterCol)==str(sourceName)) & ((col("LD_DT").cast("date")>=start_dt) & (col("LD_DT").cast("date")<=end_dt)))
	elif readType == "rec-source-filtered-list-active-with-loaddate":  # Reac source filteed with filter of active record
		if start_dt == "NA" and end_dt == "NA":
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(sourceFilterCol).isin(recSourceList)) & (col(loadEndKey).isNull()))
		else :
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(sourceFilterCol).isin(recSourceList)) & (col(loadEndKey).isNull()) & ((col("LD_DT").cast("date")>=start_dt) & (col("LD_DT").cast("date")<=end_dt)))
	elif readType == "rec-source-filtered-active-with-loaddate":  # Reac source filteed with filter of active record
		if start_dt == "NA" and end_dt == "NA":
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(sourceFilterCol)==str(sourceName)) & (col(loadEndKey).isNull()))
		else :
			sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col(sourceFilterCol)==str(sourceName)) & (col(loadEndKey).isNull()) & ((col("LD_DT").cast("date")>=start_dt) & (col("LD_DT").cast("date")<=end_dt)))
	else : # Full load
		sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path)	
												
	return sourceDF	


def write_atomic(HASH_TABLE,spark,obj):
	#  Write table to Atomic -phoenix
	HASH_TABLE.write \
		.format("org.apache.phoenix.spark") \
		.mode(str(obj["mode-of-write"])) \
		.option("table", str(obj["target-table"])) \
		.option("zkUrl", str(obj["hdi"]["zkurl"])) \
		.save()

def write_delta(HASH_TABLE,spark,obj):
    #  Write table to Delta table
    tableName = str(obj["target-table"])
    if "adv_schema" in obj:
        schema = str(obj["adv_schema"])
        logger.debug("Using schema : "+str(schema))
    else:
        schema = str(tableName).split(".")[0]
	
    # logger.info(schema)
    if "partition_col" in obj:
        partition_col = obj["partition_col"]
        if isinstance(partition_col, list):
            pass
        elif isinstance(partition_col, str):
            partition_col = str(partition_col)
    else:
        partition_col = ["REC_SRC_NM","PART_COL"]

    logger.debug("Using partition_col : "+str(partition_col))
    # schema = str(tableName).split(".")[0]
    table = str(tableName).split(".")[1].lower()
    delta_table_path = delta_path + "/" + schema.upper() + "/" + table
    logger.info("Writing table " + schema + "." + table)

    # print(HASH_TABLE.columns)
    HASH_TABLE.write \
      .format("delta") \
      .mode("append") \
      .partitionBy(partition_col) \
      .save(delta_table_path)

def update_delta(HASH_TABLE,spark,obj,loadEndKey,loadEndValue):
	#  Write table to Delta table
	tableName = str(obj["target-table"])
	if "adv_schema" in obj:
		schema = str(obj["adv_schema"])
		logger.debug("Using schema : "+str(schema))
	else:
		schema = str(tableName).split(".")[0]
	
	# logger.info(schema)
	logger.info("Updating target table")
	partition_col = "REC_SRC_NM"
	partition_col2 = "PART_COL"
	part_col_value = []
	try:
		for c in HASH_TABLE.columns:
			if "PART_COL" == c.upper():
				part_col_value_temp = list(set(HASH_TABLE.rdd.map(lambda z : z.PART_COL).collect()))
				part_col_value = part_col_value + part_col_value_temp
	except:
		pass

	if "source-filter" in obj:
		source_filter = str(obj["source-filter"])
	else:
		source_filter = ''

	table = str(tableName).split(".")[1].lower()
	delta_table_path = delta_path + "/" + schema.upper() + "/" + table
	
	if "keys-to-identify-unique" in obj["scd2"]:
		update_key_cols = obj["scd2"]["keys-to-identify-unique"]["key-list"] + ['LD_DT','BTCH_DT']
	elif 'REF_ID' in HASH_TABLE.columns:
		update_key_cols = obj["scd2"]["target-table-keys"] + ['REF_ID','LD_DT','BTCH_DT']
	else:
		update_key_cols = obj["scd2"]["target-table-keys"] + ['LD_DT','BTCH_DT']
	
	
	update_condition = ""
	if len(part_col_value) == 1 and (part_col_value[0] != '' or part_col_value[0] is not None):
		for colname in update_key_cols:
			if update_condition == "" and source_filter != '':
				update_condition = "deltaTable."+ partition_col + "= '" + source_filter + "' and deltaTable."+ partition_col2 + "= '" + str(part_col_value[0]) + "' and deltaTable." + colname + " <=> updatedTable." + colname
			elif update_condition == "" : 
				update_condition = "deltaTable."+ colname + " <=> updatedTable." + colname
			else:
				update_condition = update_condition + " and deltaTable." + colname + " <=> updatedTable." + colname
	else:
		for colname in update_key_cols:
			if update_condition == "" and source_filter != '':
				update_condition = "deltaTable."+ partition_col + "= '" + source_filter + "' and deltaTable." + colname + " <=> updatedTable." + colname
			elif update_condition == "" : 
				update_condition = "deltaTable."+ colname + " <=> updatedTable." + colname
			else:
				update_condition = update_condition + " and deltaTable." + colname + " <=> updatedTable." + colname

	## Update only the open records in target table
	update_condition = update_condition + " and deltaTable." + loadEndKey + " is null"

	DELTA_TABLE = DeltaTable.forPath(spark,delta_table_path)
	DELTA_TABLE.alias("deltaTable").merge(HASH_TABLE.alias("updatedTable"), update_condition).whenMatchedUpdate(set = { loadEndKey : lit(loadEndValue).cast("timestamp") }).whenNotMatchedInsertAll().execute()

def upsert_delta(HASH_TABLE,spark,obj,loadEndKey,loadEndValue,colListToInsert):
	#  Write table to Delta table
	tableName = str(obj["target-table"])
	if "adv_schema" in obj:
		schema = str(obj["adv_schema"])
		logger.debug("Using schema : "+str(schema))
	else:
		schema = str(tableName).split(".")[0]
	
	# logger.info(schema)
	logger.info("Updating target table")
	partition_col = "REC_SRC_NM"
	partition_col2 = "PART_COL"
	part_col_value = []
	try:
		for c in HASH_TABLE.columns:
			if "PART_COL" == c.upper():
				part_col_value_temp = list(set(HASH_TABLE.rdd.map(lambda z : z.PART_COL).collect()))
				part_col_value = part_col_value + part_col_value_temp
	except:
		pass
	
	if "source-filter" in obj:
		source_filter = str(obj["source-filter"])
	else:
		source_filter = ''

	table = str(tableName).split(".")[1].lower()
	delta_table_path = delta_path + "/" + schema.upper() + "/" + table
	if "keys-to-identify-unique" in obj["scd2"]:
		update_key_cols = obj["scd2"]["keys-to-identify-unique"]["key-list"] + ['LD_DT','BTCH_DT']
	elif 'REF_ID' in HASH_TABLE.columns:
		update_key_cols = obj["scd2"]["target-table-keys"] + ['REF_ID','LD_DT','BTCH_DT']
	else:
		update_key_cols = obj["scd2"]["target-table-keys"] + ['LD_DT','BTCH_DT']

	if "historicalFlag" in obj and obj["historicalFlag"] == "true" and table[:3] == 'sat':
		if 'REF_ID' in HASH_TABLE.columns:
			update_key_cols = update_key_cols + ["REF_ID"]
		
		if 'SRC_EFF_DT' in HASH_TABLE.columns:
			update_key_cols = update_key_cols + ["SRC_EFF_DT"]
	
	update_key_cols  = list(dict.fromkeys(update_key_cols))

	update_condition = ""
	addCond = ""
	setColConditionDict = {}
	setUpdateConditionDict = {}
	colsToUpdate = [loadEndKey]
	colListToInsert.remove("rankColumn")
	# colListToInsert2 = set(HASH_TABLE.columns).intersection(colListToInsert)
	if len(part_col_value) == 1 and (part_col_value[0] != '' or part_col_value[0] is not None):
		for colname in update_key_cols:
			if update_condition == "" and source_filter != '':
				update_condition = "deltaTable."+ partition_col + "= '" + source_filter + "' and deltaTable."+ partition_col2 + "= '" + str(part_col_value[0]) + "' and deltaTable." + colname + " <=> updatedTable." + colname
			elif update_condition == "" : 
				update_condition = "deltaTable."+ colname + " <=> updatedTable." + colname
			else:
				update_condition = update_condition + " and deltaTable." + colname + " <=> updatedTable." + colname
	else:
		for colname in update_key_cols:
			if update_condition == "" and source_filter != '':
				update_condition = "deltaTable."+ partition_col + "= '" + source_filter + "' and deltaTable." + colname + " <=> updatedTable." + colname
			elif update_condition == "" : 
				update_condition = "deltaTable."+ colname + " <=> updatedTable." + colname
			else:
				update_condition = update_condition + " and deltaTable." + colname + " <=> updatedTable." + colname

	
	## Update only the open records in target table
	if "historicalFlag" not in obj or ("historicalFlag" in obj and obj["historicalFlag"] == "true" and table[:3] != 'sat'):
		update_condition = update_condition + " and deltaTable." + loadEndKey + " is null"

	for colname in colListToInsert:
		setColConditionDict[colname] = "updatedTable."+colname
	
	for colName in colsToUpdate:
		setUpdateConditionDict[colName] = "updatedTable."+colName
	
	DELTA_TABLE = DeltaTable.forPath(spark,delta_table_path)
	if "scd2-column" in obj["scd2"]:
		for keyName in sorted(obj["scd2"]["scd2-column"].keys()):
			colname = obj["scd2"]["scd2-column"][keyName]["colName"]
			setUpdateConditionDict[colname] = "updatedTable."+colname

		DELTA_TABLE.alias("deltaTable").merge(HASH_TABLE.alias("updatedTable"), update_condition).whenMatchedUpdate(set = setUpdateConditionDict).whenNotMatchedInsert(values = setColConditionDict).execute()
	else:
		DELTA_TABLE.alias("deltaTable").merge(HASH_TABLE.alias("updatedTable"), update_condition).whenMatchedUpdate(set =setUpdateConditionDict).whenNotMatchedInsert(values = setColConditionDict).execute()


def update_delta_historical(HASH_TABLE,spark,obj,colListToUpdate,updateKeyCols =[]):
	#  Write table to Delta table
	tableName = str(obj["target-table"])
	if "adv_schema" in obj:
		schema = str(obj["adv_schema"])
		logger.debug("Using schema : "+str(schema))
	else:
		schema = str(tableName).split(".")[0]
	
	# logger.info(schema)
	logger.info("Updating target table")
	partition_col = "REC_SRC_NM"
	partition_col2 = "PART_COL"
	part_col_value = []
	try:
		for c in HASH_TABLE.columns:
			if "PART_COL" == c.upper():
				part_col_value_temp = list(set(HASH_TABLE.rdd.map(lambda z : z.PART_COL).collect()))
				part_col_value = part_col_value + part_col_value_temp
	except:
		pass
	
	if "source-filter" in obj:
		source_filter = str(obj["source-filter"])
	else:
		source_filter = ''

	table = str(tableName).split(".")[1].lower()
	delta_table_path = delta_path + "/" + schema.upper() + "/" + table
	
	if len(updateKeyCols) != 0:
		update_key_cols = updateKeyCols
	elif "keys-to-identify-unique" in obj["scd2"]:
		update_key_cols = obj["scd2"]["keys-to-identify-unique"]["key-list"] + ['LD_DT']
	else:
		update_key_cols = obj["scd2"]["target-table-keys"] + ['LD_DT']	
	
	if 'REF_ID' in HASH_TABLE.columns:
		update_key_cols = update_key_cols + ['REF_ID']

	if "SRC_EFF_DT" in HASH_TABLE.columns:
		update_key_cols = update_key_cols + ["SRC_EFF_DT"]

	if "historicalFlag" in obj and obj["historicalFlag"] == "true" and "sourceName" in obj and obj["sourceName"].lower() in ["majesco"]:
			if "BTCH_DT" in HASH_TABLE.columns:
				update_key_cols = update_key_cols + ["BTCH_DT"]
	
	update_key_cols = list(dict.fromkeys(update_key_cols))

	if "historicalFlag" in obj and obj["historicalFlag"] == "true":
		HASH_TABLE = HASH_TABLE.dropDuplicates(update_key_cols)

	update_condition = ""
	setColConditionDict = {}

	if len(part_col_value) == 1 and (part_col_value[0] != '' or part_col_value[0] is not None):
		for colname in update_key_cols:
			if update_condition == "" and source_filter != '':
				update_condition = "deltaTable."+ partition_col + "= '" + source_filter + "' and deltaTable."+ partition_col2 + "= '" + str(part_col_value[0]) + "' and deltaTable." + colname + " <=> updatedTable." + colname
			elif update_condition == "" : 
				update_condition = "deltaTable."+ colname + " <=> updatedTable." + colname
			else:
				update_condition = update_condition + " and deltaTable." + colname + " <=> updatedTable." + colname
	else:
		for colname in update_key_cols:
			if update_condition == "" and source_filter != '':
				update_condition = "deltaTable."+ partition_col + "= '" + source_filter + "' and deltaTable." + colname + " <=> updatedTable." + colname
			elif update_condition == "" : 
				update_condition = "deltaTable."+ colname + " <=> updatedTable." + colname
			else:
				update_condition = update_condition + " and deltaTable." + colname + " <=> updatedTable." + colname


	for colname in colListToUpdate:
		if colname not in ["REC_SRC_NM","PART_COL"]:
			setColConditionDict[colname] = "updatedTable."+colname

	DELTA_TABLE = DeltaTable.forPath(spark,delta_table_path)

	DELTA_TABLE.alias("deltaTable").merge(
		HASH_TABLE.alias("updatedTable"), update_condition
	).whenMatchedUpdate(set = setColConditionDict 
	).execute() 


################# REF_DATA_LOOK_UP TABLE #STANDARDIZATION ###########
def read_ref_data_look_up_atomic(spark,obj,tableName):
	#Read Atomic Table from phoenix for XFRMTN
	ref_data_df = spark.read \
				.format("org.apache.phoenix.spark") \
				.option("table", str(tableName)) \
				.option("zkUrl", str(obj["hdi"]["zkurl"])) \
				.load()
	
	return ref_data_df
	
def read_ref_data_look_up_delta(spark,obj,tableName,isActive = 1):
	#Read Atomic Table from phoenix for XFRMTN
	if "adv_schema" in obj:
		schema = str(obj["adv_schema"])
		logger.debug("Using schema : "+str(schema))
	else:
		schema = str(tableName).split(".")[0]
	
	table = str(tableName).split(".")[1].lower()
	delta_table_path = delta_path + "/" + schema.upper() + "/" + table
	ref_data_df = spark.read \
				.format("delta") \
				.load(delta_table_path).where((col('IS_ACTV_FLG')==str(isActive)))
	
	return ref_data_df
	
######################################## END ################################

def ref_look_up_list(spark,obj,ref):
	#Read Atomic Table from phoenix for REF_LOOK_UP
	tableName = str(ref["ref-table-name"])
	if "adv_schema" in obj:
		schema = str(obj["adv_schema"])
		logger.debug("Using schema : "+str(schema))
	else:
		schema = str(tableName).split(".")[0]
	
	table = str(tableName).split(".")[1].lower()
	delta_table_path = delta_path + "/" + schema.upper() + "/" + table

	# REF_TABLE = spark.read \
	# 				.format("org.apache.phoenix.spark") \
	# 				.option("table", str(ref["ref-table-name"])) \
	# 				.option("zkUrl", str(obj["hdi"]["zkurl"])) \
	# 				.load()	
	
	REF_TABLE = spark.read \
					.format("delta") \
					.load(delta_table_path)	

	for refColName in ref["ref-look-up-values"].keys():
		REF_TABLE = REF_TABLE.filter(lower(col(refColName)) == str(ref["ref-look-up-values"][refColName]).lower())
		#refColName

	#return REF_TABLE
	 # Obtain the list of values from the return look up col
	returnList1 = REF_TABLE.select(ref["return-look-up-col"]).collect()

	if "return-look-up-col-const" in ref:
		lenConst = int(ref["return-look-up-col-const"]["length"])
		padValue = str(ref["return-look-up-col-const"]["rpad"])
		returnList = [str(row[ref["return-look-up-col"]]).strip().rjust(lenConst,str(padValue)) for row in returnList1]
	else:
		returnList = [str(row[ref["return-look-up-col"]]) for row in returnList1]

	#print returnList[1:25]
	return returnList

def update_expiring_records(HASH_TABLE,spark,obj,loadEndKey,loadEndValue,colListToInsert):
	#  Write table to Delta table
	tableName = str(obj["target-table"])
	if "adv_schema" in obj:
		schema = str(obj["adv_schema"])
		logger.debug("Using schema : "+str(schema))
	else:
		schema = str(tableName).split(".")[0]
	
	# logger.info(schema)
	logger.info("Updating target table")
	partition_col = "REC_SRC_NM"
	partition_col2 = "PART_COL"
	part_col_value = []
	try:
		for c in HASH_TABLE.columns:
			if "PART_COL" == c.upper():
				part_col_value_temp = list(set(HASH_TABLE.rdd.map(lambda z : z.PART_COL).collect()))
				part_col_value = part_col_value + part_col_value_temp
	except:
		pass
	
	if "source-filter" in obj:
		source_filter = str(obj["source-filter"])
	else:
		source_filter = ''

	setColConditionDict = {}
	setUpdateConditionDict={}
	if "cols-to-update" in obj["update-keys"]:
		colsToUpdate = obj["update-keys"]["cols-to-update"]
	else:
		colsToUpdate = [loadEndKey]
	
	table = str(tableName).split(".")[1].lower()
	delta_table_path = delta_path + "/" + schema.upper() + "/" + table
	
	if "update-keys" in obj:
		update_key_cols = obj["update-keys"]["key-list"]
		
	
	update_condition = ""
	if len(part_col_value) == 1 and (part_col_value[0] != '' or part_col_value[0] is not None):
		for colname in update_key_cols:
			if update_condition == "" and source_filter != '':
				update_condition = "deltaTable."+ partition_col + "= '" + source_filter + "' and deltaTable."+ partition_col2 + "= '" + str(part_col_value[0]) + "' and deltaTable." + colname + " <=> updatedTable." + colname
			elif update_condition == "" : 
				update_condition = "deltaTable."+ colname + " <=> updatedTable." + colname
			else:
				update_condition = update_condition + " and deltaTable." + colname + " <=> updatedTable." + colname
	else:
		for colname in update_key_cols:
			if update_condition == "" and source_filter != '':
				update_condition = "deltaTable."+ partition_col + "= '" + source_filter + "' and deltaTable." + colname + " <=> updatedTable." + colname
			elif update_condition == "" : 
				update_condition = "deltaTable."+ colname + " <=> updatedTable." + colname
			else:
				update_condition = update_condition + " and deltaTable." + colname + " <=> updatedTable." + colname

	## Update only the open records in target table
	update_condition = update_condition + " and deltaTable." + loadEndKey + " is null" + " and deltaTable.BTCH_DT <= updatedTable.BTCH_DT"

	for colname in colListToInsert:
		setColConditionDict[colname] = "updatedTable."+colname
	
	## Update every record in LNK and only open record in SAT for expiring
	if len(colsToUpdate) == 1:
		setUpdateConditionDict[loadEndKey] = "updatedTable."+loadEndKey
	elif len(colsToUpdate) == 2:
		setUpdateConditionDict[loadEndKey] = "updatedTable."+loadEndKey
		setUpdateConditionDict["SRC_EXPRN_DT"] = "updatedTable.SRC_EFF_DT"

	DELTA_TABLE = DeltaTable.forPath(spark,delta_table_path)
	DELTA_TABLE.alias("deltaTable").merge(HASH_TABLE.alias("updatedTable"), update_condition)\
    .whenMatchedUpdate(set = setUpdateConditionDict)\
    .whenNotMatchedInsert(values = setColConditionDict).execute()

def xmstat_read_delta(spark,obj,logger,sourceDict=0):

	# Checking whether the read type is specified
	logger.info("Reading delta table for xmstat")
	if "read-type" in obj:
		readType = str(obj["read-type"])
	else:
		readType = "daily"
	
	#logger.info("Reading table " + schema + "." + table)
	logger.info("readType is: "+ readType)
	delta_table_path = "/mnt/cleanMount/xm/xmedw/xmstats/stat/" 
	logger.info("Reading stat table from " + delta_table_path)
	#reading data for the current month and previous month for daily load, other wise full load.
	if readType == "historical":
		logger.info("Reading historical load for xmstat table")
		sourceDF = spark.read \
				.format("delta") \
				.load(delta_table_path)
	elif readType == "full":
		logger.info("Reading full cycle year load for xmstat table")
		batch_year = str(int(obj["batchDate"][:4])-1900)
		logger.info("Reading data from xmstat table for cycle year: " + str(batch_year) )
		sourceDF = spark.read \
					.format("delta") \
					.load(delta_table_path).where(col("D36Y_NFU_ACTG_YEAR") == batch_year)
	elif readType == "daily":		
		stagingDF = spark.read.format('delta').load(delta_table_path)
		current_year = stagingDF.select(max(col("D36Y_NFU_ACTG_YEAR"))).first()[0]
		current_month = stagingDF.where(col("D36Y_NFU_ACTG_YEAR") == current_year).select(max(col("D36M_NFU_ACTG_MONTH"))).first()[0]
		if(int(current_month) == 1):
			logger.info("Reading data from xmstat table for cycle year: " + str(current_year) + " and cycle month: " + str(current_month) )
			sourceDF = spark.read \
					.format("delta") \
					.load(delta_table_path).where((col("D36Y_NFU_ACTG_YEAR") == current_year) & (col("D36M_NFU_ACTG_MONTH") == current_month))

		else:
			logger.info("Reading data from xmstat table for cycle year: " + str(current_year) + " and cycle months: " + str(current_month) + " and " + str(int(current_month) -1))
			sourceDF = spark.read \
					.format("delta") \
					.load(delta_table_path).where((col("D36Y_NFU_ACTG_YEAR") == current_year) & ((col("D36M_NFU_ACTG_MONTH") == current_month) | (col("D36M_NFU_ACTG_MONTH") == int(current_month)-1)))							

	return sourceDF    

def getPartitionCol_DM(DM_tableName):
	exceptionTableDict = {"dim_underwriting_company":"Dim_Underwriter_Company_Id"
						,"dim_policy_analytic_output":"Dim_Policy_Analytic_Id"
						,"fact_schedule_modifier_summary":"Fact_Schedule_Modifier_Monthly_Summary_Id"
						,"ref_system_close_date":"Dim_System_Close_Date_Id"}

	if DM_tableName.lower() in [x for x in sorted(exceptionTableDict.keys())]:
		partition_col = str(exceptionTableDict[str(DM_tableName.lower())])
	else:
		partition_col = DM_tableName.lower() + "_id"

	return partition_col