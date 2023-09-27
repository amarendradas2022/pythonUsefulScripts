%python
#This Script will provide Schema for the parquet files in Databricks in a specific directory
## In the Current Script, it will print Parquet Schema Details for pattern
import builtins
import os
import glob


path = "<Base path where parquet files are kept>"
dir_list = os.listdir(path)

for i in dir_list:
    path_2nd_level=path+i
    list_of_files = glob.glob(path_2nd_level+'/*') # * means all if need specific format then *.csv
    latest_file = builtins.max(list_of_files, key=os.path.getmtime)
    if '<Pattern here>' in latest_file:
        print(latest_file)
		## Prquet table name is present in 6th position. Needs to update as per position of the tablename in the directory path
        table_name=latest_file.split("/")[6]
        print(table_name)
        newdir = latest_file.replace("/dbfs", "")
        qdf= spark.read.parquet(newdir)
        qdf.printSchema()
