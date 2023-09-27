
##CREATE TABLE if not exists dmf_dq_db.dq_err_tables_master (
##  dq_err_tables_master_id LONG,
##  dq_table_name STRING,
##  no_of_exec_to_keep INTEGER,
##  actv_inactv_ind STRING
##  )
##USING delta TBLPROPERTIES (delta.autoOptimize.optimizeWrite = 'true')

##create table if not exists dmf_dq_db.dq_err_tables_clean_log (
##  dq_err_tables_master_id LONG,
##  execution_ts timestamp,
##  dq_table_name STRING,
##  exec_avail_in_frt long,
##  max_run_id_deleted_in_frt long,
##  records_removed_from_frt long
##)
##USING delta TBLPROPERTIES (delta.autoOptimize.optimizeWrite = 'true')


from pyspark.sql import SparkSession

# Creating a Spark Session
schema_name="dmf_dq_db"
spark = SparkSession.builder \
  .appName("Housekeeping of DQ tables based on retention months") \
    .getOrCreate()

## Getting the tables from DQ_TABLE_MASTER which need to be cleaned
query = "select * from "+ schema_name +".dq_err_tables_master where actv_inactv_ind = 'Y' and dq_table_name is not null and nvl(no_of_exec_to_keep,0) > 0"
result_df = spark.sql(query)

for row in result_df.collect():
  table_name = row.dq_table_name
  rentention_mnths = row.no_of_exec_to_keep
  dq_err_tables_master_id = row.dq_err_tables_master_id

  exec_avail_in_frt_qry="select count(distinct run_id) as cnt_run_id from "+ schema_name+"."+ table_name
  try:
    exec_avail_in_frt_qry_df=spark.sql(exec_avail_in_frt_qry)
    exec_avail_in_frt=[row.cnt_run_id for row in exec_avail_in_frt_qry_df.collect()]
    print("Executions Available in FRT table "+ table_name +" is: " + str(exec_avail_in_frt[0]))

    ## Getting the RUN_IDs which needs to be removed based on configuration
    ret_query="select (max(run_id) - "+str(rentention_mnths) + ") as execs_to_del from " +schema_name+"."+ table_name
    result_ret_query_df = spark.sql(ret_query)

    ## Constructing Delete Statement
    column_values=[row.execs_to_del for row in result_ret_query_df.collect()]

    if ((exec_avail_in_frt[0]-rentention_mnths) > 0):
      print("Maximum Run ID Deleted from FRT table "+ table_name +" is: " + str(column_values[0]))

      del_cnt_qry="select count(*) del_tot_cnt from "+ schema_name +"." +table_name + " where run_id <= "+ str(column_values[0])
      del_cnt_df=spark.sql(del_cnt_qry)
      del_cnt=[row.del_tot_cnt for row in del_cnt_df.collect()]
      print("Total Records deleted from table "+ table_name + " is: " + str(del_cnt[0]))

      del_query="delete from "+ schema_name +"." +table_name + " where run_id <= "+ str(column_values[0])
      print(del_query)
      ## Deleting the rows in the table older than calculated RUN_IDs
      spark.sql(del_query)

      ins_qry1="insert into "+schema_name +".dq_err_tables_clean_log(dq_err_tables_master_id, execution_ts, dq_table_name, exec_avail_in_frt, max_run_id_deleted_in_frt, records_removed_from_frt) values("
      ins_qry2=str(dq_err_tables_master_id)+", "+"current_timestamp(), '" + table_name +"', "+str(exec_avail_in_frt[0])+","+str(column_values[0])+","+ str(del_cnt[0])
      ins_qry=ins_qry1+ins_qry2+")"

      print(ins_qry)
      spark.sql(ins_qry)
    else:
      print("There are no RUN IDs to delete for table: " + table_name)
  
  except Exception as e:
    print("An Error Occured: ", str(e))

print("HouseKeeping Completed.")
