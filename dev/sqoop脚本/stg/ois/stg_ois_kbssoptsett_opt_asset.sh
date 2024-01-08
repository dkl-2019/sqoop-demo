#!/bin/bash
HIVE_TABLE=stg_ois_kbssoptsett_opt_asset
ORACLE_TABLE=OPT_ASSET
SQL_SERVER_URL='jdbc:sqlserver://172.20.5.22:1433;database=kbssoptsett'
filter_sql=" convert(varchar,UPD_TIME,112)=${batch_date} "
function getinfo() {
    SYS=ois
    SYS_USER=${SYS}.user
    SYS_PASSWORD=${SYS}.password
    FILENAME=DataMp/stg/config/password.properties
    echo $FILENAME
    for line in `sudo -u dsg cat $FILENAME`
    do
        #echo $line
        if [ ${line:0:${#SYS_USER}} = $SYS_USER ]; then
            USER=${line:${#SYS_USER}+1}
         fi
        if [ ${line:0:${#SYS_PASSWORD}} = $SYS_PASSWORD ]; then
            PASSWORD=${line:${#SYS_PASSWORD}+1}
         fi
    done
}
getinfo
echo "---------------------------------------${batch_date}---------------------------------------"
sqoop import -D mapred.job.name=sqoop_job_sql_server_to_hive_${HIVE_TABLE} \
--connect "${SQL_SERVER_URL}" \
--username $USER \
--password $PASSWORD  \
--query "SELECT ${batch_date} insert_date, 
CONVERT(varchar,GETDATE(),20) insert_time,
CUST_CODE         ,
CUST_TYPE         ,
CUACCT_CODE       ,
CUACCT_ATTR       ,
CUACCT_CLS        ,
CUACCT_LVL        ,
CUACCT_GRP        ,
INT_ORG           ,
STKEX             ,
STKBD             ,
STKPBU            ,
TRDACCT           ,
TRDACCT_EXCLS     ,
CURRENCY          ,
OPT_NUM           ,
OPT_CODE          ,
OPT_NAME          ,
OPT_TYPE          ,
OPT_SIDE          ,
OPT_CVD_FLAG      ,
OPT_UNDL_CLS      ,
OPT_UNDL_CODE     ,
OPT_PREBLN        ,
OPT_BLN           ,
OPT_AVL           ,
OPT_FRZ           ,
OPT_UFZ           ,
OPT_TRD_FRZ       ,
OPT_TRD_UFZ       ,
OPT_TRD_OTD       ,
OPT_TRD_BLN       ,
OPT_CLR_FRZ       ,
OPT_CLR_UFZ       ,
OPT_CLR_OTD       ,
OPT_BCOST         ,
OPT_BCOST_RLT     ,
OPT_PLAMT         ,
OPT_PLAMT_RLT     ,
OPT_MKT_VAL       ,
OPT_PREMIUM       ,
FLOAT_MARGIN_UNIT ,
OPT_MARGIN        ,
OPT_CVD_ASSET     ,
OPT_CLS_PROFIT    ,
OPT_FLOAT_PROFIT  ,
OPT_PROFIT        ,
UPD_TIME          ,
MAC               ,
OPT_PERIOD        ,
SUM_CLS_PROFIT    ,
STD_MARGIN        ,
SUBACCT_CODE      ,
QUOTA_VAL_USED    ,
OPT_DAILY_OPEN_RLT,
COMBED_QTY        ,
OPT_MARGIN_NEW    FROM ${ORACLE_TABLE} where ${filter_sql} and \$CONDITIONS" \
--m 1 \
--target-dir hdfs://nameservice1/user/hive/warehouse/stg.db/tmp/${HIVE_TABLE}/part_ymd=${batch_date} \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database stg \
--hive-table "${HIVE_TABLE}"  \
--hive-drop-import-delims \
--hive-partition-key part_ymd \
--hive-partition-value "${batch_date}" \
--null-string '\\N' \
--null-non-string '\\N'
if [ "$?" -ne 0 ]; then
   echo "-------------------------------command failed-------------------------------"
   exit 1
fi