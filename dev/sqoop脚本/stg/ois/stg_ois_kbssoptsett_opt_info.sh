#!/bin/bash
HIVE_TABLE=stg_ois_kbssoptsett_opt_info
ORACLE_TABLE=OPT_INFO
SQL_SERVER_URL='jdbc:sqlserver://172.20.5.22:1433;database=kbssoptsett'
filter_sql=" UPD_DATE=${batch_date} "
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
STKEX                ,
STKBD                ,
OPT_NUM              ,
OPT_CODE             ,
OPT_NAME             ,
OPT_TYPE             ,
OPT_UNDL_CODE        ,
OPT_UNDL_NAME        ,
OPT_UNDL_CLS         ,
EXE_TYPE             ,
OPT_UNIT             ,
EXERCISE_PRICE       ,
START_DATE           ,
END_DATE             ,
EXERCISE_DATE        ,
DELIVERY_DATE        ,
EXPIRE_DATE          ,
UPD_VERSION          ,
LEAVES_QTY           ,
PRE_CLOSE_PX         ,
PRE_SETT_PRICE       ,
UNDL_CLS_PRICE       ,
PRICE_LMT_TYPE       ,
OPT_UPLMT_PRICE      ,
OPT_LWLMT_PRICE      ,
FLOAT_MARGIN_UNIT    ,
STD_MARGIN_UNIT      ,
MARGIN_UNIT          ,
MARGIN_RATIO1        ,
MARGIN_RATIO2        ,
OPT_LOT_SIZE         ,
OPT_LUPLMT_QTY       ,
OPT_LLWLMT_QTY       ,
OPT_MUPLMT_QTY       ,
OPT_MLWLMT_QTY       ,
OPEN_FLAG            ,
SUSPENDED_FLAG       ,
EXPIRE_FLAG          ,
ADJUST_FLAG          ,
OPT_STATUS           ,
UPD_DATE             ,
REFF_DATE            ,
TICK_SIZE            ,
COMB_FLAG            ,
AUTO_SPLIT_DATE      ,
OPT_UNIT_NEW         ,
EXERCISE_PRICE_NEW   ,
STD_MARGIN_UNIT_NEW  FROM ${ORACLE_TABLE} where ${filter_sql} and \$CONDITIONS" \
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