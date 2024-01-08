#!/bin/bash
HIVE_TABLE=stg_ois_kbssoptsett_opt_mktinfo
ORACLE_TABLE=OPT_MKTINFO
SQL_SERVER_URL='jdbc:sqlserver://172.20.5.22:1433;database=kbssoptsett'
filter_sql=" TRD_DATE=${batch_date} "
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
TRD_DATE             ,
TOTAL_AMT            ,
TOTAL_VOLUME         ,
OPT_CLS_PRICE        ,
OPT_SETT_PRICE       ,
OPT_OPEN_PRICE       ,
OPT_CURR_PRICE       ,
OPT_HIGH_PRICE       ,
OPT_LOW_PRICE        ,
OPT_TRD_PRICE        ,
OPT_AUCT_PRICE       ,
BUY_PRICE1           ,
BUY_VOLUME1          ,
SELL_PRICE1          ,
SELL_VOLUME1         ,
BUY_PRICE2           ,
BUY_VOLUME2          ,
SELL_PRICE2          ,
SELL_VOLUME2         ,
BUY_PRICE3           ,
BUY_VOLUME3          ,
SELL_PRICE3          ,
SELL_VOLUME3         ,
BUY_PRICE4           ,
BUY_VOLUME4          ,
SELL_PRICE4          ,
SELL_VOLUME4         ,
BUY_PRICE5           ,
BUY_VOLUME5          ,
SELL_PRICE5          ,
SELL_VOLUME5         FROM ${ORACLE_TABLE} where ${filter_sql} and \$CONDITIONS" \
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