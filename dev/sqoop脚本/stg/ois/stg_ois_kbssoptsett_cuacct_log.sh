#!/bin/bash
HIVE_TABLE=stg_ois_kbssoptsett_cuacct_log
ORACLE_TABLE=CUACCT_LOG
SQL_SERVER_URL='jdbc:sqlserver://172.20.5.22:1433;database=kbssoptsett'
filter_sql=" SETT_DATE=${batch_date} "
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
BIZ_NO            ,
SERIAL_NO         ,
SETT_DATE         ,
OCCUR_DATE        ,
OCCUR_TIME        ,
COME_IN_DATE      ,
INT_ORG           ,
USER_CODE         ,
USER_ROLE         ,
USER_NAME         ,
USER_CLS          ,
CUACCT_CODE       ,
CUACCT_ATTR       ,
CUACCT_CLS        ,
CUACCT_LVL        ,
CUACCT_GRP        ,
CUACCT_DMF        ,
CURRENCY          ,
EXT_ORG           ,
EXT_ORG_TYPE      ,
BIZ_CODE          ,
BIZ_AMT           ,
BIZ_AMT_EX        ,
FUND_BLN          ,
OP_USER           ,
OP_ROLE           ,
OP_NAME           ,
OP_ORG            ,
OP_SITE           ,
CHANNEL           ,
CANCEL_FLAG       ,
ORIGINAL_SN       ,
EXT_SERIAL_NO     ,
CHK_APP_SN        ,
SUBSYS_SN         ,
REMARK            FROM ${ORACLE_TABLE} where ${filter_sql} and \$CONDITIONS" \
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