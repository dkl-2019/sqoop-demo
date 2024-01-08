#!/bin/bash
HIVE_TABLE=stg_mts_kgdbrzrq_capital
ORACLE_TABLE=CAPITAL
ORACLE_URL='jdbc:oracle:thin:@172.20.25.45:1521:qstest'
filter_sql=" 1=1 "
function getinfo() {
    SYS=mts
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
sqoop import -D mapred.job.name=sqoop_job_oracle_to_hive_${HIVE_TABLE} \
-D sqoop.parquet.logical_types.decimal.enable=true \
-D parquetjob.configurator.implementation=hadoop \
-D sqoop.avro.decimal_padding.enable=true \
-D sqoop.avro.logical_types.decimal.default.precision=16 \
-D sqoop.avro.logical_types.decimal.default.scale=2 \
--connect "${ORACLE_URL}" \
--username $USER \
--password $PASSWORD  \
--query "SELECT ${batch_date} insert_date, 
TO_CHAR(sysdate, 'YYYY-MM-DD HH24:MI:SS') insert_time,
USER_CODE         ,
ACCOUNT           ,
CURRENCY          ,
BALANCE           ,
AVAILABLE         ,
FROZEN            ,
TRD_FRZ           ,
OUTSTANDING       ,
OTD_AVL           ,
CR_AMT            ,
DR_AMT            ,
OTD_DR            ,
CR_BLN            ,
CR_AVL            ,
SAVING_GRP        ,
DR_RATE_GRP       ,
SAVING_ACCRUAL    ,
LAST_INT_BLN      ,
CAL_INT_DATE      ,
SAVING_INT        ,
SAVING_INT_TAX    ,
DR_INT            ,
CASH_ACCRUAL      ,
CHQ_ACCRUAL       ,
MKT_VAL           ,
ASSETS_FLOOR      ,
RELATED_ACC_FLAG  ,
UPD_DATE          ,
MAC               ,
OPENED_BIZ        ,
STATUS            ,
ACC_CHAR          ,
CERTIFIED_AMT     FROM KGDBRZRQ.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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