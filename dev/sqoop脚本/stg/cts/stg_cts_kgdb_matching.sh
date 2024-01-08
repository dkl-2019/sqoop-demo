#!/bin/bash
HIVE_TABLE=stg_cts_kgdb_matching
ORACLE_TABLE=MATCHING
ORACLE_URL='jdbc:oracle:thin:@172.20.25.44:1521:kdtest'
filter_sql=" substr(TRD_DATE,1,8)=${batch_date} "
function getinfo() {
    SYS=cts
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
RECORD_SN            ,
TRD_DATE             ,
MATCHED_TIME         ,
ORDER_DATE           ,
CUST_CODE            ,
SECU_ACC_NAME        ,
USER_CHAR            ,
CUST_CLS             ,
SPECIAL_CUST         ,
ACCOUNT              ,
CURRENCY             ,
ACC_CLS              ,
BRANCH               ,
SECU_ACC             ,
TRD_ID               ,
IS_WITHDRAW          ,
MARKET               ,
ORDER_ID             ,
BOARD                ,
SEAT                 ,
SECU_INTL            ,
SECU_CODE            ,
SECU_NAME            ,
SECU_CLS             ,
ORDER_PRICE          ,
ORDER_QTY            ,
ORDER_FRZ_AMT        ,
MATCHED_PRICE        ,
MATCHED_QTY          ,
MATCHED_AMT          ,
RLT_SETT_AMT         ,
SETT_AMT             ,
AVAILABLE            ,
SHARE_AVL            ,
PROFIT               ,
MATCHED_SN           ,
OP_USER              ,
OP_NAME              ,
OP_ROLE              ,
OP_BRH               ,
CHANNEL              ,
REMARK               ,
EXT_INST             ,
EXT_ACC              ,
EXT_SUB_ACC          ,
OCCUR_DATE           FROM kgdb.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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