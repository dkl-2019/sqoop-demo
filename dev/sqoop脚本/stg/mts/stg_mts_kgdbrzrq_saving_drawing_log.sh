#!/bin/bash
HIVE_TABLE=stg_mts_kgdbrzrq_saving_drawing_log
ORACLE_TABLE=SAVING_DRAWING_LOG
ORACLE_URL='jdbc:oracle:thin:@172.20.25.45:1521:qstest'
filter_sql=" substr(SETT_DATE,1,8)=${batch_date} "
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
SETT_DATE            ,
OCCUR_DATE           ,
OCCUR_DATE2          ,
SERIAL_NO            ,
USER_CODE            ,
USER_ROLE            ,
USER_CHAR            ,
USER_NAME            ,
USER_CLS             ,
ACCOUNT              ,
CURRENCY             ,
ACC_CLS              ,
BRANCH               ,
EXT_INST             ,
BIZ_CODE             ,
CPTL_AMT             ,
BALANCE              ,
IS_CHECK             ,
SPECIAL_DRAWING_CLS  ,
OP_USER              ,
OP_ROLE              ,
OP_NAME              ,
OP_BRH               ,
OP_SITE              ,
CFM_OP_USER          ,
CFM_OP_NAME          ,
CFM_OP_SITE          ,
AGENT_CODE           ,
AGENT_NAME           ,
CHANNEL              ,
APPENDIX             ,
REMARK               ,
INITIATOR            ,
EXT_REC_NUM          ,
EXT_BIZ_NO           ,
EXT_ACC              ,
EXT_SUB_ACC          ,
CANCEL_FLAG          ,
BIZ_NO               FROM KGDBRZRQ.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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