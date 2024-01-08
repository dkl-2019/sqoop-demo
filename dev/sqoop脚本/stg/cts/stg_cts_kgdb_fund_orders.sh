#!/bin/bash
HIVE_TABLE=stg_cts_kgdb_fund_orders
ORACLE_TABLE=FUND_ORDERS
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
OCCUR_DATE           ,
TRD_DATE             ,
CUST_CODE            ,
USER_NAME            ,
USER_CHAR            ,
CUST_CLS             ,
ACCOUNT              ,
CURRENCY             ,
ACC_CLS              ,
BRANCH               ,
EXT_INST             ,
INITIATOR            ,
TRD_ID               ,
ORDER_SN             ,
APP_SN               ,
TA_CODE              ,
FUND_ACC             ,
FUND_TRD_ACC         ,
FUND_ACC_NAME        ,
FUND_INTL            ,
FEE_TYPE             ,
ORDER_VOL            ,
ORDER_AMT            ,
FUND_CODE            ,
PROTOCOL             ,
FUND_NAME            ,
DISCOUNT_RATIO       ,
REDEEM_TYPE          ,
CAUSE                ,
DEADLINE             ,
ORDER_STATUS         ,
ORDER_FRZ_AMT        ,
AVAILABLE            ,
SETT_AMT             ,
REDEEM_FLAG          ,
POST_DAYS            ,
BOOKING_DATE         ,
FUND_AVL             ,
ORI_CFM_SN           ,
DIV_MATHOD           ,
DIV_ASSIGN_RATIO     ,
TARGET_FUND_CODE     ,
TARGET_FEE_TYPE      ,
TARGET_FUND_ACC      ,
TARGET_DISTRIBUTOR   ,
TARGET_BRH           ,
TARGET_TRD_ACC       ,
FUND_TRD_FRZ         ,
FUND_FRZ             ,
REMARK               ,
ORI_APP_SN           ,
IS_WITHDRAW          ,
OP_USER              ,
OP_ROLE              ,
OP_BRH               ,
OP_SITE              ,
CHANNEL              ,
EXT_REC_NUM          ,
EXT_ORDER_ID         ,
EXT_BIZ_NO           ,
EXT_ACC              ,
EXT_SUB_ACC          ,
EXT_FRZ_AMT          ,
EXT_SETT_AMT         ,
CFM_DATE             ,
MATCHED_AMT          ,
MATCHED_VOL          ,
NAV                  ,
CFM_SN               ,
CHARGE_TYPE          ,
SPECIFY_RATE_FEE     ,
SPECIFY_FEE          ,
BACKENLOAD_DISCOUNT  ,
TARGET_TA_CODE       ,
PRE_TRD_DATE         ,
PRE_ORDER_AMT        ,
BUY_TYPE             ,
QUAL_CONTR_SN        FROM kgdb.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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