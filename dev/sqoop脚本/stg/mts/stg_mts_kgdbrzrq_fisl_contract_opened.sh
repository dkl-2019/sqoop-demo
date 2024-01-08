#!/bin/bash
HIVE_TABLE=stg_mts_kgdbrzrq_fisl_contract_opened
ORACLE_TABLE=FISL_CONTRACT_OPENED
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
AGREEMENT_NO         ,
CASH_NO              ,
TRD_DATE             ,
OPENING_DATE         ,
CUST_CODE            ,
CUST_CLS             ,
ACCOUNT              ,
CURRENCY             ,
ACC_CLS              ,
BRANCH               ,
EXT_INST             ,
SECU_ACC             ,
SECU_ACC_NAME        ,
MARKET               ,
BOARD                ,
SEAT                 ,
ORDER_ID             ,
SECU_INTL            ,
SECU_CODE            ,
SECU_NAME            ,
SECU_CLS             ,
TRD_ID               ,
CONTRACT_TYPE        ,
ORDER_QTY            ,
ORDER_PRICE          ,
ORDER_AMT            ,
ORDER_FRZ_AMT        ,
WITHDRAWN_QTY        ,
CONTRACT_QTY         ,
CONTRACT_AMT         ,
CONTRACT_INT         ,
CONTRACT_FEE         ,
OCCUPED_FEE          ,
MANAGER_FEE          ,
MARGIN_RATIO         ,
MARGIN_AMT           ,
REPAID_QTY           ,
REPAID_AMT           ,
REPAID_INT           ,
RLT_REPAID_QTY       ,
RLT_REPAID_AMT       ,
CAL_INT_DATE         ,
CONTRACT_INT_RATE    ,
CONTRACT_INT_ACCRUAL ,
INT_CONTRACT_AMT     ,
FREE_DATE            ,
FREE_ACCRUAL         ,
FREE_INT             ,
FREE_INT_RATE        ,
INT_FLAG             ,
FREE_FLAG            ,
CONTRACT_STATUS      ,
CLOSING_DATE         ,
CLOSING_PRICE        ,
CONTRACT_INT_EX      ,
TRD_DAYS             ,
FREE_DAYS            ,
SUSP_DAYS            ,
FREE_INT_EX          ,
REPAID_FREE_INT      ,
EXPIRATION_DATE      ,
ZFISL_DAYS           ,
ZOPENING_DATE        ,
ZMARKET              ,
ZBOARD               ,
ZORDER_ID            ,
REPAY_ORDER_SN       ,
CONTRACT_INT_LAST    ,
EXTD_CNT             ,
CMPD_INT_FLAG        ,
CMPD_INT_ACCRUAL     ,
CMPD_INT_RATE        ,
CMPD_INT             ,
CMPD_INT_EX          ,
CMPD_INT_DATE        ,
REPAID_CMPD_INT      ,
EXTEND_STATUS        ,
CMPD_PRIN            ,
CONTRACT_COST        ,
TOTAL_CONTRACT_COST  FROM KGDBRZRQ.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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