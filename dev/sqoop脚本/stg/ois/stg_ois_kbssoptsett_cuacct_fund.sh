#!/bin/bash
HIVE_TABLE=stg_ois_kbssoptsett_cuacct_fund
ORACLE_TABLE=CUACCT_FUND
SQL_SERVER_URL='jdbc:sqlserver://172.20.5.22:1433;database=kbssoptsett'
filter_sql=" 1=1 "
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
USER_CODE             ,
ECIF_CODE             ,
USER_NAME             ,
CUACCT_CODE           ,
CUACCT_ATTR           ,
CUACCT_CLS            ,
CUACCT_LVL            ,
CUACCT_GRP            ,
CUACCT_DMF            ,
INT_ORG               ,
CURRENCY              ,
FUND_PREBLN           ,
FUND_BLN              ,
FUND_AVL              ,
FUND_FRZ              ,
FUND_UFZ              ,
FUND_TRD_FRZ          ,
FUND_TRD_UFZ          ,
FUND_TRD_OTD          ,
FUND_TRD_BLN          ,
FUND_CLR_FRZ          ,
FUND_EXE_FRZ          ,
FUND_CLR_UFZ          ,
FUND_CLR_OTD          ,
FUND_CLR_CTF          ,
FUND_TRN_FRZ          ,
FUND_TRN_UFZ          ,
FUND_DEBT             ,
FUND_CREDIT           ,
INT_RATE_SN           ,
DR_RATE_SN            ,
INT_CAL_BLN           ,
INT_BLN_ACCU          ,
INT_CAL_DATE          ,
INTEREST              ,
INT_TAX               ,
DR_INT                ,
MKT_VAL               ,
CASH_ACCU             ,
CHEQUE_ACCU           ,
LAST_FUND_CLR         ,
LWLMT_FUND            ,
LWLMT_ASSET           ,
UPD_TIME              ,
FUND_STATUS           ,
MAC                   ,
PAYLATER              ,
DEBT_BLN_ACCU         ,
FUND_CSDC_MARIN       ,
FUND_EXE_MARGIN       ,
FUND_FEE_FRZ          ,
DAILY_OUT_AMT         ,
DAILY_IN_AMT          ,
FUND_RET              ,
PREADVA_PAY           ,
PRE_INTEREST          ,
PAYLATER2             ,
SH_SPLIT_RATE         ,
TRANS_AMT             ,
DRAW_CASH_MARGIN_RATE ,
LMT_OPEN_MARGIN_RATE  ,
FUND_CLR_FRZ_NEW      FROM ${ORACLE_TABLE} where ${filter_sql} and \$CONDITIONS" \
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