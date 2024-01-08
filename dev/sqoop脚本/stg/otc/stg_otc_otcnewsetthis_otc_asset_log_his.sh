#!/bin/bash
HIVE_TABLE=stg_otc_otcnewsetthis_otc_asset_log_his
ORACLE_TABLE=OTC_ASSET_LOG_HIS
ORACLE_URL='jdbc:oracle:thin:@172.20.5.129:1521:otcmdb'
filter_sql=" to_char(UPD_TIMESTAMP,'YYYYMMDD')=${batch_date} "
function getinfo() {
    SYS=otc
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
SNO                 ,
DELIVY_DATE         ,
SETT_NO             ,
SETT_DATE           ,
SETT_BAT_NO         ,
TRD_DATE            ,
APP_SNO             ,
CNTR_NO             ,
CNTR_REG_DATE       ,
BIZ_CODE            ,
TRD_ID              ,
TA_CODE             ,
TRANS_ACCT          ,
INT_ORG             ,
TA_ACCT             ,
CUST_CODE           ,
ECIF_CODE           ,
CUACCT_CODE         ,
CURRENCY            ,
BANK_CODE           ,
PAY_ACCT            ,
PAY_ORG             ,
PAY_WAY             ,
BANK_ACCT           ,
ISS_CODE            ,
INST_SNO            ,
INST_ID             ,
INST_CODE           ,
INST_SNAME          ,
INST_TYPE           ,
INST_CLS            ,
ORD_QTY             ,
ORD_AMT             ,
ORD_PRICE           ,
CFM_AMT             ,
CFM_VOL             ,
CFM_PRICE           ,
PAID_AMT            ,
FRZ_AMT             ,
UNFRZ_AMT           ,
RET_AMT             ,
AMT_BAL_EFCT        ,
AMT_AVL_EFCT        ,
AMT_BAL_AFTER       ,
VOL_BAL_EFCT        ,
VOL_AVL_EFCT        ,
VOL_BAL_AFTER       ,
ORD_FRZ_QTY         ,
ORD_FRZ_AMT         ,
ORD_UNFRZ_AMT       ,
ORD_UNFRZ_QTY       ,
CLS_FRZ_AMT         ,
CLS_FRZ_QTY         ,
COMMISION           ,
CHARGE              ,
FUND_REWARD         ,
OTHER_FEE1          ,
OTHER_FEE2          ,
OTHER_FEE3          ,
OP_USER_CODE        ,
OP_USER_ROLE        ,
OP_USER_NAME        ,
OP_CHNL             ,
OP_SITE             ,
REMARK              ,
DATA_SRC            ,
UPD_TIMESTAMP       ,
FUND_ARRIV_DATE     ,
CLS_UNFRZ_AMT       ,
EXP_STAT            ,
APP_DATE            ,
FORTUNE_ACCT        FROM otcnewsetthis.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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