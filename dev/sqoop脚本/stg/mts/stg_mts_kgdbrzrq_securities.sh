#!/bin/bash
HIVE_TABLE=stg_mts_kgdbrzrq_securities
ORACLE_TABLE=SECURITIES
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
MARKET              ,
BOARD               ,
CURRENCY            ,
SECU_INTL           ,
SECU_CODE           ,
SECU_NAME           ,
SECU_CLS            ,
TRADES              ,
CUSTODY_MODE        ,
SPREAD              ,
SECU_STATUS         ,
UNDERLYING_STOCK    ,
TRD_FLOOR           ,
TRD_CEILING         ,
FACE_VAL            ,
RISING_LIMIT        ,
FALLING_LIMIT       ,
PRICE_CEILING       ,
PRICE_FLOOR         ,
SUSP_FLAG           ,
SUSP_DAYS           ,
MKT_VAL_FLAG        ,
TT_STOCK            ,
CC_STOCK            ,
SECU_PRNAME         ,
SECU_ENNAME         ,
ISIN                ,
TRADE_TYPE          ,
ISSCU_TOTAL         ,
CIRCULATE           ,
LAST_PROFIT         ,
NOW_PROFIT          ,
FUND_VALUE          ,
HAND_RATE           ,
STAMP_RATE          ,
TRANS_RATE          ,
FLOTATION_DATE      ,
PAY_INT_DATE        ,
BOND_END_DATE       ,
BUY_UNIT            ,
SELL_UNIT           ,
CALL_AUCTION        ,
CONT_AUCTION        ,
AUCTION_CHAR        ,
BLOCK_CEILING       ,
BLOCK_FLOOR         ,
CONVERT_RATE        ,
COLLATERAL_RATE     ,
FIN_FLAG            ,
LEN_FLAG            ,
CONSTIT_FLAG        ,
MARKET_MAKER        ,
MARKET_CODE         ,
SECU_CLS_XXN        ,
SECU_LEVEL          ,
TRD_CLS             ,
TRD_PHASE           ,
FIN_STATUS          ,
LEN_STATUS          ,
LEN_LIMIT           ,
VOTE_FLAG           ,
OTHER_STATUS        ,
UPDATE_TIME         ,
BACKUP_MARK         ,
BACKUP_FLAG         ,
WARNING_INFO        ,
DELIST_DATE         ,
BGN_CLEARUP_DATE    ,
STOP_DAYS           ,
TRD_MODE            ,
TRD_LIMIT           ,
UNIT_QTY            ,
TIME_STAMP          ,
SECU_ATTR           ,
NOPROFIT            ,
CDR_WVR             ,
SJ_TRD_FLOOR        ,
SJ_TRD_CEILING      ,
PHDJ_BUY_CEILING    ,
PHDJ_SELL_CEILING   ,
SECU_RISK_CLS       ,
SECU_LNAME          FROM KGDBRZRQ.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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