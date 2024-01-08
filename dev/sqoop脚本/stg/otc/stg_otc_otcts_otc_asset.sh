#!/bin/bash
HIVE_TABLE=stg_otc_otcts_otc_asset
ORACLE_TABLE=OTC_ASSET
ORACLE_URL='jdbc:oracle:thin:@172.20.5.129:1521:otcmdb'
filter_sql=" 1=1 "
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
TA_CODE           ,
TA_ACCT           ,
TRANS_ACCT        ,
CUST_CODE         ,
ECIF_CODE         ,
INT_ORG           ,
CUACCT_CODE       ,
INST_SNO          ,
INST_LAST_BAL     ,
INST_BAL          ,
INST_AVL          ,
INST_TRD_FRZ      ,
INST_LONG_FRZ     ,
INST_OTD          ,
INST_BAL_OTD      ,
LAST_COST         ,
CURRENT_COST      ,
MKT_VALUE         ,
CNTR_FLAG         ,
TODAY_SUBS_AMT    ,
HIS_SUBS_AMT      ,
BOOK_SUBS_AMT     ,
BOOK_BIDS_AMT     ,
BOOK_REDEEM_QTY   ,
REMARK            ,
UPD_TIMESTAMP     ,
ACCU_BUY_AMT      ,
UNSETT_QTY_ASSET  ,
UNSETT_FUND       ,
FIRST_GET_DATE    ,
ASSET_TO_AMT_AVL  ,
FORTUNE_ACCT      ,
LAST_MKT_VALUE    FROM otcts.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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