#!/bin/bash
HIVE_TABLE=stg_otc_otcts_otc_inst_mkt_info
ORACLE_TABLE=OTC_INST_MKT_INFO
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
INST_SNO          ,
TRD_DATE          ,
LAST_NET          ,
ACCU_NET          ,
CUR_PROFIT_RATE   ,
CLOSE_PRICE       ,
OPEN_PRICE        ,
LAST_PRICE        ,
LAST_QTY          ,
BEST_BUY_PRICE    ,
BEST_SELL_PRICE   ,
BEST_BUY_QTY      ,
BEST_SELL_QTY     ,
TODAY_MATCHED_QTY ,
TODAY_MATCHED_AMT ,
BUYER_QTY         ,
SELLER_QTY        ,
BUY_QTY1          ,
BUY_PRICE1        ,
BUY_QTY2          ,
BUY_PRICE2        ,
BUY_QTY3          ,
BUY_PRICE3        ,
BUY_QTY4          ,
BUY_PRICE4        ,
BUY_QTY5          ,
BUY_PRICE5        ,
SELL_QTY1         ,
SELL_PRICE1       ,
SELL_QTY2         ,
SELL_PRICE2       ,
SELL_QTY3         ,
SELL_PRICE3       ,
SELL_QTY4         ,
SELL_PRICE4       ,
SELL_QTY5         ,
SELL_PRICE5       ,
UPD_TIMESTAMP     ,
NET_DATE          ,
UNIT_DIV          ,
CUR_TOTAL_INCOME  ,
OF_STAT           ,
YIELD             ,
INST_INCOME_UNIT  ,
CONVER_FACTOR     ,
EXT_PRICE         FROM otcts.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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