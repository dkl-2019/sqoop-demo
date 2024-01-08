#!/bin/bash
HIVE_TABLE=stg_cts_kgdb_xy_contracts
ORACLE_TABLE=XY_CONTRACTS
ORACLE_URL='jdbc:oracle:thin:@172.20.25.44:1521:kdtest'
filter_sql=" 1=1 "
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
TRD_DATE             ,
MARKET               ,
CONSTR_SN            ,
CONSTR_TYPE          ,
SECU_ACC             ,
SEAT                 ,
CUST_CODE            ,
ACCOUNT              ,
CURRENCY             ,
SECU_ACC_OUT         ,
ORDER_ID             ,
SECU_INTL            ,
SECU_CODE            ,
CONTRACT_QTY         ,
HAS_BB_QTY           ,
INT_QTY              ,
BB_QTY               ,
CONTRACT_AMT         ,
FEE_RATE             ,
FEE_AMT              ,
HAS_BB_AMT           ,
INT_AMT              ,
BB_AMT               ,
ORG_BB_DATE          ,
BB_DATE              ,
BB_DAYS              ,
ORG_BB_DAYS          ,
ORG_FEE_RATE         ,
ORG_BB_AMT           ,
REMARK               ,
PATH_NO              ,
BB_STATUS            ,
CON_FLAG             ,
OMARKET_AMT          ,
LTLX                 ,
QYLB                 ,
GPNF                 ,
SUPPLEMENTAL         ,
OPEN_BRH             ,
SECU_ACC_NAME        ,
MBR_ID               ,
INVES_ID             ,
INVES_TYPE           ,
INVES_NAME           ,
TRADER_CODE          ,
CP_MBR_ID            ,
CP_INVES_ID          ,
CP_INVES_TYPE        ,
CP_TRADER_CODE       FROM kgdb.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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