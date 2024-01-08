#!/bin/bash
HIVE_TABLE=stg_cts_kgdb_gp_contracts
ORACLE_TABLE=GP_CONTRACTS
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
SECU_ACC_IN          ,
CUST_CODE_IN         ,
ACCOUNT_IN           ,
CURRENCY_IN          ,
SECU_ACC_OUT         ,
CUST_CODE_OUT        ,
ACCOUNT_OUT          ,
CURRENCY_OUT         ,
LENDER_CLS           ,
PAWNEE_CLS           ,
ORDER_ID             ,
SECU_CODE            ,
SHARE_CLS            ,
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
SECU_INTL            ,
PERFORM_DATE         ,
PERFORM_FLAG         ,
PERFORM_AMT          ,
OMARKET_AMT          ,
HANDLE_DATE          ,
LTLX                 ,
QYLB                 ,
GPNF                 ,
GP_FLAG              ,
MER_TRD_DATE         ,
MER_CONSTR_SN        ,
CASH_AMT             ,
HAS_CONTR_AMT        ,
INVESTMENTTYPE       ,
ALERTRATIO           ,
SETTLEMENTRATIO      ,
QTDBWMS              ,
QTDBWJZ              ,
FEE_CAL_DAYS         ,
FDJX                 ,
CURR_FEE_ACCRU       ,
CURR_FEE_AMT         ,
UP_DATE_FEEAMTT      ,
UP_DATE_FEEACCRU     ,
RISKRATEMODE         ,
DBWRISKMODE          ,
ZYDJBH               ,
CNW_CONSTR_FLAG      ,
MAIN_CONSTR_FLAG     ,
MAIN_CONSTR_DATE     ,
MAIN_CONSTR_SN       ,
WYCZ_QTY   FROM kgdb.\"${ORACLE_TABLE}\" where ${filter_sql} and \$CONDITIONS" \
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