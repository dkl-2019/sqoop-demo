#!/bin/bash
HIVE_TABLE=stg_ois_kbssoptsett_opt_matching
ORACLE_TABLE=OPT_MATCHING
SQL_SERVER_URL='jdbc:sqlserver://172.20.5.22:1433;database=kbssoptsett'
filter_sql=" TRD_DATE=${batch_date} "
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
TRD_DATE           ,
MATCHED_TIME       ,
ORDER_DATE         ,
ORDER_SN           ,
ORDER_BSN          ,
ORDER_ID           ,
ORDER_TYPE         ,
INT_ORG            ,
CUST_CODE          ,
CUST_NAME          ,
CUACCT_CODE        ,
STKEX              ,
STKBD              ,
STKPBU             ,
TRDACCT            ,
SUBACCT_CODE       ,
TRDACCT_EXID       ,
TRDACCT_TYPE       ,
TRDACCT_EXCLS      ,
STK_BIZ            ,
STK_BIZ_ACTION     ,
STK_BIZ_EX         ,
OWNER_TYPE         ,
OPT_NUM            ,
OPT_CODE           ,
OPT_NAME           ,
OPT_TYPE           ,
OPT_EXE_TYPE       ,
STK_FLAG           ,
COMB_NUM           ,
COMB_STRA_CODE     ,
LEG1_NUM           ,
LEG2_NUM           ,
LEG3_NUM           ,
LEG4_NUM           ,
CURRENCY           ,
OPT_UNDL_CLS       ,
OPT_UNDL_CODE      ,
OPT_UNDL_NAME      ,
ORDER_PRICE        ,
ORDER_QTY          ,
ORDER_AMT          ,
ORDER_FRZ_AMT      ,
MARGIN_PRE_AMT     ,
IS_WITHDRAW        ,
MATCHED_TYPE       ,
MATCHED_SN         ,
MATCHED_PRICE      ,
MATCHED_QTY        ,
MATCHED_AMT        ,
RLT_SETT_AMT       ,
MARGIN_REAL_AMT    ,
MATCHED_FEE        ,
OP_USER            ,
OP_ROLE            ,
OP_NAME            ,
OP_ORG             ,
OP_SITE            ,
CHANNEL            ,
STKEX_ORG_ID       ,
ORDER_ID_EX        ,
QUERY_POS          ,
REFF_ORDER_ID      ,
SUBSYS_SN          ,
LEG1_SIDE          ,
LEG2_SIDE          ,
LEG3_SIDE          ,
LEG4_SIDE          ,
LEG1_NAME          ,
LEG2_NAME          ,
LEG3_NAME          ,
LEG4_NAME          FROM ${ORACLE_TABLE} where ${filter_sql} and \$CONDITIONS" \
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