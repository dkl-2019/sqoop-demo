-- TODO 3.ods_ads_日表
set mapreduce.job.queuename = root.users.rtassets;
with crm as (
    select rq                                                              as DATA_DATE
         , yyb                                                             as INNERORG_CODE
         , yybmc                                                           as INNERORG_NAME
         , cast(lrxfsr / 10000 as decimal(19, 6))                          as CRD_IEXP_INCOME
         , xzlryxh                                                         as ADD_CRD_VALID_CUST_CNT
         , cast(jrcpxssr / 10000 as decimal(19, 6))                        as FIN_PD_SELL_INCOME
         , jrcpfgjs                                                        as FIN_PD_CUST_CARDIN
         , jrcpyxhs                                                        as FIN_PD_VALID_CUST_CNT
         , syxkkhs                                                         as PER_MTH_NEW_CUST_CNT
         , syxkkhrjs                                                       as PER_MTH_NEW_TRD_CUST_CNT
         , cast(yybxzzc / 10000 as decimal(19, 6))                         as ADD_AST
         , cast(yybzzc / 10000 as decimal(19, 6))                          as TOT_AST
         , cast(tgcpzsr / 10000 as decimal(19, 6))                         as IA_PD_INCOME
         , cast(tgcpqdsr / 10000 as decimal(19, 6))                        as IA_PD_FRONT_INCOME
         , cast(tgcphdsr / 10000 as decimal(19, 6))                        as IA_PD_BACK_INCOME
         , cast(if(jrcpfgjs > 0, jrcpyxhs / jrcpfgjs, 0) as decimal(6, 2)) as COVER_RATE
         , cast(if(syxkkhs > 0, syxkkhrjs / syxkkhs, 0) as decimal(6, 2))  as IN_MONEY_RATE
         , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')          as DATA_TIME
    from rtassets_ods.ods_bs_crm_d
    where part_ymd = '${batch_date}'
),
     org_area as (
         select INNERORG_CODE,
                INNERORG_AREA
         from rtassets_dw.dim_innerorg_area_relation
     )
insert overwrite table rtassets_ads.ads_bs_crm_d partition (part_ymd = '${batch_date}')
select crm.DATA_DATE,
       crm.INNERORG_CODE,
       crm.INNERORG_NAME,
       org_area.INNERORG_AREA,
       crm.CRD_IEXP_INCOME,
       crm.ADD_CRD_VALID_CUST_CNT,
       crm.FIN_PD_SELL_INCOME,
       crm.FIN_PD_CUST_CARDIN,
       crm.FIN_PD_VALID_CUST_CNT,
       crm.PER_MTH_NEW_CUST_CNT,
       crm.PER_MTH_NEW_TRD_CUST_CNT,
       crm.ADD_AST,
       crm.TOT_AST,
       crm.IA_PD_INCOME,
       crm.IA_PD_FRONT_INCOME,
       crm.IA_PD_BACK_INCOME,
       crm.COVER_RATE,
       crm.IN_MONEY_RATE,
       crm.DATA_TIME
from crm
         left join org_area on crm.INNERORG_CODE = org_area.INNERORG_CODE;



set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
-- 执行map前进行小文件合并
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.min.split.size=100000000;
set mapred.max.split.size=100000000;
-- 一个节点上split的至少的大小 ，决定了多个data node上的文件是否需要合并
set mapred.min.split.size.per.node=50000000;
-- 一个交换机下split的至少的大小，决定了多个交换机上的文件是否需要合并
set mapred.min.split.size.per.rack=50000000;
--在Map-only的任务结束时合并小文件
set hive.merge.mapfiles=true;
--在Map-Reduce的任务结束时合并小文件
set hive.merge.mapredfiles=true;
--合并后文件的大小为128M左右
set hive.merge.size.per.task=128000000;
set hive.exec.reducers.bytes.per.reducer=128000000;
-- set hive.merge.smallfiles.avgsize=128000000;  --当输出文件的平均大小小于128M时，启动一个独立的map-reduce任务进行文件merge
