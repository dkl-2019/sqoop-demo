-- TODO 4. 月表
set mapreduce.job.queuename = root.users.rtassets;
-- 内存1024跑不下来 默认0 和 0
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
-- 执行map前进行小文件合并 默认 (org.apache.hadoop.hive.ql.io.CombineHiveInputFormat)
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
--默认1
set mapred.min.split.size=128000000;
--默认256000000
set mapred.max.split.size=256000000;
--在Map-only的任务结束时合并小文件(默认是true)
set hive.merge.mapfiles=true;
--在Map-Reduce的任务结束时合并小文件(默认false)
set hive.merge.mapredfiles=true;
--合并后文件的大小为256M左右 (默认64M)
set hive.exec.reducers.bytes.per.reducer=256000000;
--set hive.merge.size.per.task (默认256M);

insert overwrite table rtassets_ads.ads_bs_idx_m partition (PART_YM = '${batch_month}')
select '${batch_month}'                                       as DATA_MONTH,
       INNERORG_CODE,
       INNERORG_NAME,
       IDX_NAME,
       IDX_VALUE,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as DATA_TIME
from (
         select 'ptzhzzc'                                                          as IDX_NAME,
                INNERORG_CODE,
                INNERORG_NAME,
                cast(sum(PT_TOT_AST) / count(distinct part_ymd) as decimal(19, 6)) as IDX_VALUE
         from rtassets_ads.ads_bs_trd_ast_d
         where substr(part_ymd, 1, 6) = '${batch_month}'
         group by INNERORG_CODE, INNERORG_NAME
         union all
         select 'lrzzc'                                                            as IDX_NAME,
                INNERORG_CODE,
                INNERORG_NAME,
                cast(sum(LR_TOT_AST) / count(distinct part_ymd) as decimal(19, 6)) as IDX_VALUE
         from rtassets_ads.ads_bs_trd_ast_d
         where substr(part_ymd, 1, 6) = '${batch_month}'
         group by INNERORG_CODE, INNERORG_NAME
         union all
         select 'yybzzc'                                                        as IDX_NAME,
                INNERORG_CODE,
                INNERORG_NAME,
                cast(sum(TOT_AST) / count(distinct part_ymd) as decimal(19, 6)) as IDX_VALUE
         from rtassets_ads.ads_bs_crm_d
         where substr(part_ymd, 1, 6) = '${batch_month}'
         group by INNERORG_CODE, INNERORG_NAME
         union all
         select 'qdsr'                                                                     as IDX_NAME,
                INNERORG_CODE,
                INNERORG_NAME,
                cast(sum(IA_PD_FRONT_INCOME)  as decimal(19, 6)) as IDX_VALUE
         from rtassets_ads.ads_bs_crm_d
         where substr(part_ymd, 1, 6) = '${batch_month}'
         group by INNERORG_CODE, INNERORG_NAME
         union all
         select 'hdsr'                                                                    as IDX_NAME,
                INNERORG_CODE,
                INNERORG_NAME,
                cast(sum(IA_PD_BACK_INCOME) as decimal(19, 6)) as IDX_VALUE
         from rtassets_ads.ads_bs_crm_d
         where substr(part_ymd, 1, 6) = '${batch_month}'
         group by INNERORG_CODE, INNERORG_NAME
         union all
         select 'xssr',
                INNERORG_CODE,
                INNERORG_NAME,
                cast(sum(FIN_PD_SELL_INCOME)  as decimal(19, 6)) as IDX_VALUE
         from rtassets_ads.ads_bs_crm_d
         where substr(part_ymd, 1, 6) = '${batch_month}'
         group by INNERORG_CODE, INNERORG_NAME
         union all
         select 'lrrzzc'                                                        as IDX_NAME,
                INNERORG_CODE,
                INNERORG_NAME,
                cast(sum(FIN_AST) / count(distinct part_ymd) as decimal(19, 6)) as FIN_AST
         from rtassets_ads.ads_bs_trd_ast_d
         where substr(part_ymd, 1, 6) = '${batch_month}'
         group by INNERORG_CODE, INNERORG_NAME
         union all
         select 'lrrqzc'                                                        as IDX_NAME,
                INNERORG_CODE,
                INNERORG_NAME,
                cast(sum(SEC_AST) / count(distinct part_ymd) as decimal(19, 6)) as SEC_AST
         from rtassets_ads.ads_bs_trd_ast_d
         where substr(part_ymd, 1, 6) = '${batch_month}'
         group by INNERORG_CODE, INNERORG_NAME
     ) t;


