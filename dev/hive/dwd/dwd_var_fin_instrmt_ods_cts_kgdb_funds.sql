-------------------------------------------------------------------------#
-- 任务名：       dwd_var_fin_instrmt_ods_cts_kgdb_funds.q
-- 目标表：       dwd_var_fin_instrmt
-- 源表：         rtassets_ods.ods_cts_kgdb_funds
-- 运行频度：     每日
-- 任务功能说明：金融工具 
-- 作者：        xujianliang
-- 创建日期：    20221024
-------------------------------------------------------------------------#
--不启用锁特性
set hive.support.concurrency=false;
--动态分区严格模式
set hive.exec.dynamic.partition.mode=strict;
--日常跑批脚本关闭动态分区特性
set hive.exec.dynamic.partition=false;
--不进行parquet压缩
set parquet.compression=uncompressed;
set mapreduce.job.queuename=root.users.rtassets;

--删除数据
alter table rtassets_dw.dwd_var_fin_instrmt drop if exists partition (part_ymd='${batch_date}',ssys_tab = 'ods_cts_kgdb_funds');

-- 更新数据
insert into table rtassets_dw.dwd_var_fin_instrmt partition (part_ymd='${batch_date}',ssys_tab = 'ods_cts_kgdb_funds')
(
   data_date
   ,etl_time
   ,pd_num                    --产品编码
   ,pd_type_code              --产品类型代码
   ,fin_instrmt_type_code     --金融工具类型代码
   ,orig_scrt_class           --原证券类别
   ,pd_incd                   --产品内码
   ,pd_code                   --产品代码
   ,pd_name                   --产品名称
   ,trade_mkt                 --交易市场
   ,crrc                      --币种
   ,list_date                 --上市日期
   ,end_date                  --终止日期
   ,pd_state                  --产品状态
   ,ssys_code                 --系统来源代码
   ,ssys_num                  --系统来源编码
)
select
     '${batch_date}'                                           as data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')     as etl_time
    ,concat(concat('var|pof|0204|',t1.fund_code))   as pd_num                              --产品编码
    ,'1'                                                       as pd_type_code                        --产品类型代码
    ,'pof'                                                     as fin_instrmt_type_code               --金融工具类型代码
    ,null                                                      as orig_scrt_class                     --原证券类别
    ,cast(t1.fund_intl as string)                              as pd_incd                             --产品内码
    ,t1.fund_code                                              as pd_code                             --产品代码
    ,t1.full_name                                              as pd_name                             --产品名称
    ,'0204'                                                    as trade_mkt                         --交易市场
    ,case when a3.std_code is null then concat('@',t1.currency) else a3.std_code end      as crrc     --币种
    ,null                                                      as list_date                           --上市日期
    ,null                                                      as end_date                            --终止日期
    ,t1.fund_status                                            as pd_state                            --产品状态
    ,'cts2'                                                     as ssys_code                           --系统来源代码
    ,cast(t1.fund_intl as string)                              as ssys_num                            --系统来源编码
from (select * from rtassets_ods.ods_cts_kgdb_funds where part_ymd='${batch_date}') t1
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='cts' and src_code_type='currency' and std_code_type ='crrc')a3
on t1.currency=a3.src_code
;