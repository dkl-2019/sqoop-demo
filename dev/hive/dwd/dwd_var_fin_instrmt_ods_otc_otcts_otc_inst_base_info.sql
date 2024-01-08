-------------------------------------------------------------------------#
-- 任务名：       dwd_var_fin_instrmt_ods_otc_otcts_otc_inst_base_info.q
-- 目标表：       dwd_var_fin_instrmt
-- 源表：         rtassets_ods.ods_otc_otcts_otc_inst_base_info
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
alter table rtassets_dw.dwd_var_fin_instrmt drop if exists partition (part_ymd='${batch_date}',ssys_tab = 'ods_otc_otcts_otc_inst_base_info');

-- 更新数据
insert into table rtassets_dw.dwd_var_fin_instrmt partition (part_ymd='${batch_date}',ssys_tab = 'ods_otc_otcts_otc_inst_base_info')
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
    ,concat(concat('var|',a1.std_code,'|',nvl(a2.std_code,concat('@',t1.mkt_code)),'|',t1.inst_code))  as pd_num   --产品编码
    ,'1'                                                       as pd_type_code                                        --产品类型代码
    ,case when a1.std_code is null then concat('@',t1.inst_type) else a1.std_code end   as fin_instrmt_type_code      --金融工具类型代码
    ,t1.inst_type                                              as orig_scrt_class                                     --原证券类别
    ,cast(t1.inst_sno as string)                               as pd_incd                                             --产品内码
    ,t1.inst_code                                              as pd_code                                             --产品代码
    ,t1.inst_sname                                             as pd_name                                             --产品名称
    ,case when a2.std_code is null then concat('@',t1.mkt_code) else a2.std_code end    as trade_mkt                  --交易市场
    ,case when a3.std_code is null then concat('@',t1.currency) else a3.std_code end    as crrc                       --币种
    ,null                                                      as list_date                                           --上市日期
    ,null                                                      as end_date                                            --终止日期
    ,null                                                      as pd_state                                            --产品状态
    ,'otc'                                                     as ssys_code                                           --系统来源代码
    ,concat(t1.iss_code,'|',cast(t1.inst_sno as string))       as ssys_num                                            --系统来源编码
from (select * from rtassets_ods.ods_otc_otcts_otc_inst_base_info where part_ymd='${batch_date}') t1
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='otc' and src_code_type='inst_type' and std_code_type ='fin_instrmt_type_code')a1
on t1.inst_type=a1.src_code
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='otc' and src_code_type='mkt_code' and std_code_type ='trade_mkt')a2
on t1.mkt_code=a2.src_code
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='otc' and src_code_type='currency' and std_code_type ='crrc')a3
on t1.currency=a3.src_code
;
