-------------------------------------------------------------------------#
-- 任务名：       dwd_ast_cptl_ods_mts_kgdbrzrq_capital.sql
-- 目标表：       dwd_ast_cptl   资金
-- 源表：         ods_mts_kgdbrzrq_capital
-- 运行频度：     每日
-- 任务功能说明：
-- 作者：        xujianliang
-- 创建日期：    20220825
-------------------------------------------------------------------------#
-- 修改人        修改日期     修改内容
--
--
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
alter table rtassets_dw.dwd_ast_cptl drop if exists partition (part_ymd = '${batch_date}' ,ssys_tab = 'ods_mts_kgdbrzrq_capital');

insert into table rtassets_dw.dwd_ast_cptl partition(part_ymd='${batch_date}',ssys_tab='ods_mts_kgdbrzrq_capital')
(  data_date         --DATA_DATE
  ,etl_time          --ETL_TIME
  ,busi_date         --业务日期
  ,ast_num           --资产编码
  ,ast_type_code     --资产类型代码
  ,cptl_type_code    --资金类型代码
  ,cust_num          --客户编码
  ,cptl_acct         --资金账户
  ,crrc              --币种
  ,yest_bal          --昨日余额
  ,acct_bal          --账户余额
  ,aval_amt          --可用金额
  ,frz_amt           --冻结金额
  ,unfrz_amt         --解冻金额
  ,excep_frz_amt     --异常冻结金额
  ,trade_frz_amt     --交易冻结金额
  ,trade_unfrz_amt   --交易解冻金额
  ,intrns_amt        --在途金额
  ,intrns_aval_amt   --在途可用金额
  ,dep_int_aggr      --存款利息积数
  ,dep_int           --存款利息
  ,cptl_state        --资金状态
  ,ssys_code         --系统来源代码
  ,ssys_num          --系统来源编码
  )
select '${batch_date}'                                        as data_date                    --DATA_DATE
     ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')   as etl_time                     --ETL_TIME
     ,'${batch_date}'                                         as busi_date                    --业务日期
     ,concat('ast','|','130','|',cast(t1.account as string),'|',t1.currency)               as ast_num           --资产编码
     ,'1'                                                     as ast_type_code                --资产类型代码
     ,'130'                                                   as cptl_type_code               --资金类型代码
     ,cast(t1.user_code as string)                            as cust_num                     --客户编码
     ,cast(t1.account as string)                              as cptl_acct                    --资金账户
     ,case when a1.std_code is null then concat('@',t1.currency) else a1.std_code end      as crrc              --币种
     ,null                                                    as yest_bal                     --昨日余额
     ,cast(t1.balance as decimal(24,6))                       as acct_bal                     --账户余额
     ,cast(t1.available as decimal(24,6))                     as aval_amt                     --可用金额
     ,cast(t1.frozen+t1.trd_frz as decimal(24,6))             as frz_amt                      --冻结金额
     ,null                                                    as unfrz_amt                    --解冻金额
     ,cast(t1.frozen  as decimal(24,6))                       as excep_frz_amt                --异常冻结金额
     ,cast(t1.trd_frz as decimal(24,6))                       as trade_frz_amt                --交易冻结金额
     ,null                                                    as trade_unfrz_amt              --交易解冻金额
     ,cast(t1.outstanding    as decimal(24,6))                as intrns_amt                   --在途金额
     ,cast(t1.otd_avl        as decimal(24,6))                as intrns_aval_amt              --在途可用金额
     ,cast(t1.saving_accrual as decimal(24,6))                as dep_int_aggr                 --存款利息积数
     ,cast(t1.saving_int     as decimal(24,6))                as dep_int                      --存款利息
     ,t1.status                                               as cptl_state                   --资金状态        
     ,'mts'                                                   as ssys_code                    --系统来源代码
     ,concat(cast(t1.account as string),'|',t1.currency)      as ssys_num                     --系统来源编码
from (select * from rtassets_ods.ods_mts_kgdbrzrq_capital where part_ymd='${batch_date}') t1
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='mts' and src_code_type='currency' and std_code_type ='crrc')a1
on t1.currency=a1.src_code
;
