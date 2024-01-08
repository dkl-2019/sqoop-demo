-------------------------------------------------------------------------#
--任务名：dwd_ast_exch_hldp_ods_cts_kgdb_shares.q
--目标表：dwd_ast_exch_hldp 场内持仓 
--源表：ods_cts_kgdb_shares
--运行频度：每日
--任务功能说明：
--作者：xujianliang
--创建日期：20220829
-------------------------------------------------------------------------#
--修改人修改日期修改内容
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
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.job.queuename=root.users.rtassets;

--删除数据
alter table rtassets_dw.dwd_ast_exch_hldp drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_cts_kgdb_shares');

insert into table rtassets_dw.dwd_ast_exch_hldp partition(part_ymd='${batch_date}',ssys_tab='ods_cts_kgdb_shares')
( 
   data_date               --DATA_DATE
  ,etl_time                --ETL_TIME
  ,busi_date               --业务日期
  ,ast_num                 --资产编码
  ,ast_type_code           --资产类型代码
  ,hldp_type_code          --持仓类型代码
  ,cust_num                --客户编码
  ,cptl_acct               --资金账户
  ,crrc                    --币种
  ,trade_acct              --交易账户
  ,trade_mkt               --交易市场
  ,trade_boar              --交易板块
  ,pd_incd                 --产品内码
  ,pd_code                 --产品代码
  ,scrt_type_code          --证券类型代码
  ,innerorg_num            --内部机构编码
  ,ext_org_num             --外部机构编码
  ,reg_org_num             --登记机构编码
  ,seat                    --席位
  ,yest_bal                --昨日余额
  ,share_bal               --股份余额
  ,share_aval_vol          --股份可用数量
  ,non_circ_shares_vol     --非流通股数量
  ,share_frz               --股份冻结
  ,share_unfrz             --股份解冻
  ,yest_buy_in_cost        --昨日买入成本
  ,cur_buy_in_cost         --当前买入成本
  ,yest_cost               --昨日成本
  ,cur_cost                --当前成本
  ,pl_amt                  --盈亏金额
  ,excep_frz_vol           --异常冻结数量
  ,trade_frz_vol           --交易冻结数量
  ,intrns_aval_vol         --在途可用数量
  ,intrns_vol              --在途数量
  ,pd_mval                 --产品市值
  ,ssys_code               --系统来源代码
  ,ssys_num                --系统来源编码
)
select '${batch_date}'                                           as data_date                     --data_date
       ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')    as etl_time                      --etl_time
       ,'${batch_date}'                                          as busi_date                     -- 业务日期
       ,concat('ast','|','210','|',cast(t1.account as string),'|',t1.secu_acc,'|',cast(t1.secu_intl as string),'|',t1.seat,'|',cast(t1.ext_inst as string)) as ast_num     -- 资产编码
       ,'2'                                                      as ast_type_code                 --资产类型代码
       ,'210'                                                    as hldp_type_code                --持仓类型代码
       ,cast(t1.cust_code as string)                             as cust_num                      --客户编码
       ,cast(t1.account   as string)                             as cptl_acct                     --资金账户
       ,case when a2.std_code is null then concat('@',t1.currency) else a2.std_code end              as crrc                          --币种
       ,t1.secu_acc                                              as trade_acct                    --交易账户
       ,case when a1.std_code is null then concat('@',t1.market) else a1.std_code end                as trade_mkt                     --交易市场
       ,t1.board                                                 as trade_boar                    --交易板块
       ,cast(t1.secu_intl as string)                             as pd_incd                       --产品内码
       ,null                                                     as pd_code                       --产品代码
       ,t1.secu_cls                                              as scrt_type_code                --证券类型代码
       ,cast(t1.branch   as string)                              as innerorg_num                  --内部机构编码
       ,cast(t1.ext_inst as string)                              as ext_org_num                   --外部机构编码
       ,null                                                     as reg_org_num                   --登记机构编码
       ,t1.seat                                                  as seat                          --席位
       ,null                                                     as yest_bal                      --昨日余额
       ,cast(t1.share_bln as decimal(24,6))                      as share_bal                     --股份余额
       ,cast(t1.share_avl as decimal(24,6))                      as share_aval_vol                --股份可用数量
       ,cast(t1.share_untrade_qty as decimal(24,6))              as non_circ_shares_vol           --非流通股数量
       ,cast((t1.share_frz+t1.share_trd_frz) as decimal(24,6))   as share_frz                     --股份冻结
       ,null                                                     as share_unfrz                   --股份解冻
       ,cast(t1.last_cost2    as decimal(24,6))                  as yest_buy_in_cost              --昨日买入成本
       ,cast(t1.current_cost2 as decimal(24,6))                  as cur_buy_in_cost               --当前买入成本
       ,cast(t1.last_cost     as decimal(24,6))                  as yest_cost                     --昨日成本
       ,cast(t1.current_cost  as decimal(24,6))                  as cur_cost                      --当前成本
       ,null                                                     as pl_amt                        --盈亏金额
       ,cast(t1.share_frz     as decimal(24,6))                  as excep_frz_vol                 --异常冻结数量
       ,cast(t1.share_trd_frz as decimal(24,6))                  as trade_frz_vol                 --交易冻结数量
       ,cast(t1.share_otd_avl as decimal(24,6))                  as intrns_aval_vol               --在途可用数量
       ,cast(t1.share_otd     as decimal(24,6))                  as intrns_vol                    --在途数量
       ,cast(t1.mkt_val       as decimal(24,6))                  as pd_mval                       --产品市值
       ,'cts'                                                    as ssys_code                     --系统来源代码
       ,concat(cast(t1.account as string),'|',t1.secu_acc,'|',cast(t1.secu_intl as string),'|',t1.seat,'|',cast(t1.ext_inst as string))       as ssys_num   -- 系统来源编码
from (select * from rtassets_ods.ods_cts_kgdb_shares where part_ymd='${batch_date}') t1
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='cts' and src_code_type='market' and std_code_type ='trade_mkt')a1
on t1.market=a1.src_code
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='cts' and src_code_type='currency' and std_code_type ='crrc')a2
on t1.currency=a2.src_code
;