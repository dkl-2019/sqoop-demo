-------------------------------------------------------------------------#
-- 任务名：       dwd_ast_pstk_opt_hldp_ods_ois_kbssoptsett_opt_asset.sql
-- 目标表：       dwd_ast_pstk_opt_hldp 个股期权持仓
-- 源表：         rtassets_ods.ods_ois_kbssoptsett_opt_asset
-- 运行频度：     
-- 任务功能说明：个股期权持仓
-- 作者：        xujianliang
-- 创建日期：    20220909
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
alter table rtassets_dw.dwd_ast_pstk_opt_hldp drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_ois_kbssoptsett_opt_asset');

-- 更新数据
insert into table rtassets_dw.dwd_ast_pstk_opt_hldp partition (part_ymd = '${batch_date}' , ssys_tab = 'ods_ois_kbssoptsett_opt_asset') 
(
  data_date              --data_date
 ,etl_time               --etl_time
 ,busi_date              --业务日期
 ,ast_num                --资产编码
 ,ast_type_code          --资产类型代码
 ,hldp_type_code         --持仓类型代码
 ,cust_num               --客户编码
 ,cptl_acct              --资金账户
 ,crrc                   --币种
 ,trade_acct             --交易账户
 ,trade_mkt              --交易市场
 ,trade_boar             --交易板块
 ,pd_code                --产品代码
 ,innerorg_num           --内部机构编码
 ,trade_unit             --交易单元
 ,contr_num              --合约编码
 ,contr_code             --合约代码
 ,contr_name             --合约名称
 ,contr_type_code        --合约类型代码
 ,hldp_dir               --持仓方向
 ,covd_flag              --备兑标志
 ,subj_stk_type          --标的券类型
 ,subj_stk_code          --标的券代码
 ,yest_bal               --昨日余额
 ,today_bal              --本日余额
 ,aval_vol               --可用数量
 ,frz_vol                --冻结数量
 ,unfrz_vol              --解冻数量
 ,trade_frz_vol          --交易冻结数量
 ,trade_unfrz_vol        --交易解冻数量
 ,trade_intrns_vol       --交易在途数量
 ,trade_netting_vol      --交易扎差数量
 ,liqd_frz_vol           --清算冻结数量
 ,liqd_unfrz_vol         --清算解冻数量
 ,liqd_intrns_vol        --清算在途数量
 ,buy_in_cost            --买入成本
 ,rt_buy_in_cost         --实时买入成本
 ,pl_amt                 --盈亏金额
 ,rt_pl_amt              --实时盈亏金额
 ,contr_mval             --合约市值
 ,occp_buy_in_limit      --占用买入额度
 ,righ_money             --权利金
 ,flot_marg_unit         --浮动保证金单位
 ,marg                   --保证金
 ,covd_upp_lev_nums_qty  --备兑股份数量
 ,today_offset_pl        --本日平仓盈亏
 ,accu_offset_pl         --累计平仓盈亏
 ,flot_pl                --浮动盈亏
 ,dur_pd                 --存续期
 ,opt_pl                 --期权盈亏
 ,opt_rt_receipt         --期权实时开仓
 ,carding_vol            --梳理数量
 ,ssys_code              --系统来源代码
 ,ssys_num               --系统来源编码
)
select 
    '${batch_date}'                                                    as data_date           --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')             as etl_time            --etl_time
    ,'${batch_date}'                                                   as busi_date           --业务日期
    ,concat('ast','|','250','|',cast(t1.cuacct_code as string),'|',t1.opt_num,'|',t1.stkbd,'|',t1.stkpbu,'|',t1.trdacct,'|',t1.opt_side)  as ast_num      --资产编码
    ,'2'                                                               as ast_type_code          --资产类型代码
    ,'250'                                                             as hldp_type_code         --持仓类型代码
    ,cast(t1.cust_code   as string)                                    as cust_num               --客户编码
    ,cast(t1.cuacct_code as string)                                    as cptl_acct              --资金账户
    ,case when a2.std_code is null then concat('@',t1.currency) else a2.std_code end             as crrc                   --币种
    ,t1.trdacct                                                        as trade_acct             --交易账户
    ,case when a1.std_code is null then concat('@',t1.stkex) else a1.std_code end                as trade_mkt              --交易市场
    ,t1.stkbd                                                          as trade_boar             --交易板块
    ,t1.opt_code                                                       as pd_code                --产品代码
    ,cast(t1.int_org as string)                                        as innerorg_num           --内部机构编码
    ,t1.stkpbu                                                         as trade_unit             --交易单元
    ,t1.opt_num                                                        as contr_num              --合约编码
    ,t1.opt_code                                                       as contr_code             --合约代码
    ,t1.opt_name                                                       as contr_name             --合约名称
    ,t1.opt_type                                                       as contr_type_code        --合约类型代码
    ,t1.opt_side                                                       as hldp_dir               --持仓方向
    ,t1.opt_cvd_flag                                                   as covd_flag              --备兑标志
    ,t1.opt_undl_cls                                                   as subj_stk_type          --标的券类型
    ,t1.opt_undl_code                                                  as subj_stk_code          --标的券代码
    ,cast(t1.opt_prebln/1000   as decimal(24,6))                       as yest_bal               --昨日余额
    ,cast(t1.opt_bln/1000      as decimal(24,6))                       as today_bal              --本日余额
    ,cast(t1.opt_avl           as decimal(24,6))                       as aval_vol               --可用数量
    ,cast(t1.opt_frz           as decimal(24,6))                       as frz_vol                --冻结数量
    ,cast(t1.opt_ufz           as decimal(24,6))                       as unfrz_vol              --解冻数量
    ,cast(t1.opt_trd_frz       as decimal(24,6))                       as trade_frz_vol          --交易冻结数量
    ,cast(t1.opt_trd_ufz       as decimal(24,6))                       as trade_unfrz_vol        --交易解冻数量
    ,cast(t1.opt_trd_otd       as decimal(24,6))                       as trade_intrns_vol       --交易在途数量
    ,cast(t1.opt_trd_bln       as decimal(24,6))                       as trade_netting_vol      --交易扎差数量
    ,cast(t1.opt_clr_frz       as decimal(24,6))                       as liqd_frz_vol           --清算冻结数量
    ,cast(t1.opt_clr_ufz       as decimal(24,6))                       as liqd_unfrz_vol         --清算解冻数量
    ,cast(t1.opt_clr_otd       as decimal(24,6))                       as liqd_intrns_vol        --清算在途数量
    ,cast(t1.opt_bcost/1000         as decimal(24,6))                  as buy_in_cost            --买入成本
    ,cast(t1.opt_bcost_rlt/1000     as decimal(24,6))                  as rt_buy_in_cost         --实时买入成本
    ,cast(t1.opt_plamt/1000         as decimal(24,6))                  as pl_amt                 --盈亏金额
    ,cast(t1.opt_plamt_rlt/1000     as decimal(24,6))                  as rt_pl_amt              --实时盈亏金额
    ,cast(t1.opt_mkt_val/1000       as decimal(24,6))                  as contr_mval             --合约市值
    ,cast(t1.quota_val_used/1000    as decimal(24,6))                  as occp_buy_in_limit      --占用买入额度
    ,cast(t1.opt_premium/1000       as decimal(24,6))                  as righ_money             --权利金
    ,cast(t1.float_margin_unit/1000 as decimal(24,6))                  as flot_marg_unit         --浮动保证金单位
    ,cast(t1.opt_margin/1000        as decimal(24,6))                  as marg                   --保证金
    ,cast(t1.opt_cvd_asset     as decimal(24,6))                       as covd_upp_lev_nums_qty  --备兑股份数量
    ,cast(t1.opt_cls_profit/1000    as decimal(24,6))                  as today_offset_pl        --本日平仓盈亏
    ,null                                                              as accu_offset_pl         --累计平仓盈亏
    ,cast(t1.opt_float_profit/1000 as decimal(24,6))                   as flot_pl                --浮动盈亏
    ,cast(t1.opt_period as string)                                     as dur_pd                 --存续期
    ,cast(t1.opt_profit/1000         as decimal(24,6))                 as opt_pl                 --期权盈亏
    ,cast(t1.opt_daily_open_rlt/1000 as decimal(24,6))                 as opt_rt_receipt         --期权实时开仓
    ,cast(t1.combed_qty         as decimal(24,6))                      as carding_vol            --梳理数量
    ,'ois'                                                             as ssys_code              --系统来源代码
    ,concat(cast(t1.cuacct_code as string),'|',t1.opt_num,'|',t1.stkbd,'|',t1.stkpbu,'|',t1.trdacct,'|',t1.opt_side)        as ssys_num         -- 系统来源编码
from (select * from rtassets_ods.ods_ois_kbssoptsett_opt_asset where part_ymd='${batch_date}') t1     
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='ois' and src_code_type='stkex' and std_code_type ='trade_mkt')a1
on t1.stkex=a1.src_code
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='ois' and src_code_type='currency' and std_code_type ='crrc')a2
on t1.currency=a2.src_code
;