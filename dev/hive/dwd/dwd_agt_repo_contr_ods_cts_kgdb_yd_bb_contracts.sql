-------------------------------------------------------------------------#
-- 任务名：       dwd_agt_repo_contr_ods_cts_kgdb_yd_bb_contracts.q
-- 目标表：       dwd_agt_repo_contr
-- 源表：         rtassets_ods.ods_cts_kgdb_yd_bb_contracts
-- 运行频度：     每日
-- 任务功能说明：回购合约 -2
-- 作者：        xujianliang
-- 创建日期：    20220902
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
alter table rtassets_dw.dwd_agt_repo_contr drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_cts_kgdb_yd_bb_contracts');

-- 更新数据
insert into table rtassets_dw.dwd_agt_repo_contr partition (part_ymd = '${batch_date}' , ssys_tab = 'ods_cts_kgdb_yd_bb_contracts') 
(
 data_date             --DATA_DATE
,etl_time              --ETL_TIME
,contr_num             --合约编号
,contr_type_code       --合约类型代码
,cust_num              --客户编码
,trade_date            --交易日期
,buyb_date             --购回日期
,crrc                  --币种
,trade_mkt             --交易市场
,trade_boar            --交易板块
,trade_seat            --交易席位
,contr_seq             --合同序号
,mtch_num              --成交编号
,pd_incd               --产品内码
,pd_code               --产品代码
,pd_name               --产品名称
,repo_dir_code         --回购方向代码
,trade_behav           --交易行为
,entr_prc              --委托价格
,contr_amt             --合约金额
,buyb_prc              --购回价格
,buyb_vol              --购回数量
,buyb_amt              --购回金额
,guar_flag             --履约标志
,guar_amt              --履约金额
,share_trade_frz_cnt   --股份交易冻结数
,trade_frz_amt         --交易冻结金额
,pnlt_deal_flag        --罚息处理标志
,pnlt_amt              --罚息金额
,pnlt_inta_start_dt    --罚息计息开始日期
,orig_buyb_date        --原始购回日期
,repo_contr_flag_code  --回购合约标志代码
,supp_trade_trade_date --补充交易交易日期
,supp_trade_mtch_num   --补充交易成交编号
,scrt_acct             --证券账户
,cptl_acct             --资产账户
,fndr_type_code        --融出方类型代码
,pawn_type_code        --质权人类型代码
,entr_seq              --委托序号
,share_char_code       --股份性质代码
,init_contr_vol        --初始合约数量
,has_relp_vol          --已解押数量
,bstk_vol              --红股数量
,fin_intr              --融资利息
,has_buyb_amt          --已购回金额
,divd_amt              --红利金额
,fin_period            --融资期限
,init_fin_period       --初始融资期限
,init_fin_int_rate     --初始融资利率
,init_buyb_amt         --初始购回金额
,otc_fins_amt          --场外了结金额
,pnlt_deal_date        --罚息处理日期
,init_trade_date_pd    --初始交易日期
,init_contr_seq        --初始合同序号
,is_supp_contr         --是否补充合约
,end_dt                --结束日期
,ssys_code             --来源系统代码
,ssys_num              --来源系统编码
)
select                                                                     
     '${batch_date}'                                       as data_date                  --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_time                   --etl_time
    ,concat('agt','|','2','|',cast(t1.trd_date as string),'|',t1.secu_acc,'|',t1.trd_id,'|',t1.market,'|',t1.board,'|',t1.secu_code,'|',t1.matched_sn)            as CONTR_NUM                  --合约编号
    ,'2'                                                   as contr_type_code            --合约类型代码
    ,cast(t1.cust_code as string)                          as cust_num                   --客户编码
    ,cast(t1.trd_date as string)                           as trade_date                 --交易日期
    ,cast(t1.bb_date as string)                            as buyb_date                  --购回日期
    ,case when a2.std_code is null then concat('@',t1.currency) else a2.std_code end        as crrc                       --币种
    ,case when a1.std_code is null then concat('@',t1.market) else a1.std_code end          as trade_mkt                  --交易市场
    ,t1.board                                              as trade_boar                 --交易板块
    ,t1.seat                                               as trade_seat                 --交易席位
    ,null                                                  as contr_seq                  --合同序号
    ,t1.matched_sn                                         as mtch_num                   --成交编号
    ,cast(t1.secu_intl as string)                          as pd_incd                    --产品内码
    ,t1.secu_code                                          as pd_code                    --产品代码
    ,t1.secu_name                                          as pd_name                    --产品名称
    ,null                                                  as repo_dir_code              --回购方向代码
    ,t1.trd_id                                             as trade_behav                --交易行为
    ,cast(t1.order_price  as decimal(28,6))                as entr_prc                   --委托价格
    ,cast(t1.contract_amt as decimal(28,6))                as contr_amt                  --合约金额
    ,cast(t1.bb_price     as decimal(28,6))                as buyb_prc                   --购回价格
    ,cast(t1.bb_qty       as decimal(28,0))                as buyb_vol                   --购回数量
    ,cast(t1.bb_amt       as decimal(28,6))                as buyb_amt                   --购回金额
    ,null                                                  as guar_flag                  --履约标志
    ,null                                                  as guar_amt                   --履约金额
    ,null                                                  as share_trade_frz_cnt        --股份交易冻结数
    ,null                                                  as trade_frz_amt              --交易冻结金额
    ,null                                                  as pnlt_deal_flag             --罚息处理标志
    ,null                                                  as pnlt_amt                   --罚息金额
    ,null                                                  as pnlt_inta_start_dt         --罚息计息开始日期
    ,null                                                  as orig_buyb_date             --原始购回日期
    ,null                                                  as repo_contr_flag_code       --回购合约标志代码
    ,t1.bc_trd_date                                        as supp_trade_trade_date      --补充交易交易日期
    ,t1.bc_matched_sn                                      as supp_trade_mtch_num        --补充交易成交编号
    ,t1.secu_acc                                           as scrt_acct                  --证券账户
    ,cast(t1.account as string)                            as cptl_acct                  --资金账户
    ,null                                                  as fndr_type_code             --融出方类型代码
    ,null                                                  as pawn_type_code             --质权人类型代码
    ,t1.order_id                                           as entr_seq                   --委托序号
    ,null                                                  as share_char_code            --股份性质代码
    ,null                                                  as init_contr_vol             --初始合约数量
    ,null                                                  as has_relp_vol               --已解押数量
    ,null                                                  as bstk_vol                   --红股数量
    ,null                                                  as fin_intr                   --融资利息
    ,null                                                  as has_buyb_amt               --已购回金额
    ,null                                                  as divd_amt                   --红利金额
    ,null                                                  as fin_period                 --融资期限
    ,null                                                  as init_fin_period            --初始融资期限
    ,null                                                  as init_fin_int_rate          --初始融资利率
    ,null                                                  as init_buyb_amt              --初始购回金额
    ,null                                                  as otc_fins_amt               --场外了结金额
    ,null                                                  as pnlt_deal_date             --罚息处理日期
    ,null                                                  as init_trade_date_pd         --初始交易日期
    ,null                                                  as init_contr_seq             --初始合同序号
    ,null                                                  as is_supp_contr              --是否补充合约
    ,null                                                  as end_dt                     --结束日期
    ,'cts'                                                 as ssys_code                  --来源系统代码
    ,concat(cast(t1.trd_date as string),'|',t1.secu_acc,'|',t1.trd_id,'|',t1.market,'|',t1.board,'|',t1.secu_code,'|',t1.matched_sn)       as ssys_num      --来源系统编码
from (select * from rtassets_ods.ods_cts_kgdb_yd_bb_contracts where part_ymd='${batch_date}') t1     
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='cts' and src_code_type='market' and std_code_type ='trade_mkt')a1
on t1.market=a1.src_code
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='cts' and src_code_type='currency' and std_code_type ='crrc')a2
on t1.currency=a2.src_code
;