-------------------------------------------------------------------------#
-- 任务名：       dwd_agt_crd_contr_ods_mts_kgdbrzrq_fisl_contract_opened.q
-- 目标表：       dwd_agt_crd_contr 融资融券合约
-- 源表：         rtassets_ods.ods_mts_kgdbrzrq_fisl_contract_opened
-- 运行频度：     
-- 任务功能说明：融资融券合约
-- 作者：        xujianliang
-- 创建日期：    202209015
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
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.job.queuename=root.users.rtassets;

--删除数据
alter table rtassets_dw.dwd_agt_crd_contr drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_mts_kgdbrzrq_fisl_contract_opened');

-- 更新数据
insert into table rtassets_dw.dwd_agt_crd_contr partition (part_ymd = '${batch_date}' , ssys_tab = 'ods_mts_kgdbrzrq_fisl_contract_opened') 
(
    data_date                --DATA_DATE
    ,etl_time                 --ETL_TIME
    ,contr_num                --合约编号
    ,contr_type_code          --合约类型代码
    ,crd_contr_num            --融资融券合同号
    ,readrcash_num            --头寸编号
    ,trade_date               --交易日期
    ,receipt_date             --开仓日期
    ,cust_num                 --客户编码
    ,cust_class               --客户类别
    ,cptl_acct                --资金帐户
    ,crrc                     --币种
    ,ast_class                --资产分类
    ,dept_code                --营业部代码
    ,ext_org                  --外部机构
    ,shaholder_code           --股东代码
    ,shaholder_name           --股东名称
    ,trade_mkt                --交易市场
    ,trade_boar               --交易板块
    ,trade_seat               --交易席位
    ,entr_num                 --委托编号
    ,scrt_incd                --证券内码
    ,scrt_code                --证券代码
    ,scrt_name                --证券名称
    ,scrt_class               --证券类别
    ,trade_behav              --交易行为
    ,crd_type                 --融资融券类型
    ,mtch_vol                 --成交数量
    ,mtch_prc                 --成交价格
    ,trade_amt                --交易金额
    ,entr_frz_amt             --委托冻结金额
    ,whdw_vol                 --撤单数量
    ,contr_vol                --合约数量
    ,fin_amt                  --融资金额
    ,contr_intr               --合约利息
    ,crd_fee                  --融资融券费用
    ,crd_limit_ocp_fee        --融资融券额度占用费
    ,crd_mgt_fee              --融资融券管理费
    ,marg_ratio               --保证金比例
    ,occp_marg                --占用保证金
    ,shts_paid_vol            --融券已还数量
    ,fin_paid_amt             --融资已还金额
    ,paid_intr                --已还利息
    ,tdy_shts_rt_repy         --当日融券实时归还
    ,tdy_fin_rt_repy          --当日融资实时归还
    ,start_inta_date          --开始计息日期
    ,contr_int_rate           --合约利率
    ,intr_arrg                --利息积数
    ,intr_contr_amt           --利息合约金额
    ,pnlt_inta_start_dt       --罚息计息开始日期
    ,pnlt_arrg                --罚息积数
    ,expe_pnlt                --预计罚息
    ,pnlt_int_rate            --罚息利率
    ,intr_deal_flag           --利息处理标志
    ,pnlt_deal_flag           --罚息处理标志
    ,contr_state              --合约状态
    ,contr_fins_date          --合约了结日期
    ,contr_prc                --合约价格
    ,flot_inta_pre_intr       --浮动计息预利息
    ,accu_trade_days          --累计的交易天数
    ,trade_date_inta          --交易日期计息
    ,ovdue_stop_days          --超期停牌天数
    ,free_int_ex              --罚息_EX
    ,repaid_free_int          --已还罚息
    ,expiration_date          --到期日期
    ,rfn_days                 --转融天数
    ,rfn_receipt_date         --转融开仓日期
    ,rfn_mkt                  --转融市场
    ,rfn_boar                 --转融板块
    ,rfn_contr_seq            --转融合同序号
    ,rpay_seq_num             --偿还顺序号
    ,pre_liqd_intr            --上一清算利息
    ,extension_cnt            --展期次数
    ,cmpd_int_flag            --复利标志
    ,comp_int_arrg            --复利积数
    ,comp_int_int_rate        --复利利率
    ,comp_int_amt             --复利金额
    ,seg_comp_int_amt         --分段复利金额
    ,comp_int_inta_date       --复利计息日期
    ,repaid_cmpd_int          --已还复利
    ,contr_extension_state    --合约展期状态
    ,comp_int_prin            --复利本金
    ,tdy_add_contr_cost       --当日新增合约成本
    ,tot_contr_cost           --总合约成本
    ,ssys_code                --系统来源代码
    ,ssys_num                 -- 系统来源编码
)
select 
    '${batch_date}'                                                    as data_date           --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')             as etl_time            --etl_time
    ,concat('agt','|','5',cast(t1.opening_date as string),'|',cast(t1.cust_code as string),'|',t1.market,'|',t1.board,'|',t1.order_id)          as contr_num                --合约编号
    ,'5'                                                               as contr_type_code          --合约类型代码
    ,cast(t1.agreement_no as string)                                   as crd_contr_num            --融资融券合同号
    ,cast(t1.cash_no      as string)                                   as readrcash_num            --头寸编号
    ,cast(t1.trd_date     as string)                                   as trade_date               --交易日期
    ,cast(t1.opening_date as string)                                   as receipt_date             --开仓日期
    ,cast(t1.cust_code    as string)                                   as cust_num                 --客户编码
    ,t1.cust_cls                                                       as cust_class               --客户类别
    ,cast(t1.account as string)                                        as cptl_acct                --资金帐户
    ,case when a2.std_code is    null then concat('@',t1.currency) else a2.std_code end                as crrc                --币种代码
    ,t1.acc_cls                                                        as ast_class                --资产分类
    ,cast(t1.branch   as string)                                       as dept_code                --营业部代码
    ,cast(t1.ext_inst as string)                                       as ext_org                  --外部机构
    ,t1.secu_acc                                                       as shaholder_code           --股东代码
    ,t1.secu_acc_name                                                  as shaholder_name           --股东名称
    ,case when a1.std_code is null then concat('@',t1.market) else a1.std_code end                  as trade_mkt                --交易市场
    ,t1.board                                                          as trade_boar               --交易板块
    ,t1.seat                                                           as trade_seat               --交易席位
    ,t1.order_id                                                       as entr_num                 --委托编号
    ,cast(t1.secu_intl as string)                                      as scrt_incd                --证券内码
    ,t1.secu_code                                                      as scrt_code                --证券代码
    ,t1.secu_name                                                      as scrt_name                --证券名称
    ,t1.secu_cls                                                       as scrt_class               --证券类别
    ,t1.trd_id                                                         as trade_behav              --交易行为
    ,t1.contract_type                                                  as crd_type                 --融资融券类型
    ,cast(t1.order_qty      as decimal(24,6))                          as mtch_vol                 --成交数量
    ,cast(t1.order_price    as decimal(24,6))                          as mtch_prc                 --成交价格
    ,cast(t1.order_amt      as decimal(24,6))                          as trade_amt                --交易金额
    ,cast(t1.order_frz_amt  as decimal(24,6))                          as entr_frz_amt             --委托冻结金额
    ,cast(t1.withdrawn_qty  as decimal(24,6))                          as whdw_vol                 --撤单数量
    ,cast(t1.contract_qty   as decimal(24,6))                          as contr_vol                --合约数量
    ,cast(t1.contract_amt   as decimal(24,6))                          as fin_amt                  --融资金额
    ,cast(t1.contract_int   as decimal(24,6))                          as contr_intr               --合约利息
    ,cast(t1.contract_fee   as decimal(24,6))                          as crd_fee                  --融资融券费用
    ,cast(t1.occuped_fee    as decimal(24,6))                          as crd_limit_ocp_fee        --融资融券额度占用费
    ,cast(t1.manager_fee    as decimal(24,6))                          as crd_mgt_fee              --融资融券管理费
    ,cast(t1.margin_ratio   as decimal(24,6))                          as marg_ratio               --保证金比例
    ,cast(t1.margin_amt     as decimal(24,6))                          as occp_marg                --占用保证金
    ,cast(t1.repaid_qty     as decimal(24,6))                          as shts_paid_vol            --融券已还数量
    ,cast(t1.repaid_amt     as decimal(24,6))                          as fin_paid_amt             --融资已还金额
    ,cast(t1.repaid_int     as decimal(24,6))                          as paid_intr                --已还利息
    ,cast(t1.rlt_repaid_qty as decimal(24,6))                          as tdy_shts_rt_repy         --当日融券实时归还
    ,cast(t1.rlt_repaid_amt as decimal(24,6))                          as tdy_fin_rt_repy          --当日融资实时归还
    ,t1.cal_int_date                                                   as start_inta_date          --开始计息日期
    ,cast(t1.contract_int_rate    as decimal(24,8))                    as contr_int_rate           --合约利率
    ,cast(t1.contract_int_accrual as decimal(24,6))                    as intr_arrg                --利息积数
    ,cast(t1.int_contract_amt     as decimal(24,6))                    as intr_contr_amt           --利息合约金额
    ,cast(t1.free_date            as decimal(24,6))                    as pnlt_inta_start_dt       --罚息计息开始日期
    ,cast(t1.free_accrual         as decimal(24,6))                    as pnlt_arrg                --罚息积数
    ,cast(t1.free_int             as decimal(24,6))                    as expe_pnlt                --预计罚息
    ,cast(t1.free_int_rate        as decimal(24,6))                    as pnlt_int_rate            --罚息利率
    ,t1.int_flag                                                       as intr_deal_flag           --利息处理标志
    ,t1.free_flag                                                      as pnlt_deal_flag           --罚息处理标志
    ,t1.contract_status                                                as contr_state              --合约状态
    ,cast(t1.closing_date    as decimal(24,6))                         as contr_fins_date          --合约了结日期
    ,cast(t1.closing_price   as decimal(24,6))                         as contr_prc                --合约价格
    ,cast(t1.contract_int_ex as decimal(24,6))                         as flot_inta_pre_intr       --浮动计息预利息
    ,cast(t1.trd_days        as decimal(24,6))                         as accu_trade_days          --累计的交易天数
    ,cast(t1.free_days       as decimal(24,6))                         as trade_date_inta          --交易日期计息
    ,cast(t1.susp_days       as decimal(24,6))                         as ovdue_stop_days          --超期停牌天数
    ,cast(t1.free_int_ex     as decimal(24,6))                         as free_int_ex              --罚息_ex
    ,cast(t1.repaid_free_int as decimal(24,6))                         as repaid_free_int          --已还罚息
    ,cast(t1.expiration_date as decimal(24,6))                         as expiration_date          --到期日期
    ,cast(t1.zfisl_days      as decimal(24,6))                         as rfn_days                 --转融天数
    ,cast(t1.zopening_date   as decimal(24,6))                         as rfn_receipt_date         --转融开仓日期
    ,t1.zmarket                                                        as rfn_mkt                  --转融市场
    ,t1.zboard                                                         as rfn_boar                 --转融板块
    ,t1.zorder_id                                                      as rfn_contr_seq            --转融合同序号
    ,cast(t1.repay_order_sn    as decimal(24,6))                       as rpay_seq_num             --偿还顺序号
    ,cast(t1.contract_int_last as decimal(24,6))                       as pre_liqd_intr            --上一清算利息
    ,cast(t1.extd_cnt          as decimal(24,6))                       as extension_cnt            --展期次数
    ,t1.cmpd_int_flag                                                  as cmpd_int_flag            --复利标志
    ,cast(t1.cmpd_int_accrual as decimal(24,6))                        as comp_int_arrg            --复利积数
    ,cast(t1.cmpd_int_rate    as decimal(24,6))                        as comp_int_int_rate        --复利利率
    ,cast(t1.cmpd_int         as decimal(24,6))                        as comp_int_amt             --复利金额
    ,cast(t1.cmpd_int_ex      as decimal(24,6))                        as seg_comp_int_amt         --分段复利金额
    ,cast(t1.cmpd_int_date    as decimal(24,6))                        as comp_int_inta_date       --复利计息日期
    ,cast(t1.repaid_cmpd_int  as decimal(24,6))                        as repaid_cmpd_int          --已还复利
    ,t1.extend_status                                                  as contr_extension_state    --合约展期状态
    ,cast(t1.cmpd_prin           as decimal(24,6))                     as comp_int_prin            --复利本金
    ,cast(t1.contract_cost       as decimal(24,6))                     as tdy_add_contr_cost       --当日新增合约成本
    ,cast(t1.total_contract_cost as decimal(24,6))                     as tot_contr_cost           --总合约成本
    ,'mts'                                                             as ssys_code               --系统来源代码
    ,concat(cast(t1.opening_date as string),'|',cast(t1.cust_code as string),'|',t1.market,'|',t1.board,'|',t1.order_id)     as ssys_num         -- 系统来源编码
from (select * from rtassets_ods.ods_mts_kgdbrzrq_fisl_contract_opened where part_ymd='${batch_date}' ) t1
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='mts' and src_code_type='market' and std_code_type ='trade_mkt')a1
on t1.market=a1.src_code
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='mts' and src_code_type='currency' and std_code_type ='crrc')a2
on t1.currency=a2.src_code
;