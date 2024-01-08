-------------------------------------------------------------------------#
-- 任务名：       dwd_agt_crd_crdt_agmt_ods_mts_kgdbrzrq_fisl_agreement.q
-- 目标表：       dwd_agt_crd_crdt_agmt
-- 源表：         rtassets_ods.ods_mts_kgdbrzrq_fisl_agreement
-- 运行频度：     每日
-- 任务功能说明：融资融券授信合同
-- 作者：        xujianliang
-- 创建日期：    202209028
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
alter table rtassets_dw.dwd_agt_crd_crdt_agmt drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_mts_kgdbrzrq_fisl_agreement');

-- 更新数据
insert into table rtassets_dw.dwd_agt_crd_crdt_agmt partition (part_ymd = '${batch_date}' , ssys_tab = 'ods_mts_kgdbrzrq_fisl_agreement') 
(
    data_date                --data_date
    ,etl_time                --etl_time
    ,agmt_num                --合同编码
    ,agmt_type_code          --合同类型代码
    ,agmt_id                 --合同ID
    ,cust_num                --客户编码
    ,cptl_acct               --资金账户
    ,crrc                    --币种
    ,crdt_lmt                --信用额度
    ,crd_int_rate_group      --融资融券利率组
    ,crdt_lmt_type           --信用额度类型
    ,crdt_lmt_coef           --信用额度系数
    ,fin_crdt_lmt_coef       --融资信用额度系数
    ,max_crdt_lmt_upper      --最大信用额度上限
    ,min_crdt_lmt_lower      --最小信用额度下限
    ,contr_max_days          --合约最大天数
    ,agt_start_dt            --协议开始日期
    ,agt_end_dt              --协议结束日期
    ,ext_acct_num            --外部帐号
    ,intr_cal_mode           --利息计算模式
    ,marg_liab_mode          --保证金负债模式
    ,intr_comp_mode          --派息补偿方式
    ,bstk_comp_mode          --红股补偿方式
    ,plac_comp_mode          --配股补偿方式
    ,bak_crdt_cardin         --备份信用基数
    ,fin_crdt_lmt            --融资信用额度
    ,shts_crdt_lmt           --融券信用额度
    ,fin_crdt_lmt_frz        --融资信用额度冻结
    ,shts_crdt_lmt_frz       --融券信用额度冻结
    ,cash_num                --头寸编号
    ,src_cash_num            --源头寸编号
    ,fin_daily_rate          --融资日利率
    ,shts_daily_rate         --融券日利率
    ,auto_lmt_adj_flag       --自动额度调整标志
    ,auto_lmt_crdt_coef      --自动额度授信系数
    ,agmt_state_code         --状态
    ,memo                    --备注
    ,ssys_code               --来源系统代码
    ,ssys_num                --来源系统编码 
)
select    
     '${batch_date}'                                         as data_date                    --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')   as etl_time                     --etl_time
    ,concat('agt','|','1','|',cast(t1.account as string),'|',t1.currency)        as agmt_num           --合同编码
    ,'1'                                                     as agmt_type_code     --合同类型代码
    ,cast(t1.agreement_no as string)                         as agmt_id            --合同ID
    ,cast(t1.cust_code as string)                            as cust_num           --客户编码
    ,cast(t1.account as string)                              as cptl_acct          --资金账户
    ,case when a1.std_code is null then concat('@',t1.currency) else a1.std_code end            as crrc               --币种
    ,cast(t1.credit_line    as decimal(24,6))                as crdt_lmt           --信用额度
    ,cast(t1.credit_int_grp as decimal(24,6))                as crd_int_rate_group --融资融券利率组
    ,cast(t1.credit_type     as decimal(24,6))               as crdt_lmt_type      --信用额度类型
    ,cast(t1.credit_degree   as decimal(24,6))               as crdt_lmt_coef      --信用额度系数
    ,cast(t1.fi_credit_ratio as decimal(24,6))               as fin_crdt_lmt_coef  --融资信用额度系数
    ,cast(t1.max_credit      as decimal(24,6))               as max_crdt_lmt_upper --最大信用额度上限
    ,cast(t1.min_credit      as decimal(24,6))               as min_crdt_lmt_lower --最小信用额度下限
    ,cast(t1.contract_term   as decimal(24,0))               as contr_max_days     --合约最大天数
    ,t1.agt_bgn_date                                         as agt_start_dt       --协议开始日期
    ,t1.agt_end_date                                         as agt_end_dt         --协议结束日期
    ,t1.ext_account                                          as ext_acct_num       --外部帐号
    ,t1.cal_int_mode                                         as intr_cal_mode      --利息计算模式
    ,t1.debt_repay_mode                                      as marg_liab_mode     --保证金负债模式
    ,t1.coupon_offer                                         as intr_comp_mode     --派息补偿方式
    ,t1.bonus_offer                                          as bstk_comp_mode     --红股补偿方式
    ,t1.alloted_offer                                        as plac_comp_mode     --配股补偿方式
    ,cast(t1.credit_degreebak as decimal(24,6))              as bak_crdt_cardin    --备份信用基数
    ,cast(t1.fi_credit        as decimal(24,6))              as fin_crdt_lmt       --融资信用额度
    ,cast(t1.sl_credit        as decimal(24,6))              as shts_crdt_lmt      --融券信用额度
    ,cast(t1.fi_credit_frz    as decimal(24,6))              as fin_crdt_lmt_frz   --融资信用额度冻结
    ,cast(t1.sl_credit_frz    as decimal(24,6))              as shts_crdt_lmt_frz  --融券信用额度冻结
    ,cast(t1.cash_no as string)                              as cash_num           --头寸编号
    ,cast(t1.cash_no_bak as string)                          as src_cash_num       --源头寸编号
    ,cast(t1.fi_int_rate as decimal(24,6))                   as fin_daily_rate     --融资日利率
    ,cast(t1.sl_int_rate as decimal(24,6))                   as shts_daily_rate    --融券日利率
    ,t1.auto_flag                                            as auto_lmt_adj_flag  --自动额度调整标志
    ,cast(t1.auto_credit_ratio as decimal(24,6))             as auto_lmt_crdt_coef --自动额度授信系数
    ,t1.status                                               as agmt_state_code    --状态
    ,t1.remark                                               as memo               --备注
    ,'cts'                                                   as ssys_code          --来源系统代码
    ,concat(cast(t1.account as string),'|',t1.currency)      as ssys_num           --来源系统编码
from (select * from rtassets_ods.ods_mts_kgdbrzrq_fisl_agreement where part_ymd='${batch_date}') t1         
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='cts' and src_code_type='currency' and std_code_type ='crrc')a1
on t1.currency=a1.src_code
;