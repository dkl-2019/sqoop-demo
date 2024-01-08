-------------------------------------------------------------------------#
--任务名：      dwd_ast_crdt_cust_ast_liab_ods_mts_kgdbrzrq_fisl_maintratio.q
--目标表：      dwd_ast_crdt_cust_ast_liab 信用客户资产负债 
--源表：        ods_mts_kgdbrzrq_fisl_maintratio
--              ods_mts_kgdbrzrq_capital
--运行频度：    每日
--任务功能说明：
--作者：        zhangyuanhui
--创建日期：    20221123
-------------------------------------------------------------------------#
--修改人        修改日期      修改内容
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
alter table rtassets_dw.dwd_ast_crdt_cust_ast_liab drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_mts_kgdbrzrq_fisl_maintratio');

insert into table rtassets_dw.dwd_ast_crdt_cust_ast_liab partition(part_ymd='${batch_date}',ssys_tab='ods_mts_kgdbrzrq_fisl_maintratio')
( 
 data_date          --data_date
,etl_time           --etl_time
,busi_date          --业务日期
,ast_num            --资产编码
,ast_type_code      --资产类型代码
,liab_type_code     --负债类型代码
,cust_num           --客户编码
,cptl_acct          --资金账户
,crrc               --币种
,innerorg_code      --内部机构代码
,rec_seq            --记录序号
,cptl_ast           --资金资产
,scrt_mval          --证券市值
,cust_tot_marg      --客户总保证金
,fin_liab           --融资负债
,shts_liab_mval     --融券负债市值
,fin_bal            --融资余额
,shts_bal           --融券余额
,fin_amt_fee        --融资金额费用
,has_occp_fin_lmt   --已占用融资额度
,has_occp_shts_lmt  --已占用融券额度
,no_rpay_fin_intr   --未偿还融资利息
,no_rpay_shts_fee   --未偿还融券费用
,fin_pl             --融资盈亏
,shts_pl            --融券盈亏
,guar_ratio         --担保品维持率
,kcm_mval           --科创板市值
,oth_guar_ast       --其他担保物资产
,gem_mval           --创业板市值
,marg_aval_bal      --保证金可用余额
,fin_marg_used_amt  --融资保证金已使用金额
,shts_marg_used_amt --融券保证金已使用金额
,fin_pl_cnvr        --融资盈亏折算
,shts_pl_cnvr       --融券盈亏折算
,ssys_code          --系统来源代码
,ssys_num           --系统来源编码
)
select '${batch_date}'                                                                  as data_date          --data_date
       ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')                           as etl_time           --etl_time
       ,'${batch_date}'                                                                 as busi_date          --业务日期
       ,concat('ast','|','330','|',cast(t1.account as string),'|',t1.currency)          as ast_num            --资产编码
       ,'3'                                                                             as ast_type_code      --资产类型代码
       ,'330'                                                                           as liab_type_code     --负债类型代码
       ,cast(t2.user_code as string)                                                    as cust_num           --客户编码
       ,cast(t1.account   as string)                                                    as cptl_acct          --资金账户
       ,case when a1.std_code is null then concat('@',t1.currency) else a1.std_code end as crrc               --币种
       ,cast(t1.branch    as string)                                                    as innerorg_code      --内部机构代码
       ,cast(t1.record_sn as string)                                                    as rec_seq            --记录序号
       ,cast(t1.capital           as decimal(16,2))                                     as cptl_ast           --资金资产
       ,cast(t1.market_value      as decimal(16,2))                                     as scrt_mval          --证券市值
       ,cast(t1.margin_amt        as decimal(16,2))                                     as cust_tot_marg      --客户总保证金
       ,cast(t1.fi_contract_amt   as decimal(16,2))                                     as fin_liab           --融资负债
       ,cast(t1.sl_market_value   as decimal(16,2))                                     as shts_liab_mval     --融券负债市值
       ,cast(t1.fi_bln            as decimal(16,2))                                     as fin_bal            --融资余额
       ,cast(t1.sl_bln            as decimal(16,2))                                     as shts_bal           --融券余额
       ,cast(t1.fi_amt_fee        as decimal(16,2))                                     as fin_amt_fee        --融资金额费用
       ,cast(t1.fi_used_credit    as decimal(16,2))                                     as has_occp_fin_lmt   --已占用融资额度
       ,cast(t1.sl_used_credit    as decimal(16,2))                                     as has_occp_shts_lmt  --已占用融券额度
       ,cast(t1.fi_unrepaid_int   as decimal(16,2))                                     as no_rpay_fin_intr   --未偿还融资利息
       ,cast(t1.sl_unrepaid_int   as decimal(16,2))                                     as no_rpay_shts_fee   --未偿还融券费用
       ,cast(t1.fi_profit_loss    as decimal(16,2))                                     as fin_pl             --融资盈亏
       ,cast(t1.sl_profit_loss    as decimal(16,2))                                     as shts_pl            --融券盈亏
       ,cast(t1.manit_ratio       as decimal(20,4))                                     as guar_ratio         --担保品维持率
       ,cast(t1.kc_market_value   as decimal(16,2))                                     as kcm_mval           --科创板市值
       ,cast(t1.out_capital       as decimal(16,2))                                     as oth_guar_ast       --其他担保物资产
       ,cast(t1.cy_market_value   as decimal(16,2))                                     as gem_mval           --创业板市值
       ,cast(t1.margin_avl        as decimal(16,2))                                     as marg_aval_bal      --保证金可用余额
       ,cast(t1.fi_margin_used    as decimal(16,2))                                     as fin_marg_used_amt  --融资保证金已使用金额
       ,cast(t1.sl_margin_used    as decimal(16,2))                                     as shts_marg_used_amt --融券保证金已使用金额
       ,cast(t1.fi_profit_loss_zs as decimal(16,2))                                     as fin_pl_cnvr        --融资盈亏折算
       ,cast(t1.sl_profit_loss_zs as decimal(16,2))                                     as shts_pl_cnvr       --融券盈亏折算
       ,'mts'                                                                           as ssys_code          --系统来源代码
       ,concat(cast(t1.account as string),'|',t1.currency)                              as ssys_num           --系统来源编码
from (select * from rtassets_ods.ods_mts_kgdbrzrq_fisl_maintratio where part_ymd='${batch_date}') t1
left join (select * from rtassets_ods.ods_mts_kgdbrzrq_capital where part_ymd='${batch_date}') t2
on t1.account=t2.account and t1.currency = t2.currency
left join (select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='mts' and src_code_type='currency' and std_code_type ='crrc')a1
on t1.currency=a1.src_code
;