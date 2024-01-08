-------------------------------------------------------------------------#
--任务名：dwd_ast_otc_hldp_ods_otc_otcts_otc_asset.sql
--目标表：dwd_ast_otc_hldp 场外持仓
--源表：  ods_otc_otcts_otc_asset
--源表：  ods_otc_otcts_otc_inst_base_info
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
--取消小表加载至内存中
set hive.auto.convert.join = false;
set mapreduce.job.queuename=root.users.rtassets;

--删除数据
alter table rtassets_dw.dwd_ast_otc_hldp drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_otc_otcts_otc_asset');

insert into table rtassets_dw.dwd_ast_otc_hldp partition(part_ymd='${batch_date}',ssys_tab='ods_otc_otcts_otc_asset')
( data_date            --data_date
 ,etl_time            --etl_time
 ,busi_date           --业务日期
 ,ast_num             --资产编码
 ,ast_type_code       --资产类型代码
 ,hldp_type_code      --持仓类型代码
 ,cust_num            --客户编码
 ,cptl_acct           --资金账户
 ,crrc                --币种
 ,trade_acct          --交易账户
 ,trade_mkt           --交易市场
 ,trade_boar          --交易板块
 ,pd_incd             --产品内码
 ,pd_code             --产品代码
 ,innerorg_num        --内部机构编码
 ,ext_org_num         --外部机构编码
 ,reg_org_num         --登记机构编码
 ,yest_bal            --昨日余额
 ,share_bal           --股份余额
 ,share_aval_vol      --股份可用数量
 ,share_frz           --股份冻结
 ,share_unfrz         --股份解冻
 ,yest_buy_in_cost    --昨日买入成本
 ,cur_buy_in_cost     --当前买入成本
 ,yest_cost           --昨日成本
 ,cur_cost            --当前成本
 ,pl_amt              --盈亏金额
 ,excep_frz_vol       --异常冻结数量
 ,trade_frz_vol       --交易冻结数量
 ,long_pd_frz_vol     --长期冻结数量
 ,intrns_aval_vol     --在途可用数量
 ,intrns_buy_amt      --在途买入金额
 ,intrns_sell_amt     --在途卖出金额
 ,pd_mval             --产品市值
 ,contr_flag          --合约标志
 ,tdy_sbs_amt         --当日认购金额
 ,has_sbs_amt         --已认购金额
 ,resv_pchs_amt       --预约申购金额
 ,reserved_share      --预约赎回份额
 ,resv_sbs_amt        --预约认购金额
 ,accu_purc_amt       --累计购买金额
 ,bons_mode           --分红方式
 ,memo                --备注
 ,ssys_code           --系统来源代码
 ,ssys_num            --系统来源编码
)
select '${batch_date}'                                       as data_date                   --DATA_DATE
       ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')as etl_time                    --etl_time
       ,'${batch_date}'                                      as busi_date                   --业务日期
       ,concat('ast','|','240','|',cast(t1.cuacct_code as string),'|',cast(t1.fortune_acct as string),'|',cast(t1.inst_sno as string))  as ast_num      --资产编码
       ,'2'                                                  as ast_type_code       --资产类型代码
       ,'240'                                                as hldp_type_code      --持仓类型代码
       ,cast(t1.cust_code as string)                         as cust_num            --客户编码
       ,cast(t1.cuacct_code as string)                       as cptl_acct           --资金账户
       ,'CNY'                                                  as crrc                --币种
       ,t1.trans_acct                                        as trade_acct          --交易账户
       ,case when a1.std_code is null then concat('@',t2.mkt_code) else a1.std_code end     as trade_mkt           --交易市场
       ,null                                                 as trade_boar          --交易板块
       ,cast(t1.inst_sno as string)                          as pd_incd             --产品内码
       ,null                                                 as pd_code             --产品代码
       ,cast(t1.int_org as string)                           as innerorg_num        --内部机构编码
       ,null                                                 as ext_org_num         --外部机构编码
       ,cast(t1.ta_code as string)                           as reg_org_num         --登记机构编码
       ,cast(t1.inst_last_bal/100 as decimal(24,6))          as yest_bal            --昨日余额
       ,cast(t1.inst_bal/100  as decimal(24,6))              as share_bal           --股份余额
       ,cast(t1.inst_avl/100  as decimal(24,6))              as share_aval_vol      --股份可用数量
       ,null                                                 as share_frz           --股份冻结
       ,null                                                 as share_unfrz         --股份解冻
       ,cast(t1.last_cost/100    as decimal(24,6))           as yest_buy_in_cost    --昨日买入成本
       ,cast(t1.current_cost/100 as decimal(24,6))           as cur_buy_in_cost     --当前买入成本
       ,null                                                 as yest_cost           --昨日成本
       ,null                                                 as cur_cost            --当前成本
       ,null                                                 as pl_amt              --盈亏金额
       ,null                                                 as excep_frz_vol       --异常冻结数量
       ,cast(t1.inst_trd_frz     as decimal(24,6))           as trade_frz_vol       --交易冻结数量
       ,cast(t1.inst_long_frz    as decimal(24,6))           as long_pd_frz_vol     --长期冻结数量
       ,cast(t1.inst_bal_otd     as decimal(24,6))           as intrns_aval_vol     --在途可用数量
       ,cast(t1.unsett_qty_asset/100 as decimal(24,6))       as intrns_buy_amt      --在途买入金额
       ,null                                                 as intrns_sell_amt     --在途卖出金额
       ,cast(t1.mkt_value/100        as decimal(24,6))       as pd_mval             --产品市值
       ,t1.cntr_flag                                         as contr_flag          --合约标志
       ,cast(t1.today_subs_amt/100   as decimal(24,6))       as tdy_sbs_amt         --当日认购金额
       ,cast(t1.his_subs_amt/100     as decimal(24,6))       as has_sbs_amt         --已认购金额
       ,cast(t1.book_bids_amt/100    as decimal(24,6))       as resv_pchs_amt       --预约申购金额
       ,cast(t1.book_redeem_qty  as decimal(24,6))           as reserved_share      --预约赎回份额
       ,cast(t1.book_subs_amt/100    as decimal(24,6))       as resv_sbs_amt        --预约认购金额
       ,cast(t1.accu_buy_amt/100     as decimal(24,6))       as accu_purc_amt       --累计购买金额
       ,null                                                 as bons_mode           --分红方式
       ,t1.remark                                            as memo                --备注
       ,'otc'                                                as ssys_code           --系统来源代码
       ,concat(cast(t1.cuacct_code as string),'|',cast(t1.fortune_acct as string),'|',cast(t1.inst_sno as string))       as ssys_num   -- 系统来源编码
from (select * from rtassets_ods.ods_otc_otcts_otc_asset where part_ymd='${batch_date}') t1
left join (select * from rtassets_ods.ods_otc_otcts_otc_inst_base_info where part_ymd='${batch_date}') t2
on t1.inst_sno=t2.inst_sno
left join (select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='otc' and src_code_type='mkt_code' and std_code_type ='trade_mkt') a1
on t2.mkt_code=a1.src_code
;
