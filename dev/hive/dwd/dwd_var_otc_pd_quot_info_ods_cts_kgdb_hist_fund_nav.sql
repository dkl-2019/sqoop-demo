-------------------------------------------------------------------------#
-- 任务名：       dwd_var_otc_pd_quot_info_ods_cts_kgdb_hist_fund_nav.q
-- 目标表：       dwd_var_otc_pd_quot_info OTC产品行情信息
-- 源表：         rtassets_ods.ods_cts_kgdb_hist_fund_nav
--                rtassets_ods.ods_cts_kgdb_funds
--                rtassets_dw.dwd_var_fin_instrmt
-- 运行频度：     
-- 任务功能说明：OTC产品行情信息
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
alter table rtassets_dw.dwd_var_otc_pd_quot_info drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_cts_kgdb_hist_fund_nav');

-- 更新数据
insert into table rtassets_dw.dwd_var_otc_pd_quot_info partition (part_ymd = '${batch_date}' , ssys_tab = 'ods_cts_kgdb_hist_fund_nav') 
(
  data_date                --data_date
  ,etl_time                --etl_time
  ,pd_num                  --产品编码
  ,pd_type_code            --产品类型代码
  ,fin_instrmt_type_code   --金融工具类型代码
  ,pd_code                 --产品代码
  ,pd_incd                 --产品内码
  ,trade_dt                --交易日期
  ,latest_net_value        --最新净值
  ,accu_net_value          --累计净值
  ,today_yield             --当天收益率
  ,tenthous_profit         --万份收益
  ,d7_aror                 --七日年化收益率
  ,yest_clqn_pric          --昨日收盘价
  ,tdy_opqn_pric           --今日开盘价
  ,recnt_mtch_prc          --最近成交价格
  ,recnt_mtch_vol          --最近成交数量
  ,best_ppri               --最优申买价格
  ,best_sapr               --最优申卖价格
  ,pd_state                --产品状态
  ,best_req_buy_vol        --最优申买数量
  ,best_req_sell_vol       --最优申卖数量
  ,req_buy_vol_one         --申买数量一
  ,ppri_one                --申买价格一
  ,req_buy_vol_two         --申买数量二
  ,ppri_two                --申买价格二
  ,req_buy_vol_thre        --申买数量三
  ,ppri_thre               --申买价格三
  ,req_buy_vol_four        --申买数量四
  ,ppri_four               --申买价格四
  ,req_buy_vol_five        --申买数量五
  ,ppri_five               --申买价格五
  ,req_sell_vol_one        --申卖数量一
  ,sapr_one                --申卖价格一
  ,sapr_two                --申卖价格二
  ,sapr_thre               --申卖价格三
  ,sapr_four               --申卖价格四
  ,sapr_five               --申卖价格五
  ,req_sell_vol_two        --申卖数量二
  ,req_sell_vol_thre       --申卖数量三
  ,req_sell_vol_four       --申卖数量四
  ,req_sell_vol_five       --申卖数量五
  ,update_date             --更新日期
  ,net_value_date          --净值日期
  ,unit_bons               --单位分红
  ,cur_income              --当前收入
  ,ssys_code               --系统来源代码
  ,ssys_num                --系统来源编码
)
select 
    '${batch_date}'                                                       as data_date               --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')                as etl_time                --etl_time
    ,t3.pd_num                                                            as pd_num                  --产品编码
    ,t3.pd_type_code                                                      as pd_type_code            --产品类型代码
    ,t3.fin_instrmt_type_code                                             as fin_instrmt_type_code   --金融工具类型代码
    ,t2.fund_code                                                         as pd_code                 --产品代码
    ,cast(t1.fund_intl as string)                                         as pd_incd                 --产品内码
    ,cast(t1.trd_date  as string)                                         as trade_dt                --交易日期
    ,cast(t1.nav       as decimal(24,6))                                  as latest_net_value        --最新净值
    ,cast(t1.addup_nav as decimal(24,6))                                  as accu_net_value          --累计净值
    ,null                                                                 as today_yield             --当天收益率
    ,cast(t1.tt_income    as decimal(24,6))                               as tenthous_profit         --万份收益
    ,cast(t1.income_yield as decimal(24,6))                               as d7_aror                 --七日年化收益率
    ,null                                                                 as yest_clqn_pric          --昨日收盘价
    ,null                                                                 as tdy_opqn_pric           --今日开盘价
    ,null                                                                 as recnt_mtch_prc          --最近成交价格
    ,null                                                                 as recnt_mtch_vol          --最近成交数量
    ,null                                                                 as best_ppri               --最优申买价格
    ,null                                                                 as best_sapr               --最优申卖价格
    ,t1.fund_status                                                       as pd_state                --产品状态
    ,null                                                                 as best_req_buy_vol        --最优申买数量
    ,null                                                                 as best_req_sell_vol       --最优申卖数量
    ,null                                                                 as req_buy_vol_one         --申买数量一
    ,null                                                                 as ppri_one                --申买价格一
    ,null                                                                 as req_buy_vol_two         --申买数量二
    ,null                                                                 as ppri_two                --申买价格二
    ,null                                                                 as req_buy_vol_thre        --申买数量三
    ,null                                                                 as ppri_thre               --申买价格三
    ,null                                                                 as req_buy_vol_four        --申买数量四
    ,null                                                                 as ppri_four               --申买价格四
    ,null                                                                 as req_buy_vol_five        --申买数量五
    ,null                                                                 as ppri_five               --申买价格五
    ,null                                                                 as req_sell_vol_one        --申卖数量一
    ,null                                                                 as sapr_one                --申卖价格一
    ,null                                                                 as sapr_two                --申卖价格二
    ,null                                                                 as sapr_thre               --申卖价格三
    ,null                                                                 as sapr_four               --申卖价格四
    ,null                                                                 as sapr_five               --申卖价格五
    ,null                                                                 as req_sell_vol_two        --申卖数量二
    ,null                                                                 as req_sell_vol_thre       --申卖数量三
    ,null                                                                 as req_sell_vol_four       --申卖数量四
    ,null                                                                 as req_sell_vol_five       --申卖数量五
    ,null                                                                 as update_date             --更新日期
    ,null                                                                 as net_value_date          --净值日期
    ,null                                                                 as unit_bons               --单位分红
    ,null                                                                 as cur_income              --当前收入
    ,'cts2'                                                                as ssys_code               --系统来源代码
    ,concat(cast(t1.trd_date as string),'|',cast(t1.fund_intl as string)) as ssys_num                --系统来源编码
from (select * from rtassets_ods.ods_cts_kgdb_hist_fund_nav where part_ymd='${batch_date}') t1
left join (select * from rtassets_ods.ods_cts_kgdb_funds  where part_ymd='${batch_date}') t2 
on t1.fund_intl = t2.fund_intl
left join (select * from rtassets_dw.dwd_var_fin_instrmt where part_ymd='${batch_date}' and ssys_tab = 'ods_cts_kgdb_funds') t3
on cast(t1.fund_intl as string)=t3.ssys_num
;
