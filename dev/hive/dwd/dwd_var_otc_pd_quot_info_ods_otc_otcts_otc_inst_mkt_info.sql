-------------------------------------------------------------------------#
-- 任务名：       dwd_var_otc_pd_quot_info_ods_otc_otcts_otc_inst_mkt_info.q
-- 目标表：       dwd_var_otc_pd_quot_info OTC产品行情信息
-- 源表：         rtassets_ods.ods_otc_otcts_otc_inst_mkt_info
--                rtassets_ods.ods_otc_otcts_otc_inst_base_info
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
--取消小表加载至内存中
set hive.auto.convert.join = false;
set mapreduce.job.queuename=root.users.rtassets;

--删除数据
alter table rtassets_dw.dwd_var_otc_pd_quot_info drop if exists partition(part_ymd = '${batch_date}' , ssys_tab = 'ods_otc_otcts_otc_inst_mkt_info');

-- 更新数据
insert into table rtassets_dw.dwd_var_otc_pd_quot_info partition (part_ymd = '${batch_date}' , ssys_tab = 'ods_otc_otcts_otc_inst_mkt_info') 
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
    '${batch_date}'                                              as data_date               --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')       as etl_time                --etl_time
    ,t3.pd_num                                                   as pd_num                  --产品编码
    ,t3.pd_type_code                                             as pd_type_code            --产品类型代码
    ,t3.fin_instrmt_type_code                                    as fin_instrmt_type_code   --金融工具类型代码
    ,cast(t2.inst_code as string)                                as pd_code                 --产品代码
    ,cast(t1.inst_sno as string)                                 as pd_incd                 --产品内码
    ,substr(replace(cast(t1.trd_date as string),'-',''),1,8)     as trade_dt                --交易日期
    ,cast(t1.last_net/10000       as decimal(24,6))              as latest_net_value        --最新净值
    ,cast(t1.accu_net/10000       as decimal(24,6))              as accu_net_value          --累计净值
    ,cast(t1.cur_profit_rate/10000 as decimal(24,6))             as today_yield             --当天收益率
    ,cast(t1.inst_income_unit/10000 as decimal(24,6))            as tenthous_profit         --万份收益
    ,cast(t1.yield/10000          as decimal(24,6))              as d7_aror                 --七日年化收益率
    ,cast(t1.close_price/10000    as decimal(24,6))              as yest_clqn_pric          --昨日收盘价
    ,cast(t1.open_price/10000     as decimal(24,6))              as tdy_opqn_pric           --今日开盘价
    ,cast(t1.last_price/10000     as decimal(24,6))              as recnt_mtch_prc          --最近成交价格
    ,cast(t1.last_qty             as decimal(24,6))              as recnt_mtch_vol          --最近成交数量
    ,cast(t1.best_buy_price/10000 as decimal(24,6))              as best_ppri               --最优申买价格
    ,cast(t1.best_sell_price/10000 as decimal(24,6))             as best_sapr               --最优申卖价格
    ,t1.of_stat                                                  as pd_state                --产品状态
    ,cast(t1.best_buy_qty      as decimal(24,6))                 as best_req_buy_vol        --最优申买数量
    ,cast(t1.best_sell_qty     as decimal(24,6))                 as best_req_sell_vol       --最优申卖数量
    ,cast(t1.buy_qty1          as decimal(24,6))                 as req_buy_vol_one         --申买数量一
    ,cast(t1.buy_price1/10000  as decimal(24,6))                 as ppri_one                --申买价格一
    ,cast(t1.buy_qty2          as decimal(24,6))                 as req_buy_vol_two         --申买数量二
    ,cast(t1.buy_price2/10000  as decimal(24,6))                 as ppri_two                --申买价格二
    ,cast(t1.buy_qty3          as decimal(24,6))                 as req_buy_vol_thre        --申买数量三
    ,cast(t1.buy_price3/10000  as decimal(24,6))                 as ppri_thre               --申买价格三
    ,cast(t1.buy_qty4          as decimal(24,6))                 as req_buy_vol_four        --申买数量四
    ,cast(t1.buy_price4/10000  as decimal(24,6))                 as ppri_four               --申买价格四
    ,cast(t1.buy_qty5          as decimal(24,6))                 as req_buy_vol_five        --申买数量五
    ,cast(t1.buy_price5/10000  as decimal(24,6))                 as ppri_five               --申买价格五
    ,cast(t1.sell_qty1         as decimal(24,6))                 as req_sell_vol_one        --申卖数量一
    ,cast(t1.sell_price1/10000 as decimal(24,6))                 as sapr_one                --申卖价格一
    ,cast(t1.sell_qty2         as decimal(24,6))                 as sapr_two                --申卖价格二
    ,cast(t1.sell_price2/10000 as decimal(24,6))                 as sapr_thre               --申卖价格三
    ,cast(t1.sell_qty3         as decimal(24,6))                 as sapr_four               --申卖价格四
    ,cast(t1.sell_price3/10000 as decimal(24,6))                 as sapr_five               --申卖价格五
    ,cast(t1.sell_qty4         as decimal(24,6))                 as req_sell_vol_two        --申卖数量二
    ,cast(t1.sell_price4/10000 as decimal(24,6))                 as req_sell_vol_thre       --申卖数量三
    ,cast(t1.sell_qty5         as decimal(24,6))                 as req_sell_vol_four       --申卖数量四
    ,cast(t1.sell_price5/10000 as decimal(24,6))                 as req_sell_vol_five       --申卖数量五
    ,t1.upd_timestamp                                            as update_date             --更新日期
    ,t1.net_date                                                 as net_value_date          --净值日期
    ,cast(t1.unit_div/10000   as decimal(24,6))                  as unit_bons               --单位分红
    ,cast(t1.cur_total_income/10000 as decimal(24,6))            as cur_income              --当前收入
    ,'otc'                                                       as ssys_code               --系统来源代码
    ,concat(cast(t1.inst_sno as string),'|',cast(t1.trd_date as string))         as ssys_num                --系统来源编码
from (select * from rtassets_ods.ods_otc_otcts_otc_inst_mkt_info where part_ymd='${batch_date}') t1
left join (select * from rtassets_ods.ods_otc_otcts_otc_inst_base_info where part_ymd='${batch_date}') t2 
on t1.inst_sno= t2.inst_sno
left join (select * from rtassets_dw.dwd_var_fin_instrmt where part_ymd='${batch_date}' and ssys_tab = 'ods_otc_otcts_otc_inst_base_info') t3
on cast(t1.inst_sno as string)=t3.pd_incd
;
