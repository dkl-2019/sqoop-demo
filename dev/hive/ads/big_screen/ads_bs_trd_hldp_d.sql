-- 实时+离线: 持仓
set mapreduce.job.queuename = root.users.rtassets;
with dim_pd as (
    select pd_incd,
           max(pd_code) as pd_code,
           max(pd_name) as pd_name,
           count(*)
    from (
             SELECT *
             FROM rtassets_dw.dwd_var_fin_instrmt
             WHERE (part_ymd = '${batch_date}' AND ssys_tab = 'ods_cts_kgdb_securities')
                or (part_ymd = '${batch_date}' AND ssys_tab = 'ods_mts_kgdbrzrq_securities')
         ) t1
    group by pd_incd
),
     dim_mds as (
         select secu_intl          as pd_incd,
                max(closing_price) as PD_PRC
         from (
                  SELECT secu_intl,
                         closing_price
                  FROM rtassets_ods.ods_cts_kgdb_h_secu_mkt_info
                  WHERE part_ymd = '${batch_date}'
                  UNION ALL
                  SELECT secu_intl,
                         closing_price
                  FROM rtassets_ods.ods_mts_kgdbrzrq_h_secu_mkt_info
                  WHERE part_ymd = '${batch_date}'
              ) t1
         group by secu_intl
     ),
     hldp as (
         SELECT 'cts'          as SSYS_CODE,
                innerorg_num   as INNERORG_CODE,
                pd_incd        as PD_INCD,
                trade_mkt      as MARKET,
                sum(share_bal) as PD_SHARE
         FROM rtassets_dw.dwd_ast_exch_hldp
         WHERE part_ymd = '${batch_date}'
           AND ssys_tab = 'ods_cts_kgdb_shares'
         group by innerorg_num,pd_incd, trade_mkt
         union all
         SELECT 'mts'          as SSYS_CODE,
                innerorg_num   as INNERORG_CODE,
                pd_incd        as PD_INCD,
                trade_mkt      as MARKET,
                sum(share_bal) as PD_SHARE
         FROM rtassets_dw.dwd_ast_exch_hldp
         WHERE part_ymd = '${batch_date}'
           AND ssys_tab = 'ods_mts_kgdbrzrq_shares'
         group by innerorg_num, pd_incd, trade_mkt
     ),
     dim_org as (
         SELECT ide_id            as INNERORG_CODE,
                innerorg_fullname as INNERORG_NAME
         FROM rtassets_dw.dwd_ide_innerorg
         WHERE ssys_tab = 'ods_uas_kbssuser_org'
     )
insert overwrite table rtassets_ads.ads_bs_trd_hldp_d partition (part_ymd = '${batch_date}')
select substr(cast(regexp_replace(
        cast(date_add(from_unixtime(unix_timestamp('${batch_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 1) as
            string),
        '-',
        '') as string), 1, 8)                                 as DATA_DATE,
       hldp.SSYS_CODE,
       hldp.INNERORG_CODE,
       INNERORG_NAME,
       hldp.PD_INCD,
       dim_pd.pd_code                                         as PD_CODE,
       dim_pd.pd_name                                         as PD_NAME,
       hldp.MARKET,
       '2'                                                    as IS_RL,
       0                                                      as PD_SHARE_CHG,
       0                                                      as PD_PRC,
       cast(hldp.PD_SHARE as decimal(19, 2))                  as YES_PD_SHARE,
       cast(dim_mds.PD_PRC as decimal(19, 2))                 as YES_PD_PRC,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as DATA_TIME
from hldp
         join dim_pd  on hldp.PD_INCD = dim_pd.pd_incd
         join dim_mds  on hldp.PD_INCD = cast(dim_mds.pd_incd as string)
         join dim_org
              on cast(hldp.INNERORG_CODE as string) = cast(dim_org.INNERORG_CODE as string);



