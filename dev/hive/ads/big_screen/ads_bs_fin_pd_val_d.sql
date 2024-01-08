----------------------------- 产品保有价值 -----------------------------
set mapreduce.job.queuename = root.users.rtassets;
with dwd_val as (
    SELECT 'otc'                                         as SSYS_CODE,
           otc_bal.INNERORG_CODE,
           otc_bal.PD_INCD,
           otc_bal.TOTAL_PD_BIN * otc_nav.PD_NAV / 10000 as RETAIN_AMT-- 转化为万元
    FROM (
             SELECT cast(int_org as string)  as INNERORG_CODE,
                    cast(inst_sno as string) as PD_INCD,
                    sum(inst_bal) / 100      as TOTAL_PD_BIN
             FROM rtassets_ods.ods_otc_otcts_otc_asset
             WHERE part_ymd = '${batch_date}'
             GROUP BY int_org, inst_sno
         ) otc_bal
             JOIN
         (
             SELECT cast(inst_sno as string) as PD_INCD,
                    last_net / 10000         AS PD_NAV -- 净值扩大了100倍
             FROM rtassets_ods.ods_otc_otcts_otc_inst_mkt_info
             WHERE part_ymd = '${batch_date}'
         ) otc_nav
         ON otc_bal.PD_INCD = otc_nav.PD_INCD
    union all
    SELECT 'cts2'                                        as SSYS_CODE,
           cts_bal.INNERORG_CODE,
           cts_bal.PD_INCD,
           cts_bal.TOTAL_PD_BIN * cts_nav.PD_NAV / 10000 as RETAIN_AMT -- 转化为万元
    FROM (
             SELECT cast(branch as string)    as INNERORG_CODE,
                    cast(fund_intl as string) as PD_INCD,
                    sum(fund_bln)             as TOTAL_PD_BIN
             FROM rtassets_ods.ods_cts_kgdb_fund_vol
             WHERE part_ymd = '${batch_date}'
             GROUP BY fund_intl, branch
         ) cts_bal
             JOIN
         (
             SELECT cast(fund_intl as string) as PD_INCD,
                    nav                       as PD_NAV
             FROM rtassets_ods.ods_cts_kgdb_hist_fund_nav
             WHERE part_ymd = '${batch_date}'
         ) cts_nav
         ON cts_bal.PD_INCD = cts_nav.PD_INCD
),
     dim_org as (
         select ide_id,
                innerorg_fullname
         from rtassets_dw.dwd_ide_innerorg
         where ssys_tab = 'ods_uas_kbssuser_org'
     ),
     dim_pd as (
         select ssys_code,
                pd_incd,
                pd_code,
                pd_name
         from rtassets_dw.dwd_var_fin_instrmt
         where (part_ymd = '${batch_date}' AND ssys_tab = 'ods_cts_kgdb_funds')
            or (part_ymd = '${batch_date}' AND ssys_tab = 'ods_otc_otcts_otc_inst_base_info')
     )
insert overwrite table rtassets_ads.ads_bs_fin_pd_val_d partition (part_ymd = '${batch_date}')
select dwd_val.SSYS_CODE,
       substr(cast(regexp_replace(
               cast(date_add(from_unixtime(unix_timestamp('${batch_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 1) as
                   string),
               '-',
               '') as string), 1, 8)                          as DATA_DATE,
       '-'                                                    as START_TM,
       '-'                                                    as END_TM,
       dwd_val.INNERORG_CODE                                  as INNERORG_CODE,
       dim_org.innerorg_fullname                              as INNERORG_NAME,
       dwd_val.PD_INCD                                        as PD_INCD,
       dim_pd.pd_name                                         as PD_NAME,
       if(dim_pd.pd_code in ('009713', 'C29001'), 0, 1)       as IS_HOLD_PD,
       0                                                      as PD_RS_AMT,
       0                                                      as PD_SH_AMT,
       cast(dwd_val.RETAIN_AMT as decimal(19, 2))             AS YES_RETAIN_AMT,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as DATA_TIME
from dwd_val
         join dim_org
              on dwd_val.INNERORG_CODE = dim_org.ide_id
         join dim_pd
              on dwd_val.PD_INCD = dim_pd.pd_incd and dwd_val.SSYS_CODE = dim_pd.ssys_code;