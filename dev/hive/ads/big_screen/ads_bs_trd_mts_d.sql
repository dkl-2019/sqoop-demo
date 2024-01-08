-- TODO 7.两融负债
set mapreduce.job.queuename = root.users.rtassets;
with dim_cny as (
    select case
               when currency = 1000 then 'USD'
               when currency = 1100 then 'HKD'
               end              as currency,
           periodendprice / 100 as p -- 期末价
    from rtassets_ods.ods_jyi_jydb_ed_rmbbaseexchange
    where part_ymd = '${batch_date}'
      and regexp_replace(substr(enddate, 1, 10), '-', '') = '${batch_date}'
      and currency in (1000, 1100)
    union all
    select 'CNY' as currency,
           1     as p
)
insert overwrite table rtassets_ads.ads_bs_trd_mts_d partition ( part_ymd = '${batch_date}')
select substr(cast(regexp_replace(
        cast(date_add(from_unixtime(unix_timestamp('${batch_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 1) as
            string),
        '-',
        '') as string), 1, 8)                                 as DATA_DATE,
       cast(t3.BRANCH as string)                              as INNERORG_CODE,
       dim_org.innerorg_fullname                              as INNERORG_NAME,
       '2'                                                    as IS_RL,
       0                                                      as FI_BLN_CHG,
       0                                                      as SL_BLN_CHG,
       cast(t3.FI_BLN / 10000 as decimal(19, 6))              as YES_FI_BLN,
       cast(t3.SL_BLN / 10000 as decimal(19, 6))              as YES_SL_BLN,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as DATA_TIME
from (
         select t2.BRANCH,
                cast(sum(t2.FI_BLN) as decimal(19, 6)) as FI_BLN,
                cast(sum(t2.SL_BLN) as decimal(19, 6)) as SL_BLN
         from (
                  select t1.BRANCH,
                         t1.FI_BLN * dim_cny.p as FI_BLN,
                         t1.SL_BLN * dim_cny.p as SL_BLN
                  from (
                           select BRANCH,
                                  case
                                      when CURRENCY = '1' then 'HKD'
                                      when CURRENCY = '2' then 'USD'
                                      else 'CNY'
                                      end     as CURRENCY,
                                  sum(FI_BLN) as FI_BLN,
                                  sum(SL_BLN) as SL_BLN
                           from rtassets_ods.ods_mts_kgdbrzrq_fisl_maintratio
                           WHERE part_ymd = '${batch_date}'
                           group by BRANCH, CURRENCY
                       ) t1
                           left join dim_cny
                                     on t1.CURRENCY = dim_cny.currency
              ) t2
         group by BRANCH
     ) t3
         join
     (select *
      from rtassets_dw.dwd_ide_innerorg
      where ssys_tab = 'ods_uas_kbssuser_org') dim_org
     on cast(t3.BRANCH as string) = cast(dim_org.ide_id as string);