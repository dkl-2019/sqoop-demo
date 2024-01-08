-- TODO 6.资金表
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
),
     dim_user as (
         select cust_num     as USER_CODE,
                innerorg_num as INNERORG_CODE
         from rtassets_dw.dwd_ide_cust
         WHERE part_ymd = '${batch_date}'
           AND ssys_tab = 'ods_uas_kbssuser_customer'
     ),
     cptl as (
         select 'cts'    as SSYS_CODE,
                cust_num as USER_CODE,
                crrc   as CURRENCY,
                acct_bal,
                intrns_amt
         from rtassets_dw.dwd_ast_cptl
         where part_ymd = '${batch_date}'
           AND ssys_tab = 'ods_cts_kgdb_capital'
         union all
         select 'mts'    as SSYS_CODE,
                cust_num as USER_CODE,
                crrc   as CURRENCY,
                acct_bal,
                intrns_amt
         from rtassets_dw.dwd_ast_cptl
         where part_ymd = '${batch_date}'
           AND ssys_tab = 'ods_mts_kgdbrzrq_capital'
     ),
     dim_org as (
         SELECT ide_id            as INNERORG_CODE,
                innerorg_fullname as INNERORG_NAME
         FROM rtassets_dw.dwd_ide_innerorg
         WHERE ssys_tab = 'ods_uas_kbssuser_org'
     )
insert overwrite table rtassets_ads.ads_bs_trd_cptl_d partition (part_ymd = '${batch_date}')
select substr(cast(regexp_replace(
        cast(date_add(from_unixtime(unix_timestamp('${batch_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 1) as
            string),
        '-',
        '') as string), 1, 8)              as DATA_DATE,
       SSYS_CODE,
       t3.INNERORG_CODE,
       INNERORG_NAME,
       '2'                                                    as IS_RL,
       0                                                      as ACCT_BAL_CHG,
       0                                                      as INTRNS_CPTL_CHG,
       YES_ACCT_BAL,
       YES_INTRNS_CPTL,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as DATA_TIME
from (
         select SSYS_CODE,
                INNERORG_CODE,
                cast(sum(YES_ACCT_BAL) / 10000 as decimal(19, 6))    as YES_ACCT_BAL,
                cast(sum(YES_INTRNS_CPTL) / 10000 as decimal(19, 6)) as YES_INTRNS_CPTL
         from (
                  select t1.SSYS_CODE,
                         INNERORG_CODE,
                         t1.YES_ACCT_BAL * dim_cny.p    as YES_ACCT_BAL,
                         t1.YES_INTRNS_CPTL * dim_cny.p as YES_INTRNS_CPTL
                  from (
                           select SSYS_CODE,
                                  INNERORG_CODE,
                                  CURRENCY,
                                  sum(acct_bal)   as YES_ACCT_BAL,
                                  sum(intrns_amt) as YES_INTRNS_CPTL
                           from (
                                    select cptl.SSYS_CODE,
                                           cptl.USER_CODE,
                                           cptl.CURRENCY,
                                           cptl.acct_bal,
                                           cptl.intrns_amt,
                                           dim_user.INNERORG_CODE
                                    from cptl
                                             join dim_user
                                                  on cptl.USER_CODE = dim_user.USER_CODE
                                ) t0
                           group by SSYS_CODE, INNERORG_CODE, CURRENCY
                       ) t1
                           left join dim_cny
                                     on t1.CURRENCY = dim_cny.currency
              ) t2
         group by SSYS_CODE, INNERORG_CODE
     ) t3
         join dim_org
              on cast(t3.INNERORG_CODE as string) = cast(dim_org.INNERORG_CODE as string)
;
