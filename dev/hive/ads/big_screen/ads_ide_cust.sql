insert overwrite table rtassets_ads.ads_ide_cust partition (part_ymd = '${batch_date}')
SELECT cust_num,innerorg_num
FROM rtassets_dw.dwd_ide_cust
WHERE part_ymd='${batch_date}'
  AND ssys_tab='ods_uas_kbssuser_users'
  and close_acct_date = '0';