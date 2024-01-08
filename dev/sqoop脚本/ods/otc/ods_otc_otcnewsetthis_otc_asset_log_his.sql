set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_otc_otcnewsetthis_otc_asset_log_his partition(part_ymd = '${batch_date}')
select 
	\`(part_ymd)?+.+\`
from stg.stg_otc_otcnewsetthis_otc_asset_log_his
where part_ymd = '${batch_date}';