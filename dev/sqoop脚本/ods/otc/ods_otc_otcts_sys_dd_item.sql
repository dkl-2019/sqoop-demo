set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_otc_otcts_sys_dd_item partition(part_ymd = '${batch_date}')
select 
	\`(part_ymd)?+.+\`
from stg.stg_otc_otcts_sys_dd_item
where part_ymd = '${batch_date}';