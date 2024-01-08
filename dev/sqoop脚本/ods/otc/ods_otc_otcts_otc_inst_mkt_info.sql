set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_otc_otcts_otc_inst_mkt_info partition(part_ymd = '${batch_date}')
select 
	\`(part_ymd)?+.+\`
from stg.stg_otc_otcts_otc_inst_mkt_info
where part_ymd = '${batch_date}';