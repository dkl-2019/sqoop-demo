set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_mts_kgdbrzrq_securities partition(part_ymd = '${batch_date}')
select 
	\`(part_ymd)?+.+\`
from stg.stg_mts_kgdbrzrq_securities
where part_ymd = '${batch_date}';