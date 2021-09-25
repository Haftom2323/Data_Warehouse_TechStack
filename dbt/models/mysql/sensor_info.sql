
SELECT
id,utc_time_id,source_ref, source_id, feed_id, 
primary_link_source_flag,samples 
FROM {{ref('all_data')}}
WHERE utc_time_id and source_ref and source_id and feed_id and primary_link_source_flag is not null