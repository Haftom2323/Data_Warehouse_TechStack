
SELECT
id,utc_time_id,source_ref, source_id, feed_id, 
primary_link_source_flag,samples 
FROM {{ref('all_data')}}
WHERE id <=20