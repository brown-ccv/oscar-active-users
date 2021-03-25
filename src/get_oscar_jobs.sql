SELECT DISTINCT
	ass.user,
	ass.parent_acct,
	job.id_user,
	job.account,
	job.id_qos,
	qos.name AS qos_name,
	acc.organization,
	job.partition,
	job.id_group,
	job.job_name,
	job.cpus_req,
	job.mem_req,
	job.nodes_alloc,
	job.tres_alloc,
	job.gres_req,
	job.time_start,
	job.time_end,
	FROM_UNIXTIME(job.time_start)  AS start_time,
	FROM_UNIXTIME(job.time_submit) AS submit_time,
	FROM_UNIXTIME(job.time_end)    AS end_time,
	job.timelimit,
	(FROM_UNIXTIME(job.time_start) - FROM_UNIXTIME(job.time_submit))   AS sec_waiting_in_queue,
	(FROM_UNIXTIME(job.time_end) - FROM_UNIXTIME(job.time_start))      AS sec_runtime,
	(FROM_UNIXTIME(job.time_end) - FROM_UNIXTIME(job.time_start)) / 60 AS min_runtime,
	job.exit_code
	
FROM
	oscar_job_table job
	LEFT JOIN oscar_assoc_table  ass ON job.id_assoc = ass.id_assoc
	LEFT JOIN qos_table          qos ON job.id_qos = qos.id
	LEFT JOIN acct_table         acc ON job.account = acc.name
ORDER BY 
	time_start
;
