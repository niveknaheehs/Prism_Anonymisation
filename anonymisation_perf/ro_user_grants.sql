-- grants for ro_users against the anonymisation owner

GRANT SELECT ON &&ANON_OWNER..obfus_control TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..obfus_run_id_seq TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..obfuscation_log TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..obfuscation_log_seq TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..peripheral_tables TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..table_populated TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..obfus_pre_check TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..obfus_onoff_switch TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..pc_transform TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..pc_transform_2 TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..obfus_ctrl_stmts TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..obfus_ctrl_manual_config TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..obfus_control_exec_result TO &&RO_USER;


GRANT SELECT ON &&ANON_OWNER..EXECUTION_REPORT TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..STATS_RESULTS_1 TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..STATS_RESULTS_2 TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..STATS_RESULTS_PIVOT1 TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..STATS_RESULTS_PIVOT2 TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..STATS_RESULTS_TMP TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..STATS_STMTS TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PERIPHERAL_TABLES TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PERIPHERAL_TABLES_LOAD TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PER_COL_MASK_OVERIDE TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PER_COL_MASK_OVERIDE_LOAD TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PER_COL_MASK_OVERIDE_LOAD_EXCEPTIONS TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PER_TABLES_LOAD_EXCEPTIONS TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PER_TRANS_COLS TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PER_TRANS_COL_CON TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PER_TRANS_COL_IND TO &&RO_USER;
GRANT SELECT ON &&ANON_OWNER..PARTITION_UPDATE_COUNTS TO &&RO_USER;

-- GRANTS ON DBA views
grant select on dba_tab_columns to &&RO_USER;
grant select on dba_tables to &&RO_USER;
grant select on dba_tab_statistics to &&RO_USER;
grant select on dba_triggers to &&RO_USER;
grant select on dba_synonyms to &&RO_USER;
grant select on dba_tab_statistics to &&RO_USER;
grant select on DBA_SCHEDULER_JOBS to &&RO_USER;
grant select on DBA_SCHEDULER_JOB_RUN_DETAILS to &&RO_USER;
grant select on DBA_SCHEDULER_JOB_LOG to &&RO_USER;
grant select on dba_tab_partitions to &&RO_USER;
grant select on dba_part_key_columns to &&RO_USER;
grant select on dba_ind_subpartitions to &&RO_USER;
grant select on dba_source to &&RO_USER;

GRANT ANALYZE ANY TO &&RO_USER;


  

 