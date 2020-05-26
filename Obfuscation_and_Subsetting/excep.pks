create or replace PACKAGE excep is

  x_obfus_not_ready EXCEPTION;
  PRAGMA exception_init (x_obfus_not_ready, -20001);

  x_obfus_control_already_exists EXCEPTION;
  PRAGMA exception_init (x_obfus_control_already_exists, -20002);

  x_cannot_continue EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_cannot_continue,-20003);

  x_unknown_mask    EXCEPTION;
  PRAGMA exception_init (x_unknown_mask, -20004);
  
  x_obfus_run_param_mismatch EXCEPTION;
  PRAGMA exception_init (x_obfus_run_param_mismatch, -20005);

  x_obfus_run_id_not_exist EXCEPTION;
  PRAGMA exception_init (x_obfus_run_id_not_exist, -20006);
 
  x_job_failure EXCEPTION;
  PRAGMA exception_init (x_job_failure, -20007);
  
  x_job_not_successful EXCEPTION;
  PRAGMA exception_init (x_job_not_successful, -20008); 
  
  x_tgt_prism_sessions_exist EXCEPTION;
  PRAGMA exception_init (x_tgt_prism_sessions_exist, -20009);   

  x_num_recursive_path_failures_exceeded EXCEPTION;
  PRAGMA exception_init (x_num_recursive_path_failures_exceeded, -20010); 
  
  x_job_stopped EXCEPTION;
  PRAGMA exception_init (x_job_stopped, -20011);  
  
  x_unknown_job_status EXCEPTION;
  PRAGMA exception_init (x_unknown_job_status, -20012);  
  

---- 

  x_table_not_exist EXCEPTION;
  PRAGMA exception_init (x_table_not_exist, -00942);

  x_user_not_exist EXCEPTION;
  PRAGMA exception_init (x_user_not_exist, -01918);

  x_object_name_already_used EXCEPTION;
  PRAGMA exception_init (x_object_name_already_used, -00955);

  x_columns_already_indexed EXCEPTION;
  PRAGMA exception_init (x_columns_already_indexed, -01408); 
  
 -- 'ORA-02275: such a referential constraint already exists in the table'
  x_ref_cons_already_exists EXCEPTION;
  PRAGMA exception_init (x_ref_cons_already_exists, -02275);
  
  x_uks_in_tab_ref_fk EXCEPTION;
  PRAGMA exception_init (x_uks_in_tab_ref_fk, -02449);   
  
  x_parent_not_part EXCEPTION;
  PRAGMA exception_init (x_parent_not_part, -14653);  
  
--'ORA-14427: table does not support modification to a partitioned state DDL'  
  x_tab_already_partitioned EXCEPTION;
  PRAGMA exception_init (x_tab_already_partitioned, -14427);
  
  --ORA-14650: operation not supported for reference-partitioned tables
  x_not_supported_for_ref_part_tabs EXCEPTION;
  PRAGMA exception_init (x_not_supported_for_ref_part_tabs, -14650); 
  
  x_index_not_exist EXCEPTION;
  PRAGMA exception_init (x_index_not_exist, -01418);

  x_synonym_not_exist EXCEPTION;
  PRAGMA exception_init (x_synonym_not_exist, -01434);

  x_CHK_ONOFF_SWITCH_violated EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_CHK_ONOFF_SWITCH_violated,-02290);
  
  x_parent_key_not_found EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_parent_key_not_found,-02291);
   
  x_unknown_job EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_unknown_job,-27475); 
  
  --ORA-27366: job "OS1"."SS_JOB_MONITOR" is not running
  x_job_not_running EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_job_not_running,-27366); 
  
  
    
end excep;  
/