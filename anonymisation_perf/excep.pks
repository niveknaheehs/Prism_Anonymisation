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

---- 

  x_table_not_exist EXCEPTION;
  PRAGMA exception_init (x_table_not_exist, -00942);

  x_idx_name_already_used EXCEPTION;
  PRAGMA exception_init (x_idx_name_already_used, -00955);
 

  x_columns_already_indexed EXCEPTION;
  PRAGMA exception_init (x_columns_already_indexed, -01408); 
  
  x_index_not_exist EXCEPTION;
  PRAGMA exception_init (x_index_not_exist, -01418);

  x_synonym_not_exist EXCEPTION;
  PRAGMA exception_init (x_synonym_not_exist, -01434);

  x_CHK_ONOFF_SWITCH_violated EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_CHK_ONOFF_SWITCH_violated,-02290);

end excep;  
/