create or replace PACKAGE excep is

  x_empty_comp_list EXCEPTION;
  PRAGMA exception_init (x_empty_comp_list, -20002);
 
  x_job_failure EXCEPTION;
  PRAGMA exception_init (x_job_failure, -20007);
  
  x_job_not_successful EXCEPTION;
  PRAGMA exception_init (x_job_not_successful, -20008); 
  
  x_metadata_already_generated EXCEPTION;
  PRAGMA exception_init (x_metadata_already_generated, -20009);   
  
  x_unexpected_new_ddl_length EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_unexpected_new_ddl_length,-20999);   
  
  x_unknown_job EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_unknown_job,-27475); 
  
  x_job_not_running EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_job_not_running,-27366); 
  
end excep;  
/