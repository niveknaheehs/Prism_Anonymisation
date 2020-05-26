create or replace package gp AS
  col_mask_row_threshold         number;
  obfus_run_id                   number;
  obfus_execution_id             number;    
  ss_run_id                      number;
  ss_execution_id                number;
  src_prefix                     varchar2(128);
  tgt_prefix                     varchar2(128);
  src_db_ver                     varchar2(128);
  pt_db_ver                      varchar2(128);
  run_env                        varchar2(128);
  anon_version                   varchar2(128);
  bln_stop_job_overrun           boolean := FALSE;
  ss_db_link                     varchar2(128);
  ss_log_file_dir                varchar2(128);
  ss_default_password            varchar2(128); 
  ss_pdb_name                    varchar2(128);  
  
  src_psm_password               varchar2(128);
  src_psm_hostname               varchar2(128);         
  src_psm_servicename            varchar2(128);
  fix_missing_part_attempts      number;
  subset_reload_attempts         number;
  job_queue_processes            number;
  parallel_job_limit             number;

  procedure set_ss_log_file_dir     (p_ss_log_file_dir  varchar2);
  function get_ss_log_file_dir return varchar2;   
  
  procedure set_ss_default_password  (p_ss_default_password varchar2); 
  
  procedure set_ss_pdb_name(p_ss_pdb_name  varchar2); 
  function get_ss_pdb_name return varchar2;   
  
  procedure set_ss_db_link (p_ss_db_link varchar2); 
  function get_ss_db_link return varchar2;  
  
  procedure set_col_mask_row_threshold  (p_col_mask_row_threshold   number);
  
  procedure set_ss_run_id(p_ss_run_id number);
  function  get_ss_run_id return number;  
  
  procedure set_obfus_run_id(p_obfus_run_id number);
  
  procedure set_obfus_execution_id(p_execution_id number);
  function  get_obfus_execution_id return number;   
  
  procedure set_ss_execution_id(p_execution_id number);
  function  get_ss_execution_id return number;   
  
  procedure set_src_prefix(p_src_prefix varchar2 );
  function get_src_prefix return varchar2;
  
  procedure set_tgt_prefix(p_tgt_prefix varchar2) ;
  function get_tgt_prefix return varchar2;  
  
  procedure set_run_env(p_run_env varchar2);
  function get_run_env return varchar2;  
  
  procedure set_anon_version(p_anon_version varchar2);
  function get_anon_version return varchar2;   
  
  procedure set_src_db_ver(p_src_db_ver varchar2);
  
  procedure set_pt_db_ver(p_pt_db_ver varchar2);
  
  procedure set_bln_stop_job_overrun(p_stop_job_overrun boolean);
  function get_bln_stop_job_overrun return boolean;
  
  procedure set_src_psm_password(p_src_psm_password  varchar2  DEFAULT 'prism');
  function get_src_psm_password return varchar2;  
  
  procedure set_src_psm_hostname(p_src_psm_hostname  varchar2  DEFAULT 'PSMPPSORA01');     
  function get_src_psm_hostname return varchar2;  
  
  procedure set_src_psm_servicename(p_src_psm_servicename  varchar2  DEFAULT 'OBFA');  
  function get_src_psm_servicename return varchar2;   
  
  procedure set_fix_missing_part_attempts(p_fix_missing_part_attempts  number);   
  function get_fix_missing_part_attempts return number;   
  
  procedure set_subset_reload_attempts(p_subset_reload_attempts  number);   
  function get_subset_reload_attempts return number;   
  
  procedure set_job_queue_processes;   
  function get_job_queue_processes return number;  
  
  procedure set_parallel_job_limit;
  function get_parallel_job_limit return number;  
  
  procedure init_gp(p_src_prefix               varchar2  DEFAULT NULL,
                    p_tgt_prefix               varchar2  DEFAULT NULL,
                    p_run_env                  varchar2  DEFAULT NULL,
                    p_anon_version             varchar2  DEFAULT NULL,
                    p_obfus_run_id             number    DEFAULT NULL,
                    p_ss_run_id                number    DEFAULT NULL,
                    p_ss_execution_id          number    DEFAULT NULL,
                    p_obfus_execution_id       number    DEFAULT NULL,
                    p_col_mask_row_threshold   number    DEFAULT NULL,
                    p_stop_job_overrun         boolean   DEFAULT FALSE,
                    p_src_psm_password         varchar2  DEFAULT NULL,
                    p_src_psm_hostname         varchar2  DEFAULT NULL,
                    p_src_psm_servicename      varchar2  DEFAULT NULL);
                    
  procedure set_global_var ( p_global_name varchar2, p_global_value in varchar );

  function  get_global_var ( p_global_name varchar2 ) return varchar2; 
                 
END gp;
/