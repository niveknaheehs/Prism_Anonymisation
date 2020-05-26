create or replace PACKAGE metadata_utilities AUTHID DEFINER
AS

  k_sleep_seconds   CONSTANT  integer := 1;

  x_company_does_not_exist EXCEPTION;
  PRAGMA exception_init (x_company_does_not_exist, -20001);
     
  x_job_mismatch EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_job_mismatch,-20998);

  x_unknown_job EXCEPTION;
  PRAGMA EXCEPTION_INIT(x_unknown_job,-27475);

  function get_db_version return varchar2;

  function get_existing_db_version return varchar2;

  procedure log(p_log_msg varchar2, p_code number, p_errm varchar2, p_module varchar2);

  function log(p_log_msg varchar2, p_code number, p_errm varchar2, p_module varchar2)  return number;

  procedure load_dd (p_prefix varchar2);
  procedure load_dd_users (p_prefix varchar2);
  procedure load_dd_tables (p_prefix varchar2);
  procedure load_dd_views (p_prefix varchar2);  
  procedure load_dd_tab_columns (p_prefix varchar2);
  procedure load_dd_tab_partitions (p_prefix varchar2);
  procedure load_dd_constraints (p_prefix varchar2);
  procedure load_dd_synonyms (p_prefix varchar2);
  procedure load_dd_cons_columns (p_prefix varchar2);
  procedure load_dd_ind_columns (p_prefix varchar2);
  procedure load_dd_indexes (p_prefix varchar2);  
  procedure load_dd_objects (p_prefix varchar2);
  procedure load_dd_tab_privs (p_prefix varchar2);
  procedure load_dd_tab_stats (p_prefix varchar2);
  procedure load_dd_tab_stats (p_src_schema varchar2,p_prefix varchar2);
  procedure load_dd_tab_col_stats (p_prefix varchar2);
  procedure load_dd_part_tables (p_prefix varchar2);

  procedure sleep ( p_sleep_seconds  number ); 
  procedure monitor_jobs( p_start_date date );
  function get_running_job_cnt return number;
  function get_incomplete_job_cnt  return number;
  procedure create_job ( p_job_name        varchar2,
                         p_job_action      varchar2,
                         p_repeat_interval varchar2  default null,
                         p_start_date      timestamp default null );  
  function create_job ( p_job_name        varchar2,
                        p_job_action      varchar2,
                        p_repeat_interval varchar2  default null,
                        p_start_date      timestamp default null )  return timestamp;
  procedure run_job ( p_job_name        varchar2 );                
  procedure merge_job_execution( p_job_name                varchar2,
                                 p_job_type                varchar2);
  procedure gather_table_partition_stats(p_actual_owner varchar2, p_table_name varchar2, p_partition varchar2);
  procedure gather_schema_partition_stats(p_src_schema varchar2, p_part_list varchar2);
  procedure gather_partition_stats(p_part_list varchar2);
  procedure gather_table_stats(p_actual_owner varchar2, p_table_name varchar2); 

  procedure gen_global_table_stats ( p_stats_est_percent         integer  default null,  -- default is AUTO
                                     p_cascade                   boolean  default TRUE,
                                     p_days_since_last_analyzed  integer  DEFAULT 1);  
  procedure create_monitor_job;  
  procedure drop_monitor_job;  
  --procedure gather_table_partition_stats_by_job(p_actual_owner varchar2, p_table_name varchar2, p_partition varchar2);    
 -- procedure gather_schema_partition_stats_by_job(p_src_schema varchar2, p_part_list varchar2);  
 -- procedure gather_partition_stats_by_job(p_part_list varchar2);
  -- procedure gather_table_stats_by_job(p_actual_owner varchar2, p_table_name varchar2);
  function get_job_start_date (p_job_name varchar2, p_ts_before_creation timestamp) return timestamp;  
  function get_job_status (p_job_name varchar2) return varchar2;    
  function get_job_errors (p_job_name varchar2) return varchar2;
  procedure swap_order (p_parent_table_actual_owner varchar2,p_parent_table_name varchar2,p_parent_table_order varchar2,
                        p_child_table_actual_owner varchar2,p_child_table_name varchar2,p_child_table_order varchar2);

  procedure resolve_glob_seq_sort;
  procedure load_comp_list ( p_comp_list in  varchar2,
                             p_part_list out varchar2);
  function get_owner_from_actual_owner (p_actual_owner in varchar2)
    return varchar;
  function get_prism_prefix return varchar;

end metadata_utilities;
/