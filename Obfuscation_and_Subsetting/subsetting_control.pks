create or replace PACKAGE subsetting_control AUTHID CURRENT_USER
AS

  TYPE string_list_257 IS TABLE OF VARCHAR2(257) INDEX BY BINARY_INTEGER;
  TYPE string_list_4000 IS TABLE OF VARCHAR2(4000) INDEX BY BINARY_INTEGER;

  procedure truncate_report_tables;

  procedure merge_ss_ctrl_exec_result( p_ss_run_id        number,
                                       p_stage_step_code  varchar2,
                                       p_stmt_seq         number,
                                       p_execution_id     number,
                                       p_start_timestamp  timestamp,
                                       p_end_timestamp    timestamp,
                                       p_status           varchar2,
                                       p_ss_log_id        number);

  function create_ss_ctrl return number;

  function fn_existing_ss_run_id (p_src_prefix in varchar2,p_run_env in varchar2, p_tgt_prefix in varchar2, p_ss_run_id number DEFAULT NULL)
    return number;

  procedure update_ss_ctrl( p_ss_run_id             NUMBER,
                            p_src_prefix            VARCHAR2,
                            p_tgt_prefix            VARCHAR2,
                            p_run_env               VARCHAR2,
                            p_anon_version          VARCHAR2,
                            p_ss_status             VARCHAR2 DEFAULT NULL,
                            p_dd_loaded             VARCHAR2 DEFAULT NULL,
                            p_ss_syns_created       VARCHAR2 DEFAULT NULL,                            
                            p_ss_stages_loaded      VARCHAR2 DEFAULT NULL,
                            p_ss_config_loaded      VARCHAR2 DEFAULT NULL,
                            p_src_metadata_loaded   VARCHAR2 DEFAULT NULL);

  procedure check_ss_ready( p_ss_run_id       IN OUT NUMBER,
                            p_src_prefix      IN OUT VARCHAR2,
                            p_tgt_prefix      IN OUT VARCHAR2,
                            p_run_env         IN OUT VARCHAR2,
                            p_anon_version    IN OUT VARCHAR2 );

  procedure create_ss_monitoring_job;

  procedure create_subset ( p_src_prefix          VARCHAR2,
                            p_tgt_prefix          VARCHAR2,
                            p_run_env             VARCHAR2,
                            p_anon_version        VARCHAR2,
                            p_ss_db_link          VARCHAR2,
                            p_comp_list           VARCHAR2,
                            p_gen_metadata        BOOLEAN  DEFAULT FALSE,
                            p_gen_load_views      BOOLEAN  DEFAULT FALSE,
                            p_stop_stage_step     VARCHAR2 DEFAULT NULL,
                            p_start_step          VARCHAR2 DEFAULT NULL,                            
                            p_end_step            VARCHAR2 DEFAULT NULL,
                            p_datapump_dir        VARCHAR2 DEFAULT 'F_DRIVE_DATA_PUMP_DIR',                            
                            p_check_dependency    VARCHAR2 DEFAULT 'Y');  

  function fn_convert_stage_step_code ( p_stage_step_code varchar2 )
     return varchar2;

  function fn_get_dependent_status ( p_dependent_stage_step_code varchar2 )
     return varchar2;

  function fn_get_dependent_step_type ( p_dependent_stage_step_code varchar2 )
     return varchar2;

  function fn_get_dep_ref_part_stage_step_code ( p_owner varchar2, p_table_name varchar2 )
     return varchar2;

  procedure execute_ss_steps(p_start_step        varchar2,
                             p_end_step          varchar2,
                             p_check_dependency  varchar2  default 'Y',
                             p_ss_run_id         number    default null,
                             p_execution_id      number    default null,
                             p_src_prefix        varchar2  default null,
                             p_tgt_prefix        varchar2  default null,
                             p_run_env           varchar2  default null,
                             p_anon_version      varchar2  default null);

  procedure load_ss_stages;

  function check_stages(p_ss_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2,p_stage_tab out string_list_257,p_msg_tab out string_list_4000)
     return varchar2;

  procedure execution_report;

  procedure load_comp_list ( p_comp_list varchar2 );

  procedure load_src_schema_list;

  procedure create_schema ( p_src_schema varchar2,p_dest_schema varchar2,p_job_name varchar2,p_log_file_dir varchar2,p_database_link varchar2,new_password varchar2 default null);
 
  procedure create_src_syns;
  
  procedure load_drop_schema_stmts;
  
  procedure load_dp_md_schema_create_stmts;
  
  procedure load_environ_stmts;

  procedure load_disable_enable_r_cons_stmts;

  procedure load_create_partition_stmts;
  
  procedure load_create_trigger_stmts;
  
  procedure load_create_grants_stmts;
  
  procedure load_create_index_stmts;  
  
  procedure load_missing_constraint_stmts;  

  -- obsolete as of OS_0.1
  --procedure rerun_failed_table_creation_stmts;
  --procedure load_rerun_tables;

  procedure load_disable_enable_trg_stmts;
  
  procedure load_data_stmts;
  
  procedure load_full_table_data;
  
  procedure load_ss_partition_data;  
     
  procedure merge_large_object_partition_data(p_owner varchar2, p_table_name varchar2, p_view_name varchar2, p_stage_step_code varchar2);
  
  procedure load_src_metadata;
  
  procedure load_report_stmts;  
 
  procedure load_subset_config;
  
  procedure load_md_part_cre_stmts;
  
  procedure load_create_md_synonym_stmts;
  
  procedure fix_missing_privs;
  
  procedure fix_missing_triggers; 
  
  procedure fix_missing_indexes;   
  
  procedure fix_missing_tables;
  
  procedure fix_missing_syns;
  
  procedure fix_missing_parts;
  
  procedure fix_subset_load;
  
  procedure resolve_ref_seq;
  
  procedure swap_order (p_parent_table_actual_owner varchar2,p_parent_table_name varchar2,p_parent_table_order varchar2,
  p_child_table_actual_owner varchar2,p_child_table_name varchar2,p_child_table_order varchar2);
  
  procedure  fix_missing_ref_cons;
  
  procedure load_cons_load_rules;
  
  procedure load_c_load_rules;
  
  procedure load_star_load_rules;
  
  function fget_part_clause(p_ss_run_id number ,p_table_name varchar2,p_partitioned_YN varchar2,p_pk_cols varchar2) return varchar2;
  
  procedure load_rule_based_data_stmts ;
 
  procedure load_final_stages; 
  
  procedure get_inv_fk_from_log;
  
  procedure  fix_inv_fk;
  
  procedure fix_inv_fk_recur (p_child_table_owner varchar2 ,p_child_table_name varchar2,p_errm varchar2,p_level number);
  
  function fget_star_rule_sql(p_ss_run_id number ,p_table_name varchar2,p_partitioned_YN varchar2) return varchar2;
  
  function fget_c_rule_sql(p_ss_run_id number ,p_table_name varchar2,p_partitioned_YN varchar2) return varchar2;
  
end subsetting_control;
/