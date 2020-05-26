create or replace PACKAGE ut AUTHID CURRENT_USER is

   procedure log(p_subsystem varchar2,p_log_msg VARCHAR2,p_code NUMBER,p_errm varchar2,p_module varchar2,p_stage_step_code varchar2 default null,p_stage_type  varchar2 default null);

   function log(p_subsystem varchar2,p_log_msg VARCHAR2, p_code NUMBER,p_errm varchar2,p_module varchar2,p_stage_step_code varchar2 default null,p_stage_type  varchar2 default null) return number;

   procedure switch_subsystem_on_off ( p_subsystem       varchar2,
                                       p_on_off          varchar2,
                                       p_run_id          number   DEFAULT NULL,
                                       p_src_prefix      varchar2 DEFAULT NULL,
                                       p_tgt_prefix      varchar2 DEFAULT NULL,
                                       p_run_env         varchar2 DEFAULT NULL,
                                       p_anon_version    varchar2 DEFAULT NULL );

   procedure switch_subsystem_off_after_stage_step ( p_subsystem         varchar2,
                                                     p_stage_step_code   varchar2 );

   function get_stmt_seq ( p_subsystem        varchar2,
                           p_stage_step_code  varchar2) return integer;

   function get_ss_stage_type (p_stage_step_code  varchar2) return varchar2;

   function can_continue(p_subsystem varchar2) return boolean;

   procedure drop_unused_cols (p_tgt_prefix in varchar2);

   function EXCLUDE (astring varchar2)  return varchar2;

   function GET_FORMATTED_SK (pi_name in varchar2,pi_holder_type varchar2,pi_holder_designation varchar2) return varchar2;

   function MAN (astring varchar2)  return varchar2;

   function TDC (astring varchar2) return varchar2;

   function TDC2 (p_string varchar2) return varchar2;

   function gen_rnd_non_alphanum ( p_not_char IN VARCHAR DEFAULT NULL ) return varchar2;

   function TDC2_UNIQUE (p_str_for_unique_obfus in varchar2) return varchar2;

   function OFE (astring varchar2)  return varchar2;

   function OFT (astring varchar2)  return varchar2;

   function OFW (astring varchar2)  return varchar2;

   function RANDOMISE_DATE_30 (p_date date) return date;

   function RD30 (p_date date) return date;

   function RN (note varchar2) return varchar2;

   function RND_DIGIT_WO_RESEL (anumber number) return number;

  -- function RND_NOTE return varchar2;

   function fn_date_mask (p_date in date) return date;

   function fn_char_mask ( p_string varchar2 ) return varchar2;

   function fn_number_mask ( p_number number ) return number;

   procedure truncate_table_new (p_table_owner varchar2, p_table_name varchar2, p_temporary varchar2 DEFAULT 'N');

   procedure rebuild_indexes (p_table_owner varchar2 ,p_table_name varchar2);

   function AN (p_account_name varchar2) return varchar2;

   procedure load_dd_stats(p_subsystem varchar2);

   procedure load_dd_constraints (p_subsystem varchar2);
   
   procedure load_dd_synonyms(p_subsystem varchar2);

   procedure load_dd_tab_partitions (p_subsystem varchar2); 

   procedure load_dd(p_subsystem varchar2);
   
   procedure load_dd_tab_privs(p_subsystem varchar2);
   
   procedure load_dd_tables(p_subsystem varchar2);
      
   procedure load_dd_views (p_subsystem varchar2);      
      
   procedure load_dd_cons_columns(p_subsystem varchar2);

   procedure load_dd_ind_columns (p_subsystem varchar2);
   
   procedure load_dd_indexes (p_subsystem varchar2);  
   
   procedure load_dd_objects (p_subsystem varchar2);
   
   procedure load_dd_part_tables(p_subsystem varchar2);
   
   procedure drop_table_if_exists (p_subsystem varchar2, p_table_name varchar2);

   procedure execute_stmt ( p_subsystem        varchar2,
                            p_stmt             varchar2, 
                            p_stage_type       varchar2, 
                            p_stage_step_code  varchar2, 
                            p_stmt_seq         number );

   procedure sleep ( p_subsystem      varchar2,
                     p_sleep_seconds  number );

   procedure wait_for_ss_jobs ( p_job_name_like varchar2 default null );

   function get_subsystem_running_job_cnt (p_subsystem    varchar2, p_job_name_like varchar2 default null)  return number;

   function get_all_running_job_cnt (p_subsystem    varchar2) return number;

   function get_incomplete_job_cnt (p_subsystem varchar2, p_run_id number, p_start_date date)  return number;

   function get_job_errors (p_subsystem   varchar2,
                            p_job_name    varchar2 )  return varchar2;

   function get_job_status ( p_subsystem    varchar2,
                             p_job_name     varchar2 )  return varchar2;

   procedure run_job ( p_subsystem  varchar2,
                       p_job_name   varchar2 );

   procedure monitor_jobs (p_subsystem varchar2, p_run_id number, p_execution_id number, p_start_date date);

   procedure create_job ( p_subsystem        varchar2,
                          p_job_name         varchar2,
                          p_job_action       varchar2,
                          p_job_type         varchar2  default 'PLSQL_BLOCK',
                          p_repeat_interval  varchar2  default null,
                          p_comments         varchar2  default null );

   function create_job( p_subsystem        varchar2,
                        p_job_name         varchar2,
                        p_job_action       varchar2,
                        p_job_type         varchar2  default 'PLSQL_BLOCK',
                        p_repeat_interval  varchar2  default null,
                        p_comments         varchar2  default null ) return varchar2;

   procedure create_ss_job ( p_job_name                varchar2,
                             p_job_action              varchar2,
                             p_start_stage_step_code   varchar2  default null,
                             p_end_stage_step_code     varchar2  default null,       
                             p_stmt_seq                number    default 1,
                             p_job_type                varchar2  default 'PLSQL_BLOCK',
                             p_repeat_interval         varchar2  default null,
                             p_comments                varchar2  default null );

   procedure drop_overrun_prevention_jobs(p_subsystem    varchar2);

   function get_latest_log_id(p_subsystem varchar2, p_stage_step_code varchar2)
      return number;

   procedure merge_job_execution( p_subsystem               varchar2,
                                  p_run_id                  number,
                                  p_job_name                varchar2,
                                  p_execution_id            number,
                                  p_start_stage_step_code   varchar2  default null,
                                  p_end_stage_step_code     varchar2  default null,
                                  p_stmt_seq                number    default 1 );

   procedure gather_partition_stats(p_owner varchar2, p_part_list varchar2);

   procedure gather_partition_stats(p_part_list varchar2);
   
   procedure reset_sequence(p_subsystem varchar2, p_seq_name varchar2, p_reset_to number default 1);  
  
   procedure ins_drop_ref_part_tab_log( p_actual_owner             varchar2,
                                        p_table_name               varchar2,
                                        p_exec_order               varchar2,
                                        p_success_drop_order       varchar2 default null,
                                        p_cross_schema_fk          varchar2 default null,
                                        p_parent_owner             varchar2 default null,
                                        p_parent_table             varchar2 default null,
                                        p_cross_schema_relation    varchar2 default null,
                                        p_errm                     varchar2 default null,
                                        p_err_code                 number   default null );
  
   procedure drop_ref_part_tab_recursive(p_owner varchar2, p_table_name varchar2); 
  
   procedure drop_ref_part_tabs( p_subsystem in varchar2 default 'SUBSETTING' );  
 
   procedure drop_ref_part_tabs_main( p_subsystem in varchar2 default 'SUBSETTING' ); 
  
   procedure drop_r_cons (p_subsystem            varchar2,
                          p_actual_r_cons_owner  varchar2  default null,
                          p_r_cons_name          varchar2  default null);    
  
   procedure disable_r_cons(p_subsystem varchar2);  
  
   procedure enable_r_cons(p_subsystem  varchar2);  
  
   procedure disable_triggers(p_subsystem  varchar2);

   procedure enable_triggers(p_subsystem  varchar2);   
   
   procedure recompile(p_subsystem  varchar2);    
   
   function get_tgt_psm_session_count(p_subsystem varchar2, p_tgt_prefix  varchar2) return number;
   
   procedure validate_db_link;   
   
   procedure create_src_synonyms;   
   
   procedure run_remote_md_utilities_job ( p_job_name        varchar2,
                                           p_job_action      varchar2,
                                           p_repeat_interval varchar2  default null,
                                           p_start_date      timestamp default null );   
   
   procedure gen_src_metadata( p_comp_list       varchar2,
                               p_job_start_time  timestamp default null );
   
   function fn_get_part_list return varchar2;   
      
   procedure build_all_load_views;
   
	 procedure ins_add_partition_log(
      p_actual_owner             varchar2,
      p_table_name               varchar2,
      p_exec_order               varchar2,
      p_success_add_order       varchar2 default null,
      p_partition_fk                      varchar2 default null,
      p_parent_owner             varchar2 default null,
      p_parent_table             varchar2 default null,
      p_cross_schema_relation    varchar2 default null,
      p_errm                     varchar2 default null,
      p_err_code                 number   default null
  );
  
   procedure ins_add_partition_recursive(p_owner varchar2, p_table_name varchar2, p_part_ddl varchar2);
  
   procedure ins_add_partition( p_subsystem in varchar2 default 'SUBSETTING' );  
 
   procedure ins_add_partition_main( p_subsystem in varchar2 default 'SUBSETTING' ); 
		
   function ins_md_ddl (p_actual_owner varchar2,  p_object_type varchar2, p_object_name varchar2) return number;
    				
   function fn_build_regexp_match_str (p_prefix in varchar2) return varchar2;
   
   procedure replace_md_ddl_actual_owner (p_src_prefix varchar2, p_tgt_prefix varchar2);
            			  
   procedure restore_session_to_run_env;    
   
   procedure load_table_exclusions;
end ut;
/