create or replace PACKAGE obfuscation_control AUTHID CURRENT_USER
AS

  TYPE string_list_257 IS TABLE OF VARCHAR2(257) INDEX BY BINARY_INTEGER;
  TYPE string_list_4000 IS TABLE OF VARCHAR2(4000) INDEX BY BINARY_INTEGER;

  procedure switch_obfus_on_off ( p_on_off                varchar2,
                                  p_obfus_run_id          NUMBER   DEFAULT NULL,
                                  p_src_prefix            VARCHAR2 DEFAULT NULL,
                                  p_tgt_prefix            VARCHAR2 DEFAULT NULL,
                                  p_run_env               VARCHAR2 DEFAULT NULL,
                                  p_anon_version          VARCHAR2 DEFAULT NULL );
  function can_continue return boolean;
  procedure init_audit_events;
  procedure truncate_report_tables;
  procedure obfus_log(p_log_msg VARCHAR2,p_src_prefix VARCHAR2,p_anon_version VARCHAR2,p_tgt_prefix VARCHAR2, p_code NUMBER,p_errm varchar2,p_module varchar2);
  function  obfus_log(p_log_msg VARCHAR2,p_src_prefix VARCHAR2,p_anon_version VARCHAR2,p_tgt_prefix VARCHAR2, p_code NUMBER,p_errm varchar2,p_module varchar2)  return number;
  procedure merge_obfus_ctrl_exec_result( p_obfus_run_id     number,
                                          p_stage_step_code  varchar2,
                                          p_stmt_seq         number,
                                          p_execution_id     number,
                                          p_start_timestamp  timestamp,
                                          p_end_timestamp    timestamp,
                                          p_status           varchar2,
                                          p_obfus_log_id     number);
  function create_obfus_control return number;
  function fn_existing_obfus_run_id (p_src_prefix in varchar2,p_run_env in varchar2, p_tgt_prefix in varchar2, p_obfus_run_id number) return number;
  procedure update_obfus_control( p_obfus_run_id          NUMBER,
                                  p_src_prefix            VARCHAR2,
                                  p_tgt_prefix            VARCHAR2,
                                  p_run_env               VARCHAR2,
                                  p_anon_version          VARCHAR2,
                                  p_obfus_status          VARCHAR2 DEFAULT NULL,
                                  p_setup_triggers        VARCHAR2 DEFAULT NULL,
                                  p_setup_synonyms        VARCHAR2 DEFAULT NULL,
                                  p_setup_indexes         VARCHAR2 DEFAULT NULL,
                                  p_setup_cheque_ranges   VARCHAR2 DEFAULT NULL,
                                  p_setup_stats           VARCHAR2 DEFAULT NULL,
                                  p_checked               VARCHAR2 DEFAULT NULL,
                                  p_peripheral_tables     VARCHAR2 DEFAULT NULL,
                                  p_environ_stages_loaded VARCHAR2 DEFAULT NULL,
                                  p_auto_stages_loaded    VARCHAR2 DEFAULT NULL,
                                  p_manual_stages_loaded  VARCHAR2 DEFAULT NULL,
                                  p_dd_loaded             VARCHAR2 DEFAULT NULL,
                                  p_per_stages_loaded     VARCHAR2 DEFAULT NULL,
                                  p_final_stages_loaded   VARCHAR2 DEFAULT NULL,
                                  p_stats_stmts_loaded    VARCHAR2 DEFAULT NULL,
                                  p_pc_transform_loaded   VARCHAR2 DEFAULT NULL,
                                  p_rnd_data_generated    VARCHAR2 DEFAULT NULL);
  procedure insert_peripheral_tables( p_obfus_run_id NUMBER );
  procedure obfus_precheck;
  procedure check_obfus_ready( p_obfus_run_id       IN OUT NUMBER,
                               p_src_prefix         IN OUT VARCHAR2,
                               p_tgt_prefix         IN OUT VARCHAR2,
                               p_run_env            IN OUT VARCHAR2,
                               p_anon_version       IN OUT VARCHAR2,
                               p_pre_check_required IN     VARCHAR2,
                               p_refresh_stats      IN     VARCHAR2);
  procedure update_cheque_ranges(p_src_prefix in varchar2);
  procedure setup_synonyms_and_grants ( p_obfus_run_id IN OUT NUMBER );
  procedure setup_obfus_env ( p_obfus_run_id       IN OUT NUMBER,
                              p_triggers                  BOOLEAN  DEFAULT TRUE,
                              p_indexes                   BOOLEAN  DEFAULT TRUE,
                              p_cheque_ranges             BOOLEAN  DEFAULT TRUE,
                              p_stats                     BOOLEAN  DEFAULT TRUE,
                              p_stats_est_percent         INTEGER  DEFAULT 10,
                              p_days_since_last_analyzed  INTEGER  DEFAULT 1);

  procedure obfuscate ( p_src_prefix          VARCHAR2,
                        p_tgt_prefix          VARCHAR2,
                        p_run_env             VARCHAR2,
                        p_anon_version        VARCHAR2,
                        p_pre_check_required  VARCHAR2 DEFAULT 'Y',
                        p_tgt_env_mode        VARCHAR2 DEFAULT 'TGT', -- Validate against src,tgt,run params. Override to SRC or DMP
                        p_refresh_stats       VARCHAR2 DEFAULT 'N',   -- Controls refresh of table stats in target db
                        p_start_step          VARCHAR2 DEFAULT NULL,
                        p_end_step            VARCHAR2 DEFAULT NULL,
                        p_obfus_run_id        NUMBER   DEFAULT NULL, -- For re-run only
                        p_check_dependency    VARCHAR2 DEFAULT 'Y');  -- Only execute steps with dependency set to 'Y'

  procedure execute_obfus_steps(p_start_step varchar2,p_end_step varchar2,p_check_dependency varchar2);

  procedure load_auto_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2);
  procedure load_manual_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2);
  procedure load_per_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2);
  procedure load_stats_stmts(p_obfus_run_id number,p_src_prefix varchar2,p_src_rep_syn_prefix varchar2,p_tgt_rep_syn_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2);
  procedure load_pc_transform;
  procedure load_environ_stages;
  procedure load_Per_trans_cols(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2);
  function check_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2,p_stage_tab out string_list_257,p_msg_tab out string_list_4000) return varchar2;
  procedure load_final_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2);
  procedure execution_report(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2);
  procedure load_dd_stats;
  procedure load_dd;
  function use_fast_mask (p_owner varchar2, p_table_name varchar2,p_src_prefix varchar2,p_anon_version varchar2,p_tgt_prefix varchar2) return varchar2 ;


 end obfuscation_control;
 /