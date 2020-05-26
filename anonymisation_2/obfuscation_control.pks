create or replace PACKAGE obfuscation_control
AS

  g_code   NUMBER;
  g_errm   varchar2(4000); 
  g_module varchar2(62);
  g_escape varchar2(1)  := '\'; 
  
  procedure obfus_log(p_log_msg VARCHAR2,p_src_prefix VARCHAR2,p_anon_version VARCHAR2,p_tgt_prefix VARCHAR2, p_code NUMBER,p_errm varchar2,p_module varchar2);
  function create_obfus_control(p_src_prefix VARCHAR2, p_tgt_prefix VARCHAR2, p_run_env VARCHAR2, p_anon_version VARCHAR2) return number;
  procedure update_obfus_control(p_obfus_run_id NUMBER, p_src_prefix VARCHAR2,  p_tgt_prefix VARCHAR2, p_run_env VARCHAR2, p_anon_version VARCHAR2, 
                                 p_obfus_status VARCHAR2 DEFAULT NULL, p_setup_triggers VARCHAR2 DEFAULT NULL, p_setup_synonyms VARCHAR2 DEFAULT NULL, 
                                 p_setup_indexes VARCHAR2 DEFAULT NULL, p_setup_cheque_ranges VARCHAR2 DEFAULT NULL, p_setup_stats VARCHAR2 DEFAULT NULL,                               
                                 p_checked VARCHAR2 DEFAULT NULL );   
  procedure insert_tables_to_truncate(p_tgt_prefix VARCHAR2);                               
  procedure obfus_precheck (p_src_prefix VARCHAR2, p_tgt_prefix VARCHAR2, p_run_env VARCHAR2, p_anon_version VARCHAR2);                                 
  procedure check_obfus_ready(p_obfus_run_id IN OUT NUMBER, p_src_prefix IN OUT VARCHAR2, p_tgt_prefix IN OUT VARCHAR2, p_run_env IN OUT VARCHAR2, p_anon_version IN OUT VARCHAR2 );                               
  procedure update_cheque_ranges (p_src_prefix in varchar2); 


end obfuscation_control;  