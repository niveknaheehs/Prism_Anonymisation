create or replace PACKAGE const is

  -- Anonymisation_process
  k_mask_chk_row_sample_size  constant number       := 1001;
  k_mask_chk_col_sample_size  constant number       := 3;
  k_date_mask             constant varchar2(10) := '01/01/2099';
  k_date_mask_format      constant varchar2(10) := 'DD/MM/YYYY';
  k_number_mask           constant varchar2(1)  := '9';
  k_char_mask             constant varchar2(1)  := '*';
  k_x_unknown_mask_errm     constant varchar2(100)  := 'Unknown Mask';

  k_max_rnd_note          constant number       := 100000;

  k_REP_SRC_SYN_PREFIX     constant varchar2(10) := 'S_RPT';
  k_REP_TGT_SYN_PREFIX     constant varchar2(10) := 'T_RPT';

  k_overrun_job_prefix     constant varchar2(20) := 'PREVENT_OVERRUN_OF_';
  k_max_parallel_jobs      constant number := 8;  -- limited by v$parameter job_queue_processes currently set to 4  (max_job_slave_processes currently null)

  k_sleep_seconds          constant number := 0.5;
  k_max_run_duration_mins  constant number := 180;

  -- AN
  k_acc_name_length       constant  number := 108;
  k_acc_name_suffix       constant  varchar2(5) := ' TEST';

  -- Obfuscation_control
  k_subsys_obfus              constant varchar2(30) := 'OBFUSCATION';  
  k_SINGLE_ENTITY_STAGE_CHECK constant varchar2(25)  := 'Single Entity Stage Check';

  k_Fail                      constant varchar2(1) := 'F';
  k_Pass                      constant varchar2(1) := 'P';
  k_Stmt_Order_Chr            constant varchar2(6) := 'IEAMPF';
  k_Stmt_Order_Num            constant varchar2(6) := '123456';

  k_escape                    constant varchar2(1) := '\';
  k_COMPLETED                 constant varchar2(10) := 'COMPLETED';
  k_STARTED                   constant varchar2(10) := 'STARTED';
  k_FAILED                    constant varchar2(10) := 'FAILED';
  k_DEP_IMCOMPLETE            constant varchar2(10) := 'DEP_INCOMP';
  k_STAGE_CODE_SIZE           constant number := 4;
  k_STEP_CODE_SIZE            constant number := 4;
  k_STAGE_CODE_ALPHA_SIZE     constant number := 2;  

  k_PER_TABLE_AUD_TYPE        constant varchar2(1) := 'A';
  k_PER_TABLE_HIST_TYPE       constant varchar2(1) := 'H';
  k_PER_TABLE_OTHER_TYPE      constant varchar2(1) := 'O';

  k_PER_TABLE_LOAD_MECH_AUTO  constant varchar2(1) := 'A';
  k_PER_TABLE_LOAD_MECH_MAN   constant varchar2(1) := 'M';
  
  k_const_ORA_ERROR_PARENT_KEY_NOT_FOUND  NUMBER := -02298;

  k_cant_continue_err_msg     constant varchar2(1000) := 'Cannot continue as subsystem_onoff_switch is turned ''OFF''.  exec ut.switch_subsystem_on_off(const.k_subsys_???,''ON''); to continue';
  k_obfus_not_ready_err_msg   constant varchar2(1000) := 'Obfuscation not ready to run: check obfus_control table. The PRE, ANON and POST schema prefixes must be configured and not already RUNNING to run with setup and checked(optional) flags Y';

  k_const_CTYPE_COL_MASK_ROW_THRESHOLD CONSTANT varchar2(22) := 'COL_MASK_ROW_THRESHOLD';

  k_const_COL_MASK_ROW_THRESHOLD_DEF NUMBER := 100000;

  k_OBFUS_JOB_MONITOR      constant varchar2(20) := 'OBFUS_JOB_MONITOR';

  --------------
  -- subsetting
  --------------
  k_subsys_subset          constant varchar2(30) := 'SUBSETTING';

  k_TYPE_SS_SUBSET         constant varchar2(20) := 'TYPE_SS_SUBSET';
  k_TYPE_SS_NONE           constant varchar2(20) := 'TYPE_SS_NONE';
  k_TYPE_ALL               constant varchar2(20) := 'TYPE_ALL'; 
 
  k_CREATE_SRC_SYN         constant varchar2(2) := 'SY';
  k_DROP_REF_PART_TABS     constant varchar2(2) := 'RD';
  k_DROP_USER              constant varchar2(2) := 'DU';
  k_CREATE_SCHEMA          constant varchar2(2) := 'CS';
  k_CONV_TO_PART           constant varchar2(2) := 'CP';
  k_ADD_PARTITION          constant varchar2(2) := 'AP';  
  k_ADD_TRIGGERS           constant varchar2(2) := 'AT';
  k_ADD_GRANTS             constant varchar2(2) := 'AG';
  k_ADD_INDEXES            constant varchar2(2) := 'AI';
  k_MISSING_CONSTRAINTS    constant varchar2(2) := 'MC'; 
  k_RERUN_TABLES           constant varchar2(2) := 'RT'; 
  k_COMPILE_SCHEMA         constant varchar2(2) := 'CO';
  k_ADD_SYNONYMS           constant varchar2(2) := 'AS';
  k_RECOMPILE_SCHEMA       constant varchar2(2) := 'RC'; 
  k_CHECK_POINT_1          constant varchar2(2) := 'C1'; 
  k_CHECK_POINT_2          constant varchar2(2) := 'C2'; 
  k_CHECK_POINT_3          constant varchar2(2) := 'C3'; 
  k_CHECK_POINT_4          constant varchar2(2) := 'C4';
  k_CHECK_POINT_5          constant varchar2(2) := 'C5';
  k_CHECK_POINT_6          constant varchar2(2) := 'C6';
  k_CHECK_POINT_7          constant varchar2(2) := 'C7';   
  k_CHECK_POINT_8          constant varchar2(2) := 'C8';    
  k_SCHEMA_REPORTING       constant varchar2(2) := 'SR';
  k_FINAL_STAGES           constant varchar2(2) := 'FI'; 
  k_LOAD_DATA_ALL          constant varchar2(2) := 'LA';
  k_LOAD_DATA_SUBSET       constant varchar2(2) := 'LS';
  k_LOAD_DATA_RULE         constant varchar2(2) := 'LR'; 
  k_GEN_VIEW_RULE          constant varchar2(2) := 'VR'; 
  k_GEN_SYN_RULE           constant varchar2(2) := 'RS';
  k_DATA_REPORTING         constant varchar2(2) := 'DR';
  k_FIX_DATA_ANOMALIES     constant varchar2(2) := 'FD';
  k_DISABLE_TRIGGERS       constant varchar2(2) := 'DT';  
  k_ENABLE_TRIGGERS        constant varchar2(2) := 'ET'; 
  k_DISABLE_REF_CONS       constant varchar2(2) := 'DC'; 
  k_ENABLE_REF_CONS        constant varchar2(2) := 'EC';    
  
  k_TABLE_FAILURE_RETRIES  constant number(2,0) := 10;
  
  k_num_seperate_recursive_paths    constant number(2,0) := 10;
  k_max_fix_missing_tables_retries  constant number(2,0):= 3;  
  k_max_fix_missing_part_retries    constant number(2,0):= 10;
  k_max_subset_load_retries         constant number(2,0):= 2;
  k_monitor_jobs_interval_seconds   constant number(2,0):= 15;
  k_SS_JOB_MONITOR                  constant varchar2(20) := 'SS_JOB_MONITOR';
  
end const;
/