create or replace PACKAGE const is

  --- Anonymisation_process
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
  k_max_parallel_jobs      constant number := 8;
  k_sleep_seconds          constant number := 1; 
  k_max_run_duration_mins  constant number := 180;

  -- AN
  k_acc_name_length       constant  number := 108;
  k_acc_name_suffix       constant  varchar2(5) := ' TEST';

  -- Obfuscation_control
  k_SINGLE_ENTITY_STAGE_CHECK constant varchar2(25)  := 'Single Entity Stage Check';

  k_Fail constant varchar2(1) := 'F';
  k_Pass constant varchar2(1) := 'P';
  k_Stmt_Order_Chr constant varchar2(6) := 'IEAMPF';
  k_Stmt_Order_Num constant varchar2(6) := '123456';												
  																	 
  k_escape    constant varchar2(1) := '\';
  k_COMPLETED constant varchar2(10) := 'COMPLETED';
  k_STARTED   constant varchar2(10) := 'STARTED';
  k_FAILED    constant varchar2(10) := 'FAILED';
  k_DEP_IMCOMPLETE  constant varchar2(10) := 'DEP_INCOMP';
  k_STAGE_CODE_SIZE    constant number := 4;
  k_STEP_CODE_SIZE    constant number := 2;

  k_PER_TABLE_AUD_TYPE  constant varchar2(1) := 'A';
  k_PER_TABLE_HIST_TYPE  constant varchar2(1) := 'H';
  k_PER_TABLE_OTHER_TYPE  constant varchar2(1) := 'O';

  k_PER_TABLE_LOAD_MECH_AUTO  constant varchar2(1) := 'A';
  k_PER_TABLE_LOAD_MECH_MAN  constant varchar2(1) := 'M';

  k_cant_continue_err_msg constant varchar2(1000) := 'Cannot continue as obfus_onoff_switch is turned ''OFF''.  exec obfuscation_control.switch_obfus_on_off(''ON''); to continue';
  k_obfus_not_ready_err_msg constant varchar2(1000) := 'Obfuscation not ready to run: check obfus_control table. The PRE, ANON and POST schema prefixes must be configured and not already RUNNING to run with setup and checked(optional) flags Y';

  k_const_CTYPE_COL_MASK_ROW_THRESHOLD CONSTANT varchar2(22) := 'COL_MASK_ROW_THRESHOLD';
  
  k_const_COL_MASK_ROW_THRESHOLD_DEF NUMBER := 100000;

end const;  
/