create or replace package body gp as


  procedure set_src_db_ver(p_src_db_ver varchar2) is
  begin
    src_db_ver := p_src_db_ver;
  end;
  
  procedure set_pt_db_ver(p_pt_db_ver varchar2) is
  begin
    pt_db_ver := p_pt_db_ver;
  end;
  
  procedure set_ss_log_file_dir     (p_ss_log_file_dir  varchar2)
  is
  begin
    ss_log_file_dir := p_ss_log_file_dir;
  end;
  
  function get_ss_log_file_dir return varchar2
  is
  begin
    return gp.ss_log_file_dir;
  end; 
  
  procedure set_ss_default_password  (p_ss_default_password varchar2)
  is
  begin
    ss_default_password := p_ss_default_password;
  end;
  
  procedure set_ss_pdb_name(p_ss_pdb_name  varchar2)
  is
  begin
    ss_pdb_name := p_ss_pdb_name;
  end;
  
  function get_ss_pdb_name      
    return varchar2
  is
  begin
    return gp.ss_pdb_name;
  end;   
  
  procedure set_ss_db_link (p_ss_db_link varchar2)
  is
  begin
    ss_db_link := p_ss_db_link;
  end;

  function get_ss_db_link 
     return varchar2
  is
  begin
    return gp.ss_db_link;
  end; 
  
  procedure set_col_mask_row_threshold  (p_col_mask_row_threshold   number) is
  begin
    col_mask_row_threshold := p_col_mask_row_threshold;
  end;

  procedure set_obfus_run_id(p_obfus_run_id number) is
  begin
    obfus_run_id := p_obfus_run_id;
    set_global_var('OBFUS_RUN_ID',p_obfus_run_id);    
  end;

  procedure set_ss_run_id(p_ss_run_id number) is
  begin
    ss_run_id := p_ss_run_id;
    set_global_var('SS_RUN_ID',p_ss_run_id);
  end;
  
  function get_ss_run_id 
    return number
  is
  begin
    return gp.ss_run_id;
  end;  

  procedure set_obfus_execution_id(p_execution_id number) is
  begin
    obfus_execution_id := p_execution_id;
    set_global_var('OBFUS_EXECUTION_ID',p_execution_id); 
  end;
  
  function  get_obfus_execution_id return number  
  is
  begin
    return gp.obfus_execution_id;
  end; 
  
  procedure set_ss_execution_id(p_execution_id number) is
  begin
    ss_execution_id := p_execution_id;
    set_global_var('SS_EXECUTION_ID',p_execution_id);    
  end;
  
  function  get_ss_execution_id return number    
  is
  begin
    return gp.ss_execution_id;
  end; 
  
  procedure set_src_prefix(p_src_prefix varchar2 )  is
  begin
    src_prefix := p_src_prefix;
  end;
  
  function get_src_prefix 
    return varchar2
  is
  begin
    return gp.src_prefix;
  end;  
  
  procedure set_tgt_prefix(p_tgt_prefix varchar2)  is
  begin
    tgt_prefix := p_tgt_prefix;
  end;
  
  function get_tgt_prefix 
    return varchar2
  is
  begin
    return gp.tgt_prefix;
  end;    
  
  procedure set_run_env(p_run_env varchar2) is
  begin
    run_env := p_run_env;
  end;
  
  function get_run_env 
    return varchar2
  is
  begin
    return gp.run_env;
  end;        
  
  procedure set_anon_version(p_anon_version varchar2)  is
  begin
      anon_version := p_anon_version;
  end;

  function get_anon_version 
    return varchar2
  is
  begin
    return gp.anon_version;
  end;    

  procedure set_bln_stop_job_overrun(p_stop_job_overrun boolean) is
  begin
      bln_stop_job_overrun := p_stop_job_overrun;
  end;

  function get_bln_stop_job_overrun
    return boolean
  is
  begin
    return gp.bln_stop_job_overrun;
  end; 

  procedure set_src_psm_password(p_src_psm_password  varchar2  DEFAULT 'prism') 
  is
  begin
      src_psm_password := p_src_psm_password;
  end;
  
  function get_src_psm_password
    return varchar2
  is
  begin
    return gp.src_psm_password;
  end;    
      
  procedure set_src_psm_servicename(p_src_psm_servicename  varchar2  DEFAULT 'OBFA')
  is
  begin
      src_psm_servicename := p_src_psm_servicename;
  end;
  
  function get_src_psm_servicename
    return varchar2
  is
  begin
    return gp.src_psm_servicename;
  end;  
  
  procedure set_src_psm_hostname(p_src_psm_hostname  varchar2  DEFAULT 'PSMPPSORA01')
  is
  begin
      src_psm_hostname := p_src_psm_hostname;
  end;
  
  function get_src_psm_hostname
    return varchar2
  is
  begin
    return gp.src_psm_hostname;
  end;  

  procedure set_fix_missing_part_attempts(p_fix_missing_part_attempts  number)  
  is
  begin
      fix_missing_part_attempts := p_fix_missing_part_attempts;
  end;
  
  function get_fix_missing_part_attempts 
     return number
  is
  begin
    return gp.fix_missing_part_attempts;
  end; 
  
  procedure set_subset_reload_attempts(p_subset_reload_attempts  number)
  is
  begin
      subset_reload_attempts := p_subset_reload_attempts;
  end;  
  
  function get_subset_reload_attempts 
    return number 
  is
  begin
    return gp.subset_reload_attempts;
  end; 
  
  procedure set_job_queue_processes
  is
  begin
     select value 
       into job_queue_processes
       from v$parameter
      where lower(name) = 'job_queue_processes';  
  end; 
  
  function get_job_queue_processes    
    return number 
  is
  begin
    return gp.job_queue_processes;
  end;
  
  procedure set_parallel_job_limit is
  begin
     select least(gp.parallel_job_limit,const.k_max_parallel_jobs)
       into parallel_job_limit
       from dual;  
  end; 
  
  function get_parallel_job_limit
    return number 
  is
  begin
    return nvl(gp.parallel_job_limit,gp.job_queue_processes);
  end;  
  
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
                    p_src_psm_servicename      varchar2  DEFAULT NULL) 
  is
  begin

     if p_src_prefix is not null then
        set_src_prefix(p_src_prefix);
     end if;

     if p_tgt_prefix is not null then
        set_tgt_prefix(p_tgt_prefix);
     end if;

     if p_run_env is not null then
        set_run_env(p_run_env);
     end if;

     if p_anon_version is not null then
        set_anon_version(p_anon_version);
     end if;

     if p_obfus_run_id is not null then
        set_obfus_run_id(p_obfus_run_id);
     end if;

     if p_ss_run_id is not null then
        set_ss_run_id(p_ss_run_id);
     end if;

     if p_obfus_execution_id is not null then
        set_obfus_execution_id(p_obfus_execution_id);
     end if;

     if p_ss_execution_id is not null then
        set_ss_execution_id(p_ss_execution_id);
     end if;

     if p_col_mask_row_threshold is not null then
        set_col_mask_row_threshold(p_col_mask_row_threshold);
     end if;

     set_bln_stop_job_overrun(p_stop_job_overrun);

     set_src_psm_password(p_src_psm_password);

     set_src_psm_hostname(p_src_psm_hostname);

     set_src_psm_servicename(p_src_psm_servicename);

     set_job_queue_processes;

     set_parallel_job_limit;

  end init_gp;
  
  procedure set_global_var ( p_global_name varchar2, p_global_value in varchar )
  is
     pragma autonomous_transaction;
  begin

      merge into ss_global_vars gv
      using (select 1 from dual) y
         on (     gv.global_name  = upper(p_global_name) )
          when matched
          then
              update
                 set global_value = upper(p_global_value)                  
          when not matched
          then
              insert ( global_name, global_value) values ( upper(p_global_name), p_global_value );     

     commit;

  end set_global_var;  
  
  function  get_global_var ( p_global_name varchar2 ) return varchar2    
  is
    v_global_value  ss_global_vars.global_value%type;
  begin
    begin
    
       select global_value
         into v_global_value
         from ss_global_vars
        where global_name = upper(p_global_name);
        
    exception
       when no_data_found then
          v_global_value := null;
    end;
    
    return v_global_value;
  end get_global_var;   
  
end gp;
/