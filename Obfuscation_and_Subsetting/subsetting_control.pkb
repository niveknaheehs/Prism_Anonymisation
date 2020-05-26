create or replace PACKAGE BODY subsetting_control
AS
 
  procedure truncate_report_tables
  is
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.truncate_report_tables';
  begin
    ut.log(const.k_subsys_subset,'truncating tables: ',null,null,const_module);
    --execute immediate 'truncate table ss_report reuse storage';
  exception
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,const_module || ' error ',v_code,v_errm,const_module);
  end truncate_report_tables;
  

  procedure merge_ss_ctrl_exec_result( p_ss_run_id        number,
                                       p_stage_step_code  varchar2,
                                       p_stmt_seq         number,
                                       p_execution_id     number,
                                       p_start_timestamp  timestamp,
                                       p_end_timestamp    timestamp,
                                       p_status           varchar2,
                                       p_ss_log_id        number )
  is
      pragma autonomous_transaction;
      const_module  CONSTANT  varchar2(62) := 'subsetting_control.merge_ss_ctrl_exec_result';
  begin

--      ut.log(const.k_subsys_subset,'merging into ss_ctrl_exec_result on ss_run_id = '||p_ss_run_id||
--                                   ' stage_step_code = '||p_stage_step_code||' stmt_seq = '||p_stmt_seq||
--                                   ' execution_id = '||p_execution_id,null,null,const_module);

      merge into ss_ctrl_exec_result x
      using (select 1 from dual) y
         on (x.ss_run_id = p_ss_run_id and x.stage_step_code = p_stage_step_code and x.stmt_seq = p_stmt_seq and x.execution_id = p_execution_id )
          when matched
          then
              update set x.start_timestamp = nvl(p_start_timestamp,x.start_timestamp),
                         x.end_timestamp   = nvl(p_end_timestamp,x.end_timestamp),
                         x.status          = p_status,
                         ss_log_id         = p_ss_log_id
          when not matched
          then
              insert (ss_run_id,stage_step_code,stmt_seq,execution_id,start_timestamp,end_timestamp,status,ss_log_id)
              values (p_ss_run_id,p_stage_step_code,p_stmt_seq,p_execution_id,p_start_timestamp,p_end_timestamp,p_status,p_ss_log_id);
      commit;

  end merge_ss_ctrl_exec_result;


  function create_ss_ctrl
    return number is
     pragma autonomous_transaction;
     v_run_id_seq NUMBER := NULL;
     const_module  CONSTANT  varchar2(62) := 'subsetting_control.create_ss_ctrl';
  begin
     begin
        insert into ss_ctrl(ss_run_id,src_prefix,tgt_prefix,run_env,anon_version,ss_status,run_start_time,run_completed_date)
             values (SS_RUN_ID_SEQ.nextval,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version,'PENDING',null,null);
        commit;

        v_run_id_seq := SS_RUN_ID_SEQ.currval;
        gp.set_ss_run_id( v_run_id_seq );

        ut.log(const.k_subsys_subset,'truncate_report_tables ready for new run_id: '||to_char(v_run_id_seq),null,null,const_module);
        subsetting_control.truncate_report_tables;

     exception
        when dup_val_on_index then
           select ss_run_id
             into v_run_id_seq
             from ss_ctrl
            where src_prefix = gp.src_prefix
              and tgt_prefix = gp.tgt_prefix
              and run_env = gp.run_env;

           ut.log(const.k_subsys_subset,'Subsetting control record already exists for source: '|| gp.src_prefix || ' , target: ' || gp.tgt_prefix || ' ,run_env: ' || gp.run_env,null,null,const_module);
     end;

     return v_run_id_seq;
  end create_ss_ctrl;


  function fn_existing_ss_run_id (p_src_prefix in varchar2, p_run_env in varchar2, p_tgt_prefix in varchar2, p_ss_run_id number DEFAULT NULL)
    return number
  is
    v_code          number;
    v_errm          varchar2(4000);
    v_ss_run_id     ss_ctrl.ss_run_id%TYPE;
    const_module    CONSTANT  varchar2(62) := 'subsetting_control.fn_existing_ss_run_id';
  begin
    begin

      select ss_run_id
        into v_ss_run_id
        from ss_ctrl
       where src_prefix   = p_src_prefix
         and run_env      = p_run_env
         and tgt_prefix   = p_tgt_prefix
         and ss_run_id = nvl(p_ss_run_id,ss_run_id);

    exception
      when no_data_found then
         v_ss_run_id := null;
      when others then
         v_code := SQLCODE;
         v_errm := SUBSTR(SQLERRM,1,4000);
         ut.log(const.k_subsys_subset,const_module || ' error ',v_code,v_errm,const_module);
    end;
    ut.log(const.k_subsys_subset,'Check for existing ss_run_id returns: ' || to_char(v_ss_run_id),SQLCODE,SQLERRM,const_module);

    return v_ss_run_id;
  end fn_existing_ss_run_id;


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
                            p_src_metadata_loaded   VARCHAR2 DEFAULT NULL)
  is
     pragma autonomous_transaction;
     const_module  CONSTANT  varchar2(62) := 'subsetting_control.update_ss_ctrl';
  begin

     ut.log(const.k_subsys_subset,'Updating ss_ctrl for p_ss_run_id: '|| to_char(p_ss_run_id) || ' and p_run_env: ' || p_run_env,null,null,const_module);

     update ss_ctrl
        set ss_status            = nvl(p_ss_status,ss_status),
            ss_stages_loaded     = nvl(p_ss_stages_loaded,ss_stages_loaded),
            dd_loaded            = nvl(p_dd_loaded,dd_loaded),
            ss_syns_created      = nvl(p_ss_syns_created,ss_syns_created),
            ss_config_loaded     = nvl(p_ss_config_loaded,ss_config_loaded),
            src_metadata_loaded  = nvl(p_src_metadata_loaded,src_metadata_loaded),
            run_start_time       = DECODE(p_ss_status,'RUNNING',sysdate,run_start_time),
            run_completed_date   = DECODE(p_ss_status,'COMPLETED',sysdate,'RUNNING',null,'FAILED',null,run_completed_date)
      where ss_run_id    = p_ss_run_id
        and src_prefix   = p_src_prefix
        and run_env      = p_run_env
        and anon_version = p_anon_version
        and tgt_prefix   = p_tgt_prefix;

     if sql%rowcount = 0 then
        ut.log(const.k_subsys_subset,'No matching ss_ctrl record found to update',SQLCODE,SQLERRM,const_module);
     end if;
     ut.log(const.k_subsys_subset,'Updated ss_ctrl',SQLCODE,SQLERRM,const_module);

     commit;
  end update_ss_ctrl;

  procedure check_ss_ready( p_ss_run_id       IN OUT NUMBER,
                            p_src_prefix      IN OUT VARCHAR2,
                            p_tgt_prefix      IN OUT VARCHAR2,
                            p_run_env         IN OUT VARCHAR2,
                            p_anon_version    IN OUT VARCHAR2 ) is
     const_module  CONSTANT   varchar2(62) := 'subsetting_control.check_ss_ready';
  begin

     select ss_run_id, src_prefix, tgt_prefix, run_env, anon_version
       into p_ss_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version
       from (
              select ss_run_id, src_prefix, tgt_prefix, run_env, anon_version
                from ss_ctrl
               where ss_status IN ( 'PENDING', 'FAILED', 'COMPLETED', 'ON' ) -- allow recovery and re-runs of specified stage ranges
                 and src_prefix = nvl(p_src_prefix,src_prefix)
                 and tgt_prefix = nvl(p_tgt_prefix,tgt_prefix)
                 and run_env    = nvl(p_run_env,run_env)
                 and anon_version = nvl(p_anon_version,anon_version)
                 and ss_run_id = nvl(p_ss_run_id,ss_run_id)
              order by ss_run_id desc
       )
      where rownum = 1;
  exception
     when no_data_found
     then
        p_ss_run_id := null;
        p_src_prefix   := null;
        p_tgt_prefix   := null;
        p_run_env      := null;
        p_anon_version := null;
  end check_ss_ready;

 
  procedure create_ss_monitoring_job
  is
    const_module      CONSTANT  varchar2(62) := 'subsetting_control.create_ss_monitoring_job';

    v_code                  number;
    v_errm                  varchar2(4000);  
    
    v_execution_id          number;
    v_monitor_job_action    varchar2(4000);
    v_repeat_interval       varchar2(4000);
  
  begin
    
     ut.log(const.k_subsys_subset,'Creating '|| const.k_SS_JOB_MONITOR ||' scheduled job to run every '||to_char(const.k_monitor_jobs_interval_seconds)||
                                      ' seconds merging into job_execution and logging number of incomplete and running jobs',null,null,const_module);
     if gp.ss_execution_id is null
     then
         v_execution_id := execution_seq.nextval;
         gp.set_ss_execution_id(v_execution_id);
         ut.log(const.k_subsys_subset,'gp.ss_execution_id = '||to_char(gp.ss_execution_id),null,null,const_module);
     end if;    
         
     v_monitor_job_action := 'BEGIN ut.monitor_jobs('||chr(39)||const.k_subsys_subset||chr(39)||','||gp.ss_run_id||','||gp.ss_execution_id||','||chr(39)||SYSDATE||chr(39)||');  END;';    
     
     v_repeat_interval := 'FREQ=SECONDLY;INTERVAL='||const.k_monitor_jobs_interval_seconds;
  
     ut.create_ss_job ( p_job_name               =>  const.k_SS_JOB_MONITOR,
                        p_job_action             =>  v_monitor_job_action,
                        p_start_stage_step_code  =>  null,
                        p_end_stage_step_code    =>  null,   
                        p_job_type               =>  'PLSQL_BLOCK',
                        p_repeat_interval        =>  v_repeat_interval );

  exception
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  end create_ss_monitoring_job;


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
                            p_check_dependency    VARCHAR2 DEFAULT 'Y' )  
  is
    const_module      CONSTANT  varchar2(62) := 'subsetting_control.create_subset';

    v_ss_run_id             ss_ctrl.ss_run_id%TYPE := null;
    v_code                  number;
    v_errm                  varchar2(4000);
    v_parallel_run_start    date;
    v_incomplete_job_cnt    number := 0;
    v_tgt_session_count     number := 0;
    rec_ss_ctrl             ss_ctrl%ROWTYPE;
    v_execution_id          number;
	  v_stage_tab             string_list_257;
    v_msg_tab               string_list_4000;

  begin

    gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(p_src_prefix,p_run_env,p_tgt_prefix);

    gp.set_src_prefix(p_src_prefix );
    gp.set_tgt_prefix(p_tgt_prefix);
    gp.set_run_env(p_run_env);
    gp.set_anon_version(p_anon_version);
    gp.set_ss_log_file_dir(p_datapump_dir);
    gp.set_ss_db_link (p_ss_db_link);
    gp.set_job_queue_processes;
    gp.set_parallel_job_limit;

    --ut.validate_db_link;

    ut.log(const.k_subsys_subset,'calling ut.get_tgt_psm_session_count to check for any tgt schema sessions',null,null,const_module);
    v_tgt_session_count := ut.get_tgt_psm_session_count(const.k_subsys_subset,gp.get_tgt_prefix);
    if v_tgt_session_count > 0 then
       raise excep.x_tgt_prism_sessions_exist;
    end if;

    if gp.ss_run_id IS NOT NULL
    then
       begin
          select * into rec_ss_ctrl from ss_ctrl  where ss_run_id = gp.ss_run_id;
       exception
         when no_data_found then
           RAISE_APPLICATION_ERROR(-20006,' ss_run_id ' || gp.ss_run_id || ' does not exist');
       end;
       if (    gp.src_prefix <> rec_ss_ctrl.src_prefix
            or gp.run_env    <> rec_ss_ctrl.run_env
            or gp.tgt_prefix <> rec_ss_ctrl.tgt_prefix )
       then
          --x_obfus_run_param_mismatch
          RAISE_APPLICATION_ERROR(-20005,'src, tgt or run_env does not match existing ss_run_id.');
       end if;
    else
      gp.ss_run_id := subsetting_control.create_ss_ctrl;
      --initialise
      ut.log(const.k_subsys_subset,'initialising ss_ctrl record',SQLCODE,SQLERRM,const_module);
       begin
          select * into rec_ss_ctrl from ss_ctrl  where ss_run_id = gp.ss_run_id;
       exception
         when no_data_found then
           RAISE_APPLICATION_ERROR(-20006,' ss_run_id ' || gp.ss_run_id || ' does not exist');
       end;
    end if;

	  if ut.can_continue(const.k_subsys_subset) then
          ut.log(const.k_subsys_subset,'load_comp_list('||p_comp_list||')',null,null,const_module);
          load_comp_list(p_comp_list);
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    -- Load table exclusions
    if ut.can_continue(const.k_subsys_subset) then
          ut.log(const.k_subsys_subset,'load_table_exclusions',null,null,const_module);
          ut.load_table_exclusions;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;


	 if ut.can_continue(const.k_subsys_subset) then
       if p_gen_metadata then
          ut.gen_src_metadata(p_comp_list, null);
       else
          ut.create_src_synonyms;  -- where not exist
          if p_gen_load_views then
             ut.build_all_load_views;
          end if;
       end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;
    
	  if ut.can_continue(const.k_subsys_subset) then
          ut.log(const.k_subsys_subset,'load_src_schema_list '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);
          load_src_schema_list;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

  if ut.can_continue(const.k_subsys_subset) then
      if (gp.ss_run_id is not null and rec_ss_ctrl.ss_syns_created = 'N') then
          ut.log(const.k_subsys_subset,'create_src_syns '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);
          create_src_syns;
          ut.log(const.k_subsys_subset,'load_environ_stmts '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);
          load_environ_stmts;
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;


	  if ut.can_continue(const.k_subsys_subset) then
      if (gp.ss_run_id is not null and rec_ss_ctrl.src_metadata_loaded = 'N') then
          ut.log(const.k_subsys_subset,'load_src_metadata '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);
          load_src_metadata;
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;


	  if ut.can_continue(const.k_subsys_subset) then
      if (gp.ss_run_id is not null and rec_ss_ctrl.dd_loaded = 'N') then
          ut.log(const.k_subsys_subset,'ut.load_dd '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);
          ut.load_dd(const.k_subsys_subset);
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

	  if ut.can_continue(const.k_subsys_subset) then
      if (gp.ss_run_id is not null and rec_ss_ctrl.ss_config_loaded = 'N') then
          ut.log(const.k_subsys_subset,'load_subset_config'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);
          load_subset_config;
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;


    if ut.can_continue(const.k_subsys_subset) then
      if (gp.ss_run_id is not null and rec_ss_ctrl.ss_stages_loaded = 'N') then
          ut.log(const.k_subsys_subset,'load_ss_stages '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);
          load_ss_stages; 
          
          if p_stop_stage_step is not null then
            ut.switch_subsystem_off_after_stage_step(const.k_subsys_subset,p_stop_stage_step);
          end if;
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;


    if ut.can_continue(const.k_subsys_subset) then

      ut.log(const.k_subsys_subset,'execute_ss_steps '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

      if const.k_max_parallel_jobs > 1 -- Parallel execution enabled
      then
      
         delete job_execution where subsystem = const.k_subsys_subset and run_id = gp.ss_run_id;
         ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from job_execution for ss_run_id '||gp.ss_run_id,null,null,const_module);
         commit;
         -- An SS_JOB_MONITOR scheduled job is created before commencement of parallel operations
         -- This is detected in execute_ss_steps for step_type 'J'ob which will call create_ss_monitoring_job 
      
      end if;     
         
      if ut.can_continue(const.k_subsys_subset) then
         subsetting_control.update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_ss_status => 'RUNNING');
         execute_ss_steps(p_start_step, p_end_step,p_check_dependency);
      else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);
      end if;

      if const.k_max_parallel_jobs > 1 -- Parallel execution enabled
      then
         --  wait for any running SS jobs to complete and then stop/drop any remaining monitoring jobs
         ut.wait_for_ss_jobs;     
      
         if gp.bln_stop_job_overrun
         then
            ut.drop_overrun_prevention_jobs(const.k_subsys_subset);
         end if;      
      
         begin
            ut.log(const.k_subsys_subset,'Stopping/Dropping '|| const.k_SS_JOB_MONITOR ||' job',null,null,const_module);

            begin
               DBMS_SCHEDULER.STOP_JOB(job_name => const.k_SS_JOB_MONITOR);
            exception
               when excep.x_job_not_running
               then
                  DBMS_SCHEDULER.DROP_JOB(job_name => const.k_SS_JOB_MONITOR);
               when excep.x_unknown_job
               then                  
                  null;
            end;
      
            ut.merge_job_execution(const.k_subsys_subset,gp.ss_run_id,const.k_SS_JOB_MONITOR,gp.ss_execution_id,null,null,null);
                        
         exception
            when others then
               v_code := SQLCODE;
               v_errm := SUBSTR(SQLERRM,1,4000);
               ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
         end;
      end if;   
                  
      subsetting_control.update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_ss_status => 'COMPLETED');
      ut.log(const.k_subsys_subset,'Finish'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);
   else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

   -- Following COMPLETION need to manually call to ut.recompile which also generates invalid object reports

 exception
    when excep.x_obfus_not_ready then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      rollback;
      RAISE_APPLICATION_ERROR(-20001,'Subsetting not ready to run: check ss_ctrl table.');
    when excep.x_cannot_continue
    then
       rollback;
       raise;
    when excep.x_tgt_prism_sessions_exist
    then
       v_code := SQLCODE;
       v_errm := SUBSTR(to_char(v_tgt_session_count)||' existing sessions found to target prism schemas.  These sessions must be terminated before allowing subsetting to continue and attempt to drop and recreate the schemas.'||SQLERRM,1,4000);
       ut.log(const.k_subsys_subset,v_errm,v_code,v_errm,const_module);
      rollback;
      raise;
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      subsetting_control.update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_ss_status => 'FAILED');
      rollback;
 end create_subset;

function fn_convert_stage_step_code ( p_stage_step_code varchar2 )
   return varchar2
is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.fn_convert_stage_step_code';
   v_stage_step_code  ss_ctrl_stmts.stage_step_code%type;
begin
   select replace(p_stage_step_code,stage_code_alpha,stage_order)
     into v_stage_step_code
     from ss_stage_order
    where substr(p_stage_step_code,1,2) = stage_code_alpha;

   return  v_stage_step_code;
end fn_convert_stage_step_code;

function fn_get_dependent_status ( p_dependent_stage_step_code varchar2 )
   return varchar2
is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.fn_get_dependent_status';
   v_status         ss_ctrl_exec_result.status%type;
   v_running_count  number;
begin
   begin
      select distinct nvl(res.status,'NONE') status
        into v_Status
        from dual left outer join
            (
              select scer.status
              from ss_ctrl_exec_result scer
              where scer.stage_step_code = p_dependent_stage_step_code
              and  scer.ss_run_id = gp.ss_run_id
              and  execution_id = (select max(scer1.execution_id)
                                    from ss_ctrl_exec_result scer1
                                    where scer1.stage_step_code = p_dependent_stage_step_code
                                    and  scer1.ss_run_id = gp.ss_run_id
                                    group by scer1.stage_step_code,scer1.ss_run_id)
            ) res on 1 = 1 ;
    
   exception 
      when no_data_found then
         v_Status := 'NONE';
      when too_many_rows then
         ut.log(const.k_subsys_subset,'stage_step_code '||p_dependent_stage_step_code||' has multiple statuses: checking if any currently running',null,null,const_module,p_dependent_stage_step_code);    
         begin
           select count(*)
             into v_running_count
             from ss_ctrl_exec_result scer
            where scer.stage_step_code = p_dependent_stage_step_code
              and scer.ss_run_id = gp.ss_run_id
              and scer.status = const.k_STARTED
              and execution_id = ( select max(scer1.execution_id)
                                     from ss_ctrl_exec_result scer1
                                    where scer1.stage_step_code = p_dependent_stage_step_code
                                      and scer1.ss_run_id = gp.ss_run_id );
         exception
           when no_data_found then
             v_Status := 'NONE';
         end;
         
         if v_running_count > 0
         then
            v_Status := const.k_STARTED;  
         end if;
   end;
          
   return  v_status;
end fn_get_dependent_status;


function fn_get_dependent_step_type ( p_dependent_stage_step_code varchar2 )
   return varchar2
is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.fn_get_dependent_step_type';
   v_step_type  ss_ctrl_stmts.step_type%type;
begin
   begin
      select distinct step_type
        into v_step_type
        from ss_ctrl_stmts
       where stage_step_code = p_dependent_stage_step_code
         and ss_run_id = gp.ss_run_id;
   exception 
      when no_data_found then
         v_step_type := '';
      when too_many_rows then
         ut.log(const.k_subsys_subset,'stage_step_code '||p_dependent_stage_step_code||' has multiple step types.',null,null,const_module,p_dependent_stage_step_code);                 
         v_step_type := '';         
   end;
          
   return  v_step_type;
end fn_get_dependent_step_type;


function fn_get_dep_ref_part_stage_step_code ( p_owner varchar2, p_table_name varchar2 )
   return varchar2
is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.fn_get_dep_ref_part_stage_step_code';
   v_stage_step_code  ss_ctrl_stmts.stage_step_code%type;
   
begin

   begin

      select distinct scs.stage_step_code --, dc2.owner, dc2.table_name
        into v_stage_step_code
        from temp_src_dd_constraints dc1
        join temp_src_dd_constraints dc2 on dc1.r_constraint_name = dc2.constraint_name
        join ss_ctrl_stmts scs on scs.owner = dc2.owner and scs.object_name = dc2.table_name
        join md_ddl mdd on mdd.owner = dc1.owner and mdd.base_object_name = dc1.table_name
       where dc1.constraint_type = 'R'
         and dc1.owner = p_owner 
         and dc1.table_name = p_table_name
         and dc1.constraint_name = mdd.ref_ptn_constraint_name
         and mdd.object_type = 'PARTITION'
         and dc2.constraint_type in ('P','U')
         and scs.stage_type = 'LOAD_DATA_SUBSET'
         and scs.ss_run_id = gp.get_ss_run_id;
          
   exception 
      when no_data_found then
         v_stage_step_code := '';
      when too_many_rows then
         ut.log(const.k_subsys_subset,'More than one dependent stage_step_code for p_owner: '||p_owner||' p_table_name:'|| p_table_name,null,null,const_module);                 
         v_stage_step_code := '';       
         raise;
   end;
          
   return v_stage_step_code;
   
end fn_get_dep_ref_part_stage_step_code;


procedure execute_ss_steps(p_start_step        varchar2,
                           p_end_step          varchar2,
                           p_check_dependency  varchar2  default 'Y',
                           p_ss_run_id         number    default null,
                           p_execution_id      number    default null,
                           p_src_prefix        varchar2  default null,
                           p_tgt_prefix        varchar2  default null,
                           p_run_env           varchar2  default null,
                           p_anon_version      varchar2  default null) as

   const_module  CONSTANT  varchar2(62) := 'subsetting_control.execute_ss_steps';

   cursor get_stmts(p_start_step varchar2,p_end_step varchar2) is
      select scs.ss_run_id, scs.stage_type, scs.stage_step_code, scs.dependent_ss_code, scs.stmt_seq, scs.step_type, scs.owner, scs.object_type, scs.object_name, scs.stmt
        from ss_ctrl_stmts scs
        join ss_stage_order sso on scs.stage_type = sso.stage_type
       where ss_run_id = gp.get_ss_run_id
         and to_number(REPLACE(REPLACE(scs.stage_step_code,sso.stage_code_alpha,sso.stage_order),'S','0')) >= to_number(REPLACE(NVL2(p_start_step,subsetting_control.fn_convert_stage_step_code(p_start_step),REPLACE(scs.stage_step_code,sso.stage_code_alpha,sso.stage_order)),'S','0'))
         and to_number(REPLACE(REPLACE(scs.stage_step_code,sso.stage_code_alpha,sso.stage_order),'S','0')) <= to_number(REPLACE(NVL2(p_end_step,subsetting_control.fn_convert_stage_step_code(p_end_step),REPLACE(scs.stage_step_code,sso.stage_code_alpha,sso.stage_order)),'S','0'))
     order by sso.stage_order,scs.stage_step_code,stmt_seq;

   v_tsStart TIMESTAMP;
   v_tsEnd TIMESTAMP;

   v_execution_id NUMBER;
   vLogID NUMBER;
   v_code NUMBER;
   v_errm         varchar2(4000);
   v_sStmt        varchar2(32676);
   v_sql          varchar2(32676);
   v_job_errors   varchar2(4000);
   v_job_status   varchar2(4000);

   v_sDependentStatus varchar2(10);
   v_dependent_step_type varchar2(1);
	 v_prev_stage_step_code  ss_ctrl_stmts.stage_step_code%type;
   v_nCount    number;
   v_rowcount  number;
begin

  if p_execution_id is null 
  then
    if gp.get_ss_execution_id is null
    then
      v_execution_id := execution_seq.nextval;
      gp.set_ss_execution_id(v_execution_id);
      --ut.log(const.k_subsys_subset,'gp.ss_execution_id = '||to_char(gp.ss_execution_id),null,null,const_module);
    else
      v_execution_id := gp.get_ss_execution_id;
    end if;
  else
    gp.set_ss_execution_id(p_execution_id);
    --ut.log(const.k_subsys_subset,'gp.ss_execution_id = '||to_char(gp.ss_execution_id),null,null,const_module);

    gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(p_src_prefix,p_run_env,p_tgt_prefix,p_ss_run_id);
    gp.set_src_prefix(p_src_prefix );
    gp.set_tgt_prefix(p_tgt_prefix) ;
    gp.set_run_env(p_run_env);
    gp.set_anon_version(p_anon_version);
  end if;

  ut.log(const.k_subsys_subset,'Executing '||const_module||' with p_start_step: '||p_start_step||' p_end_step: '||p_end_step||' p_check_dependency: '||p_check_dependency,null,null,const_module);

  v_prev_stage_step_code := null;

  --ut.log(const.k_subsys_subset,'Attempting to Process cursor get_stmts for gp.ss_run_id: '|| gp.ss_run_id,null,null,const_module);

  for get_stmts_rec in get_stmts(p_start_step, p_end_step) loop
  
    ut.log(const.k_subsys_subset,'Processing cursor get_stmts for stage_step_code: '|| get_stmts_rec.stage_step_code || 
                                                                ' stmt_seq: '|| get_stmts_rec.stmt_seq || 
                                                                ' gp.ss_run_id: '|| gp.ss_run_id,null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);

    if ( get_stmts_rec.step_type in ('S','P')
         and
         substr(get_stmts_rec.stage_step_code,1,const.k_STAGE_CODE_SIZE + const.k_STAGE_CODE_ALPHA_SIZE)  <> substr(v_prev_stage_step_code,1,const.k_STAGE_CODE_SIZE + const.k_STAGE_CODE_ALPHA_SIZE)
    ) then
        commit;
    end if;
    
    if ut.can_continue(const.k_subsys_subset)
    then
      begin

        --dbms_output.put_line('executing '||get_stmts_rec.stage_step_code ||' stmt_seq '||get_stmts_rec.stmt_seq );

        v_sStmt := get_stmts_rec.stmt;

        -- Only attempt to execute if dependent stage_step code has been executed
        if get_stmts_rec.dependent_ss_code is not null then
           v_sDependentStatus := fn_get_dependent_status(get_stmts_rec.dependent_ss_code);
           if v_sDependentStatus = const.k_STARTED
           then
              v_Dependent_Step_Type := fn_get_dependent_step_type(get_stmts_rec.dependent_ss_code);
              if v_Dependent_Step_Type = 'J' -- asynchronous execution so wait if running
              then
                 v_nCount := 0;
                 while v_sDependentStatus = const.k_STARTED
                 loop
                    v_nCount := v_nCount + 1;
                    ut.sleep(const.k_subsys_subset,const.k_sleep_seconds);
                    v_sDependentStatus := fn_get_dependent_status(get_stmts_rec.dependent_ss_code);
                    if mod(v_nCount,30) = 0 then
                      ut.log(const.k_subsys_subset,'Waiting for running job to complete: dependent stage_step_code is: '||get_stmts_rec.dependent_ss_code||' status is: '||v_sDependentStatus,null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);                 
                    end if;
                 end loop;
                 ut.log(const.k_subsys_subset,'Job stage_step_code '||get_stmts_rec.dependent_ss_code||' status is '||v_sDependentStatus,null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);                 
              end if;
           end if;
        else
          v_sDependentStatus := const.k_COMPLETED;
        end if;

        if p_check_dependency = 'N' or (p_check_dependency = 'Y' and  v_sDependentStatus = const.k_COMPLETED)  then
          case when get_stmts_rec.step_type = 'P' then

            begin
               v_tsStart := systimestamp;
               ut.log(const.k_subsys_subset,'merge_ss_ctrl_exec_result: gp.ss_execution_id: ' || gp.ss_execution_id,null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
               merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,null,const.k_STARTED,null);            
               ut.log(const.k_subsys_subset,'executing: '||get_stmts_rec.stage_step_code ||' stmt : ' ||substr(v_sStmt,1,4000 - length('executing: '||get_stmts_rec.stage_step_code ||' stmt : ')),null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
               execute immediate 'begin '|| v_sStmt ||'; end;';
               v_tsEnd := systimestamp;
               merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,v_tsEnd,const.k_COMPLETED,null);
               
            exception
               when others then
                  v_code := SQLCODE;
                  v_errm := SUBSTR(SQLERRM,1,4000);
                  ut.log(const.k_subsys_subset,substr('Error: '||dbms_utility.format_error_backtrace(),1,4000),v_code,v_errm,const_module);                  
                  raise;
            end;

          when get_stmts_rec.step_type = 'J' then
            
             while ( v_job_status is null or v_job_status not in ( 'RUNNING','SCHEDULED' ) )
             loop
               
               v_job_status := ut.get_job_status(const.k_subsys_subset,const.k_SS_JOB_MONITOR);
               case v_job_status
                 when 'RUNNING'
                 then 
                    ut.log(const.k_subsys_subset,const.k_SS_JOB_MONITOR||' is running',null,null,const_module);
              
                 when 'SUCCEEDED'-- or null
                 then    
                    ut.log(const.k_subsys_subset,const.k_SS_JOB_MONITOR||' previously completed successfully so creating a new one',null,null,const_module);
                    create_ss_monitoring_job;
              
                 when 'FAILED'
                 then
                    v_job_errors := ut.get_job_errors(const.k_subsys_subset,const.k_SS_JOB_MONITOR);
                    ut.log(const.k_subsys_subset,substr('Monitoring job '||const.k_SS_JOB_MONITOR||' has failed with errors: '||v_job_errors,1,4000),null,null,const_module); 
                    raise excep.x_job_failure;
              
                 when 'STOPPED'
                 then
                    ut.log(const.k_subsys_subset,'Monitoring job '||const.k_SS_JOB_MONITOR||' has stopped.  Attempting to run',null,null,const_module); 
                    --raise excep.x_job_stopped;  
                    ut.run_job(const.k_subsys_subset,const.k_SS_JOB_MONITOR); 
                    
                 when 'SCHEDULED'
                 then
                    ut.log(const.k_subsys_subset,'Monitoring job '||const.k_SS_JOB_MONITOR||' is scheduled (waiting to run)',null,null,const_module); 
 
                 when 'DISABLED'
                 then
                    ut.log(const.k_subsys_subset,'Monitoring job '||const.k_SS_JOB_MONITOR||' is disabled; attempting to enable ...',null,null,const_module); 
                    dbms_scheduler.enable(const.k_SS_JOB_MONITOR);                   
                    
                 else
                   -- raise excep.x_unknown_job_status; 
                   if v_job_status is null then
                      ut.log(const.k_subsys_subset,const.k_SS_JOB_MONITOR||' does not exist so creating a new one',null,null,const_module);               
                      create_ss_monitoring_job;                 
                   else
                      ut.log(const.k_subsys_subset,'Monitoring job '||const.k_SS_JOB_MONITOR||' is at status: '||v_job_status,null,null,const_module); 
                      raise_application_error( -20012, v_job_status);
                   end if;   
                 end case;            
              end loop;
            begin
              v_tsStart := systimestamp;
              ut.log(const.k_subsys_subset,'merge_ss_ctrl_exec_result: gp.ss_execution_id: ' || gp.ss_execution_id,null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
              merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,null,const.k_STARTED,null);
              ut.log(const.k_subsys_subset,'executing create job stmt: '||get_stmts_rec.stage_step_code ||' stmt : ' ||substr(v_sStmt,1,4000 - length('executing create job stmt: '||get_stmts_rec.stage_step_code ||' stmt : ')),null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);

              -- This stmt creates a job which executes a procedure ut.execute_ss_step which merges the result, to prevent false COMPLETION on successfully creating job.
              execute immediate 'begin '||
                 'ut.create_ss_job('||chr(39)||'SS_LOAD_'||get_stmts_rec.stage_step_code||'_'||get_stmts_rec.stmt_seq||chr(39)||','
                                    ||chr(39)||'BEGIN ut.execute_stmt('''||chr(39)||const.k_subsys_subset||chr(39)||''','''||chr(39)||v_sStmt||chr(39)||''','''||'''LOAD_DATA_SUBSET'''||''','''||chr(39)||get_stmts_rec.stage_step_code||chr(39)||''','||get_stmts_rec.stmt_seq||'); END;'||chr(39)||','
                                    ||chr(39)||get_stmts_rec.stage_step_code||chr(39)||','
                                    ||chr(39)||get_stmts_rec.stage_step_code||chr(39)||','
                                    ||get_stmts_rec.stmt_seq||')' ||'; end;';               
              
            exception
               when others then
                  raise;
            end;


          when get_stmts_rec.step_type = 'S' then

            v_tsStart := systimestamp;
            ut.log(const.k_subsys_subset,'merge_ss_ctrl_exec_result',null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
            merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,null,const.k_STARTED,null);
            ut.log(const.k_subsys_subset,'executing: '||get_stmts_rec.stage_step_code ||' stmt : ' ||substr(v_sStmt,1,4000 - length('executing: '||get_stmts_rec.stage_step_code ||' stmt : ')),null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
            execute immediate v_sStmt;
            v_rowcount := sql%rowcount;
            --if v_rowcount > 0
            --then
               commit;
               ut.log(const.k_subsys_subset,v_rowcount||' rows affected',null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);               
            --end if;
            v_tsEnd := systimestamp;

            merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,v_tsEnd,const.k_COMPLETED,null);

          when get_stmts_rec.step_type = 'D' then

            v_tsStart := systimestamp;
            ut.log(const.k_subsys_subset,'merge_ss_ctrl_exec_result',null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
            merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,null,const.k_STARTED,null);

            begin
            
               if get_stmts_rec.stage_type = 'ADD_TRIGGERS'
               then
                  v_sql := 'alter session set current_schema='||gp.get_tgt_prefix||'_'||get_stmts_rec.owner;
                  ut.log(const.k_subsys_subset,'executing: '||v_sql,null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  ut.log(const.k_subsys_subset,'executing: '||get_stmts_rec.stage_step_code ||' stmt : ' ||substr(v_sStmt,1,4000 - length('executing: '||get_stmts_rec.stage_step_code ||' stmt : ')),null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);

                  execute immediate v_sql;
                  
                  execute immediate v_sStmt;

                  v_sql := 'alter session set current_schema='||gp.get_run_env;
                  execute immediate v_sql; 
                  ut.log(const.k_subsys_subset,'restored current_schema to '||gp.get_run_env,null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);                 
               else
                  ut.log(const.k_subsys_subset,'executing: '||get_stmts_rec.stage_step_code ||' stmt : ' ||substr(v_sStmt,1,4000 - length('executing: '||get_stmts_rec.stage_step_code ||' stmt : ')),null,null,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  execute immediate v_sStmt; 
               end if;
               
               v_tsEnd := systimestamp;
               merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,v_tsEnd,const.k_COMPLETED,null);
            exception
               when excep.x_table_not_exist then
                  vLogID := ut.log(const.k_subsys_subset,'Table does not exist: '||get_stmts_rec.stage_step_code,SQLCODE,SQLERRM,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,systimestamp,const.k_FAILED,vLogID);
                  if get_stmts_rec.stage_type = 'ADD_TRIGGERS'
                  then                   
                     ut.restore_session_to_run_env; 
                  end if;   
                     
               when excep.x_object_name_already_used then
                  vLogID := ut.log(const.k_subsys_subset,'Object already exists: '||get_stmts_rec.stage_step_code,SQLCODE,SQLERRM,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,systimestamp,const.k_COMPLETED,vLogID);

               when excep.x_user_not_exist then
                  vLogID := ut.log(const.k_subsys_subset,'User does not exist: '||get_stmts_rec.stage_step_code,SQLCODE,SQLERRM,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,systimestamp,const.k_COMPLETED,vLogID);

               when excep.x_columns_already_indexed then
                  vLogID := ut.log(const.k_subsys_subset,'column list already indexed: '||get_stmts_rec.stage_step_code,SQLCODE,SQLERRM,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,systimestamp,const.k_COMPLETED,vLogID);

               when excep.x_tab_already_partitioned then --CONV_TO_PART: 'ORA-14427: table does not support modification to a partitioned state DDL' 
                  vLogID := ut.log(const.k_subsys_subset,'table already partitioned: '||get_stmts_rec.owner||'.'||get_stmts_rec.object_name||': '||get_stmts_rec.object_type,SQLCODE,SQLERRM,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,systimestamp,const.k_COMPLETED,vLogID);

               when excep.x_parent_not_part then --CONV_TO_PART: 'ORA-14653: parent table of a reference-partitioned table must be partitioned'
                  vLogID := ut.log(const.k_subsys_subset,'parent table of a reference-partitioned table must be partitioned: '||get_stmts_rec.owner||'.'||get_stmts_rec.object_name||': '||get_stmts_rec.object_type,SQLCODE,SQLERRM,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,systimestamp,const.k_FAILED,vLogID);

               when excep.x_ref_cons_already_exists then  -- 'MISSING_CONSTRAINTS': ORA-02275: such a referential constraint already exists in the table
                  vLogID := ut.log(const.k_subsys_subset,'Ref constraint already exists: '||get_stmts_rec.owner||'.'||get_stmts_rec.object_name,SQLCODE,SQLERRM,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                  merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,systimestamp,const.k_COMPLETED,vLogID);

               when others then
                  if get_stmts_rec.stage_type = 'ADD_TRIGGERS'
                  then    
                     vLogID := ut.log(const.k_subsys_subset,substr('Error: '||get_stmts_rec.stage_step_code ||': '|| dbms_utility.format_error_backtrace(),1,4000),SQLCODE,SQLERRM,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);
                     merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,v_tsEnd,const.k_FAILED,vLogID); 
                     ut.restore_session_to_run_env;                 
                  else
                     raise;
                  end if;

            end;
          end case;
        else
            v_tsStart := systimestamp;
            v_tsEnd := systimestamp;

            merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,v_tsEnd,const.k_DEP_IMCOMPLETE,null);

        end if;

      exception when others then

        v_tsEnd := systimestamp;

        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        vLogID := ut.log(const.k_subsys_subset,substr('Error: '||get_stmts_rec.stage_step_code ||': '|| dbms_utility.format_error_backtrace(),1,4000),v_code,v_errm,const_module,get_stmts_rec.stage_step_code,get_stmts_rec.stage_type);

        merge_ss_ctrl_exec_result(gp.ss_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,gp.ss_execution_id,v_tsStart,v_tsEnd,const.k_FAILED,vLogID);

        rollback;

      end;
    else  
      begin
         DBMS_SCHEDULER.STOP_JOB(job_name => const.k_SS_JOB_MONITOR, force => TRUE);
      exception
         when excep.x_job_not_running
         then
            DBMS_SCHEDULER.DROP_JOB(job_name => const.k_SS_JOB_MONITOR);
         when excep.x_unknown_job
         then                  
            null;      
         when others
         then
            v_code := SQLCODE;
            v_errm := SUBSTR(SQLERRM,1,4000);
            ut.log(const.k_subsys_subset,'Error stopping monitoring job',v_code,v_errm,const_module);         
      end;
      
      RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);
      
    end if;

    v_prev_stage_step_code := get_stmts_rec.stage_step_code;
  
  end loop;
  
  commit;

exception when others then
  v_code := SQLCODE;
  v_errm := SUBSTR(SQLERRM,1,4000);
  ut.log(const.k_subsys_subset,substr('Error: '||dbms_utility.format_error_backtrace(),1,4000),v_code,v_errm,const_module);
  raise;
end execute_ss_steps;


procedure create_schema (p_src_schema varchar2,p_dest_schema varchar2,p_job_name varchar2,p_log_file_dir varchar2,p_database_link varchar2,new_password varchar2 default null)
is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.create_schema';

    cursor cGetTables(p_src_schema varchar2) is
       select aggr_list from ss_part_table_aggr
        where actual_owner = p_src_schema order by aggr_list_seq;

  dphandle                NUMBER;
  logfileName             VARCHAR2(200);
  dest_schema_nameExists  NUMBER;
  valid_source_schema     NUMBER;

  job_state user_datapump_jobs.state%TYPE;
  status    ku$_Status;

  avalue clob;

  invalid_source_schema  EXCEPTION;
  dest_schema_exists     EXCEPTION;
  too_much_data_request  EXCEPTION;

  v_table_list           clob;
  v_Schema_List          varchar2(4000);
  v_sql                  varchar2(4000);
  v_code                 number;
  v_errm                 varchar2(4000);

BEGIN

  dphandle := 0;

  ut.log(const.k_subsys_subset,'open datapump job ' || 'job2_'||p_Job_Name||' over database link '||p_database_link,null,null,const_module);
  dphandle := dbms_datapump.open ( operation => 'IMPORT', job_mode => 'SCHEMA', remote_link => p_database_link, job_name => 'job2_'||p_Job_Name);

  ut.log(const.k_subsys_subset,'set logfile directory => '||p_log_file_dir,null,null,const_module);
  dbms_datapump.add_file ( handle => dphandle, filename => 'subset_dp_import_'||p_src_schema||'_'||p_Job_Name||'_'||TO_CHAR(SYSDATE,'YYYYMMDD')||'.log', directory => p_log_file_dir, filetype => dbms_datapump.ku$_file_type_log_file);

  DBMS_DATAPUMP.DATA_FILTER (handle   => dphandle,name     => 'INCLUDE_ROWS',VALUE    => 0);

  dbms_datapump.metadata_filter(dphandle,'EXCLUDE_PATH_EXPR','like ''%/TABLE/INDEX/STATISTICS/INDEX_STATISTICS''');
  dbms_datapump.metadata_filter(dphandle,'EXCLUDE_PATH_EXPR','like''%/TABLE/STATISTICS/TABLE_STATISTICS''');

  delete ss_part_table_aggr where actual_owner = p_src_schema;
  ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' ss_part_table_aggr rows for actual_owner '||p_src_schema,null,null,const_module);

  begin
     v_sql := 'insert into ss_part_table_aggr(SS_RUN_ID,owner,actual_owner,aggr_list_seq,aggr_list)

            SELECT  '||gp.get_ss_run_id||',res2.owner,res2.actual_owner,res2.col_group,listagg(''''''''||res2.table_name||'''''''', '','')
               WITHIN GROUP  (ORDER BY table_name) aggr_list
                  from
                     (
                        select res1.owner,res1.actual_owner,res1.table_name,trunc((res1.table_id - 1)/ (max(res1.table_id) over (partition by res1.owner) /20),0)  col_group
                          from
                             (
                                select owner,actual_owner,table_name,rownum table_id
                                  from
                                    (
                                       select replace(ss.src_schema,'||chr(39)||gp.src_prefix||chr(39)||'''_'','''') owner, ss.src_schema actual_owner,table_name
                                         from all_tables@SRC_LINK at
                                         join ss_schema_list ss on ss.src_schema = at.owner
                                        where ss.ss_run_id = gp.get_ss_run_id
                                          and at.owner = '''||p_src_schema||'''
                                       minus
                                       (  
                                          select distinct  replace(atp.table_owner,'||chr(39)||gp.src_prefix||chr(39)||'''_'','''') table_owner, atp.table_owner actual_table_owner, table_name 
                                            from all_tab_partitions@SRC_LINK atp
                                            join ss_schema_list ss on ss.src_schema = atp.table_owner
                                           where partition_name in ( select ''P_''||comp_code
                                                                       from '||gp.src_prefix||'_PRISM_CORE.'||'COMPANIES'||'@'||p_database_link||
                                                                    ' union select ''NULL_COMP_CODE'' from dual)
                                            and ss.ss_run_id = gp.get_ss_run_id
                                            and atp.table_owner = '''||p_src_schema||'''
                                          
                                            union
                                            select owner, actual_owner, iot_name 
                                              from src_dd_tables dt
                                             where iot_type = ''IOT_OVERFLOW''
                                        )
                                ) res
                             ) res1
                          ) res2 group by res2.owner,res2.actual_owner,res2.col_group ';

    ut.log(const.k_subsys_subset,'Executing v_sql: '||v_sql,null,null,const_module);
    execute immediate v_sql;
    
  exception
     when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,'Error executing v_sql: '||v_sql,v_code,v_errm,const_module);
  end;

  ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into ss_part_table_aggr using sql '||v_sql,null,null,const_module);

  for cGetTablesRec in  cGetTables(p_src_schema) loop

    if v_table_list is not null then
      v_table_list := v_table_list||',';
    end if;

    v_table_list := v_table_list||cGetTablesRec.aggr_list;

  end loop;

  If v_table_list is not null then
    dbms_datapump.metadata_filter(handle      => dphandle,
                                    name        => 'NAME_LIST',
                                    value       => v_table_list,
                                    object_type => 'TABLE');
  end if;

  status := dbms_datapump.get_status(handle => dphandle,
                                     mask   => dbms_datapump.ku$_status_job_error +
                                               dbms_datapump.ku$_status_job_status +
                                               dbms_datapump.ku$_status_wip,
                                     timeout => -1);

  job_state := status.job_status.state;
  ut.log(const.k_subsys_subset,'job_state = '||job_state,null,null,const_module);

  ut.log(const.k_subsys_subset,'SCHEMA_LIST defined as '||p_src_schema,null,null,const_module);
  dbms_datapump.metadata_filter( handle => dphandle, name => 'SCHEMA_LIST', value => ''''||p_src_schema||'''');

  ut.log(const.k_subsys_subset,'define REMAP_SCHEMA old_value => '||p_src_schema ||' value => '||p_dest_schema,null,null,const_module);
  dbms_datapump.metadata_remap(handle => dphandle, name => 'REMAP_SCHEMA', old_value => p_src_schema, value => p_dest_schema);

  --Set Parallelism for Export processing
  DBMS_DATAPUMP.SET_PARALLEL(handle => dphandle,  degree => 6);

  ut.log(const.k_subsys_subset,'start datapump job',null,null,const_module);
  dbms_datapump.start_job (dphandle);

  status := dbms_datapump.get_status(handle => dphandle,
                                     mask   => dbms_datapump.ku$_status_job_error +
                                               dbms_datapump.ku$_status_job_status +
                                               dbms_datapump.ku$_status_wip,
                                     timeout => -1);
  job_state := status.job_status.state;
  ut.log(const.k_subsys_subset,'job_state = '||job_state,null,null,const_module);

  dbms_datapump.wait_for_job(dphandle, job_state);
  ut.log(const.k_subsys_subset,'finish  job '||to_char(sysdate),null,null,const_module);

EXCEPTION

   WHEN OTHERS
   THEN
   
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
--          -- Tidy up
      if dphandle <> 0 then dbms_datapump.stop_job(dphandle); end if;
          
      -- e.g. ORA-31626: job does not exist from dphandle := dbms_datapump.open ( operation => 'IMPORT', job_mode => 'SCHEMA', remote_link => p_database_link, job_name => 'job2_'||p_Job_Name);
      RAISE;
      
END create_schema;

procedure load_ss_stages
as

  v_nLastStage number;

  const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_ss_stages';

begin

  v_nLastStage := 0;

  -- Generate Drop Schema Stages

  load_drop_schema_stmts;

  -- Generate Data Pump Import Schema Stages with Interleaved Comp Partitioned Table Metadata Stages (Datapump/dbms_metadata)

  load_dp_md_schema_create_stmts;

   --ADD_GRANTS	AG
  load_create_grants_stmts;

  -- MISSING_CONSTRAINTS  MC
  load_missing_constraint_stmts;

   --  CONVERT MD tables to partition tables PS
  load_md_part_cre_stmts;

     --  Load Synonyms for
  load_create_md_synonym_stmts;


 -- obsolete
 -- RERUN_TABLES  RT which FAILED
 -- load_rerun_tables;

  load_create_partition_stmts;

  --load_disable_enable_trg_stmts;

  --DISABLE_REF_CONS DC
  --ENABLE_REF_CONS EC
  load_disable_enable_r_cons_stmts;

  load_data_stmts;
  -- Generate Data Load Stages for TYPE_ALL tables  (Fast Load)

  -- Generate Data Load Satges for TYPE_SS_SUBSET	Comp_Pnt_YN = Y (Datapump)

  -- Generate Data Load Satges for TYPE_SS_SUBSET	Comp_Pnt_YN = N (Rule Based fast Load)

  --DISABLE_TRIGGERS	DT + ENABLE_TRIGGERS	ET
  load_disable_enable_trg_stmts;

  --ADD_TRIGGERS	AT
  load_create_trigger_stmts;

  --ADD_INDEXES	AI
  load_create_index_stmts;

  load_report_stmts;
  
  load_final_stages;

   commit;

   update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_ss_stages_loaded => 'Y');

end load_ss_stages;


function check_stages(p_ss_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2,p_stage_tab out string_list_257,p_msg_tab out string_list_4000) return varchar2 is

  cursor getViolations is
  select distinct owner,object_name,stage
  from
  (
      select owner,object_name,substr(stage_step_code,1,5) stage,
      count(distinct object_name) over (partition by substr(stage_step_code,1,5)) no_tables
      from ss_ctrl_stmts
  ) where no_tables > 1;

  i number;

  const_module      CONSTANT  varchar2(62) := 'subsetting_control.check_stages';
  v_code       number;
  v_errm       varchar2(4000);

  begin
    i:= 1;
    for getViolationsRec in getViolations loop
      p_stage_tab(i) := getViolationsRec.owner||'.'||getViolationsRec.object_name;
      p_msg_tab(i) :=   'Stage ('||getViolationsRec.stage ||') is in violation of integrity rule ('||const.k_SINGLE_ENTITY_STAGE_CHECK ||').';
      i:= i + 1;

    end loop;

    if i > 1 then
        return  const.k_Fail;
    else
        Return  const.k_Pass;
    end if;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          subsetting_control.update_ss_ctrl(p_ss_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_ss_status => 'FAILED');
          return  const.k_Fail;
  end check_stages;

  procedure execution_report  is

    const_module      CONSTANT  varchar2(62) := 'subsetting_control.execution_report';
    v_code       number;
    v_errm       varchar2(4000);

  begin

    delete from ss_execution_report where ss_run_id = gp.ss_run_id;
    ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_execution_report for ss_run_id: '||gp.ss_run_id,null,null,const_module);
     
    insert into ss_execution_report(ss_run_id,src_prefix,tgt_prefix,stage_type,stage_step_code,owner,table_name,stmt_seq,execution_id,start_timestamp,end_timestamp,duration_seconds,status,log_msg)

    select ctl.ss_run_id,ctl.src_prefix,ctl.tgt_prefix,sso.stage_type,scer.stage_step_code,scs.owner,scs.object_name,scer.stmt_seq,scer.execution_id ,
           scer.start_timestamp,scer.end_timestamp,scer.duration_seconds,scer.status,ssl.log_msg
    from ss_ctrl ctl
    left outer join ss_ctrl_stmts  scs  on ctl.ss_run_id = scs.ss_run_id 
    left outer join ss_stage_order sso  on scs.stage_type = sso.stage_type    
    left outer join ss_ctrl_exec_result scer on scs.stage_step_code = scer.stage_step_code and scs.ss_run_id = scer.ss_run_id and scs.stmt_seq = scer.stmt_seq
    left outer join ss_log ssl on ssl.log_id = scer.ss_log_id
    where (scer.ss_run_id,scer.stage_step_code, scer.execution_id,scer.stmt_seq) in
    (
      select ss_run_id,stage_step_code,max(execution_id) over (partition by stage_step_code) execution_id,stmt_seq
      from ss_ctrl_exec_result
      where ss_run_id = gp.ss_run_id
    )
    order by sso.stage_order,scs.stage_step_code,scer.stmt_seq;

    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into ss_execution_report for ss_run_id: '||gp.ss_run_id,null,null,const_module);

    commit;
    
    delete from ss_exec_summary where ss_run_id = gp.ss_run_id;
    ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_exec_summary for ss_run_id: '||gp.ss_run_id,null,null,const_module);
         
    
    insert into ss_exec_summary (SS_RUN_ID,SRC_PREFIX,TGT_PREFIX,STAGE_ORDER,STAGE_TYPE,STATUS,ERRM,RUN_TIME_SECONDS,RUN_TIME_MINUTES,COUNT,STAGE_DESC)
      select ctl.ss_run_id,ctl.src_prefix,ctl.tgt_prefix,
             sso.stage_order,
             (select stage_type from ss_stage_order where stage_order = sso.stage_order) stage_type,       
             ser.status,
             ssl.errm, 
             round(sum(duration_seconds)) run_time_seconds,       
             round(sum(duration_seconds)/60) run_time_minutes,
             count(*) count,
             (select stage_desc from ss_stage_order where stage_order = sso.stage_order)  stage_desc     
        from ss_ctrl ctl 
        left outer join (select stage_type, stage_desc, stage_code_alpha, stage_order, gp.ss_run_id as ss_run_id from ss_stage_order) sso  on  ctl.ss_run_id = sso.ss_run_id
        left outer join ss_ctrl_stmts  scs  on sso.stage_type = scs.stage_type and ctl.ss_run_id = scs.ss_run_id
        left outer join ss_ctrl_exec_result ser on ser.ss_run_id = scs.ss_run_id
                                               and ser.stage_step_code = scs.stage_step_code
                                               and ser.stmt_seq = scs.stmt_seq
        left outer join ss_log ssl on ssl.log_id = ser.ss_log_id and ssl.ss_run_id = ctl.ss_run_id
       where ctl.ss_run_id = gp.ss_run_id  
      group by ctl.ss_run_id,ctl.src_prefix,ctl.tgt_prefix,sso.stage_order,scs.stage_type,ser.status,ssl.errm
      order by sso.stage_order,ser.status;       

    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into ss_exec_summary for ss_run_id: '||gp.ss_run_id,null,null,const_module);

    commit;

  exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          subsetting_control.update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_ss_status => 'FAILED');
          rollback;
  end execution_report;


  procedure load_comp_list ( p_comp_list varchar2 ) is
     const_module      CONSTANT  varchar2(62) := 'subsetting_control.load_comp_list';

  begin

     delete from ss_companies where ss_run_id = gp.ss_run_id;
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_companies for ss_run_id: '||gp.ss_run_id,null,null,const_module);

     insert into ss_companies(ss_run_id,comp_code)
        select gp.get_ss_run_id,
               REGEXP_SUBSTR (p_comp_list,
                              '[^,]+',
                              1,
                              LEVEL) AS comp_name
          from dual
        connect by REGEXP_SUBSTR (p_comp_list,
                                  '[^,]+',
                                  1,
                                  LEVEL) IS NOT NULL;

      ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into ss_companies for ss_run_id: '||gp.ss_run_id,null,null,const_module);

     update ss_companies 
        set partition_name = 'P_'||comp_code
      where ss_run_id = gp.ss_run_id;
      
     insert into ss_companies(ss_run_id,comp_code,partition_name)
        select gp.get_ss_run_id,'NONE','NULL_COMP_CODE'
          from dual;      

     commit;

  end load_comp_list;


  procedure load_src_schema_list is
     const_module      CONSTANT  varchar2(62) := 'subsetting_control.load_src_schema_list';

  begin

        delete from ss_schema_list where ss_run_id = gp.ss_run_id;
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_schema_list for ss_run_id: '||gp.ss_run_id,null,null,const_module);

      insert into ss_schema_list(ss_run_id,src_schema,dest_schema,cre_seq)
      select gp.ss_run_id ss_run_id,sdu.username src_schema,replace(sdu.username,gp.src_prefix,gp.tgt_prefix) dest_schema,0 cre_seq
          from src_dd_users sdu
          where username like gp.src_prefix||'\_%' escape '\' ;
      ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into ss_schema_list for ss_run_id: '||gp.ss_run_id||' and src_prefix '||gp.src_prefix,null,null,const_module);

      merge into ss_schema_list ssl
      using (  select ss_run_id,src_schema, ROW_NUMBER() OVER (ORDER BY cnt desc) AS cre_seq
                from ( select gp.ss_run_id ss_run_id ,ac.actual_r_owner src_schema, count(*) cnt
                         from ss_schema_list ssl
                         join  src_dd_constraints ac on ac.actual_owner = ssl.src_schema and ac.constraint_type = 'R'
                        where ac.owner <> ac.r_owner
                        group by ac.actual_r_owner )
         ) res
         on (res.ss_run_id = ssl.ss_run_id and res.src_schema = ssl.src_schema)
          when matched
          then
              update set ssl.cre_seq = res.cre_seq;
      ut.log(const.k_subsys_subset,'updated '||sql%rowcount||' ss_schema_list rows by merge for ss_run_id: '||gp.ss_run_id||' to determine cre_seq by Foreign Key count',null,null,const_module);
  end load_src_schema_list;

  procedure create_src_syns is
     const_module      CONSTANT  varchar2(62) := 'subsetting_control.create_src_syns';
     v_sql varchar2(4000);
  begin

      delete from ss_synonym_ddl where ss_run_id = gp.ss_run_id;
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_synonym_ddl for ss_run_id: '||gp.ss_run_id,null,null,const_module);

      v_sql := 'insert into ss_synonym_ddl(ss_run_id,stmt)
              select '||gp.ss_run_id||' ss_run_id, ''create or replace synonym src_''||table_name||'' for '||gp.src_prefix||'_PRISM_UTILITIES.''||table_name||''@''||'''||gp.ss_db_link||''' stmt
      from '||gp.src_prefix||'_PRISM_UTILITIES'||'.dd_tables@'||gp.ss_db_link||
      ' where table_name like ''DD\_%'' escape ''\''
      or table_name like ''MD\_%'' escape ''\''
      or table_name like ''SS\_%'' escape ''\''';

      execute immediate v_sql;

      ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into ss_synonym_ddl for DD, MD and SS tables using sql: '||v_sql,null,null,const_module);

      commit;

  end create_src_syns;


 procedure load_environ_stmts is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_environ_stmts';

   v_nStep number;
   v_nOrder number;

   v_sStmt varchar2(32767);

 begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CREATE_SRC_SYN||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for ss_run_id: '||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CREATE_SRC_SYN||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for ss_run_id: '||gp.ss_run_id,null,null,const_module);

     delete from ss_src_objects_syns;
     ut.log(const.k_subsys_subset,'deleted all '||sql%rowcount||' rows from ss_src_objects_syns',null,null,const_module);


      --===================
      -- CREATE SYNONYMS TO tables in PRISM_UTILITIES
      --===================

--
      v_nStep := 1;
      v_nOrder := 1;


    v_sStmt := 'insert into ss_src_objects_syns(actual_owner,owner,object_name,object_type,object_group,synonym_name)
      select actual_owner,owner,table_name object_name ,''TABLE'' object_type,''A'' object_group ,''SRC_''||table_name
          from '||gp.src_prefix||'_PRISM_UTILITIES.dd_tables@'||gp.ss_db_link ||' where owner = ''PRISM_UTILITIES''
          and substr(table_name,1,3) in (''SS_'',''MD_'',''DD_'')';

    dbms_output.put_line(v_sStmt);
    execute immediate v_sStmt;
    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into into ss_src_objects_syns for SS, MD, DD tables using gp.ss_db_link: '||gp.ss_db_link,null,null,const_module);

    v_sStmt := 'insert into ss_src_objects_syns(actual_owner,owner,object_name,object_type,object_group,synonym_name)
    select actual_owner,owner,object_name object_name ,''TABLE'' object_type,''B'' object_group ,''SRC_''||object_name
      from '||gp.src_prefix||'_PRISM_UTILITIES.md_ddl@'||gp.ss_db_link ||' where view_name is not null and has_large_object = ''Y''
      and object_type = ''TABLE'' ';

    execute immediate v_sStmt;
    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into into ss_src_objects_syns for md_ddl src tables containing large objects using gp.ss_db_link: '||gp.ss_db_link,null,null,const_module);

    v_sStmt := 'insert into ss_src_objects_syns(actual_owner,owner,object_name,object_type,object_group,synonym_name)
    select '||chr(39)||gp.get_src_prefix||'_PRISM_UTILITIES'',''PRISM_UTILITIES'',VIEW_NAME object_name ,''VIEW'' object_type,''C'' object_group ,''SRC_''||view_name
    from '||gp.get_src_prefix||'_PRISM_UTILITIES.dd_views@'||gp.get_ss_db_link ||' where owner = ''PRISM_UTILITIES'' and view_name like''VW%'' ';

    dbms_output.put_line(v_sStmt);
    execute immediate v_sStmt;
    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into into ss_src_objects_syns for md_ddl src VIEWS using gp.ss_db_link: '||gp.ss_db_link,null,null,const_module);

    v_sStmt := 'insert into ss_src_objects_syns(actual_owner,owner,object_name,object_type,object_group,synonym_name)
       select actual_owner,owner,table_name object_name ,''TABLE'' object_type,''D'' object_group ,''SRC_''||table_name
         from dd_tables@'||gp.get_ss_db_link||'
        where substr(table_name,1,3) not in (''SS_'',''MD_'',''DD_'',''VW_'')
          and table_name not like''TEMP_%''
          and table_name not like''%_TEMP''
          and table_name not like''TEST_TABLE%''
          and ''SRC_''||table_name not in ( select synonym_name from ss_src_objects_syns )';

    dbms_output.put_line(v_sStmt);
    execute immediate v_sStmt;
    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into into ss_src_objects_syns for '||const.k_TYPE_ALL||' TABLES from ss_config',null,null,const_module);

    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_CREATE_SRC_SYN||LPAD(to_char(row_number() over (partition by 1 order by owner,object_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'D' step_type,
          'CREATE_SRC_SYN' stage_type,
          to_char(v_nOrder) as aorder ,
          owner,'SYNONYM',object_name,
          stmt
    from
    (
      select  owner, 'SRC_'||object_name  object_name,'CREATE OR REPLACE SYNONYM  SRC_'||object_name || ' for  '||actual_owner||'.'||object_name||'@'||gp.ss_db_link  stmt
      from ss_src_objects_syns
    );

    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into into ss_ctrl_stmts with CREATE OR REPLACE SYNONYM stmts generated from ss_src_objects_syns',null,null,const_module);

    commit;

    subsetting_control.update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_ss_syns_created => 'Y');

  end load_environ_stmts;


  procedure load_drop_schema_stmts is
     const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_drop_schema_stmts';

     v_nStep number;
     v_nOrder number;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DROP_REF_PART_TABS||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_DROP_USER,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DROP_REF_PART_TABS||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_DROP_USER,null,null,const_module);

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DROP_USER||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_DROP_USER,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DROP_USER||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_DROP_USER,null,null,const_module);

      --===================
      -- DROP SCHEMAS
      --===================
      -- Have to drop REFERENCE partitioned AUDIT tables first

      v_nStep := 1;
      v_nOrder := 1;

    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select ss_run_id,
           stage_step_code,
           dependent_ss_code,
           step_type,
           stage_type,
           aorder,
           owner,
           object_type,
           object_name,
           stmt
     from (
        select gp.get_ss_run_id as ss_run_id, 0 cre_seq, gp.get_tgt_prefix dest_schema,
              const.k_DROP_REF_PART_TABS||LPAD(to_char(1),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(row_number() over (partition by 1 order by 1),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
              null  dependent_ss_code,
              'P' step_type,
              'DROP_REF_PART_TABS' stage_type,
              to_char(v_nOrder) as aorder ,
              gp.get_tgt_prefix owner,'TABLES' object_type, null object_name,
              'ut.drop_ref_part_tabs_main' stmt
        from dual
        union
        select gp.get_ss_run_id as ss_run_id, cre_seq,dest_schema,
              const.k_DROP_USER||LPAD(to_char(row_number() over (partition by 1 order by cre_seq desc,dest_schema)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as  stage_step_code,
              null  dependent_ss_code,
              'D' step_type,
              'DROP_USER' stage_type,
              to_char(1) as aorder,
              dest_schema owner,'SCHEMA' object_type, dest_schema object_name,
              stmt
        from
        (
          select dest_schema,cre_seq,'DROP USER ' ||res.dest_schema ||' cascade' stmt
          from
          (
            select dest_schema,cre_seq
              from ss_schema_list sl
             where ss_run_id = gp.get_ss_run_id
          ) res
        )
        order by cre_seq,dest_schema
    )
    order by stage_step_code;

      ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' drop user statements into ss_ctrl_stmts for stage_step_code like '||const.k_DROP_USER,null,null,const_module);
    commit;
--    select substr(max(stage_step_code),2,const.k_STAGE_CODE_SIZE-1)
--    into v_nStage
--    from ss_ctrl_stmts
--    where ss_run_id = gp.ss_run_id and stage_step_code like 'D%';
--
--    p_LastStage := v_nStage;
end load_drop_schema_stmts;

procedure load_md_part_cre_stmts is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_md_part_cre_stmts';

   v_nStep number;
   v_nOrder number;


  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CONV_TO_PART||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_CONV_TO_PART||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CONV_TO_PART||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_CONV_TO_PART||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);


   --===================
   -- Step 1 Create Company partition tables
   --===================


    v_nOrder := 1;


      insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
      select gp.ss_run_id as ss_run_id,
          --const.k_CONV_TO_PART||LPAD(to_char(dense_rank() over (partition by 1 order by owner)),
          const.k_CONV_TO_PART||LPAD(to_char(1),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by 1 order by relational_level)),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'D' step_type,
          'CONV_TO_PART' stage_type,
          to_char(v_nOrder) as aorder ,
          owner ,'PARTITION',base_object_name object_name,
          object_ddl
         from md_ddl md
         where md.object_type = 'PARTITION';

      ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' ALTER TABLE to PART stmts into ss_ctrl_stmts from md_ddl (for partitioned tables)',null,null,const_module);
      commit;


  end load_md_part_cre_stmts;

procedure load_dp_md_schema_create_stmts is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_dp_md_schema_create_stmts';

   v_nStep number;
   v_nOrder number;

   cursor c_schemas_created
   is
     select ss_run_id, stage_step_code, owner
       from ss_ctrl_stmts
      where stage_type = 'CREATE_SCHEMA'
        and owner = object_name
        and ss_run_id = gp.get_ss_run_id;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CREATE_SCHEMA||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_CREATE_SCHEMA||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CREATE_SCHEMA||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_CREATE_SCHEMA||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

      --===================
      -- Step 1 Create Schemas using Data pump
      --===================

      v_nStep := 1;
      v_nOrder := 1;

    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_CREATE_SCHEMA||LPAD(to_char(row_number() over (partition by 1 order by cre_seq,dest_schema)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'CREATE_SCHEMA' stage_type,
          to_char(v_nOrder) as aorder ,
          dest_schema owner,'SCHEMA',dest_schema object_name,
          stmt
    from
    (
      select  dest_schema,cre_seq,'subsetting_control.create_schema ('''||src_schema||''','''||dest_schema||''','''||dest_schema||'_'||to_char(sysdate,'ddmmyyyyhh24miss')||''','''||gp.ss_log_file_dir||''','''||gp.ss_db_link||''','''||gp.ss_default_password||''')' stmt

      from
      (
        select src_schema,dest_schema,cre_seq
          from ss_schema_list pc
         where ss_run_id = gp.ss_run_id
      ) res1
    ) order by cre_seq,dest_schema;

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' CREATE SCHEMA stmts into ss_ctrl_stmts',null,null,const_module);
     commit;

--    select substr(max(stage_step_code),2,const.k_STAGE_CODE_SIZE-1)
--    into v_nStage
--    from ss_ctrl_stmts
--    where ss_run_id = gp.ss_run_id and stage_step_code like 'C%';


   --===================
   -- Step 2 Create Company partition tables
   --===================


    v_nStep := 2;
    v_nOrder := 1;

    for r in c_schemas_created
    loop

      insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
      select gp.ss_run_id as ss_run_id,
            substr(stage_step_code,1,length(stage_step_code)-const.k_STEP_CODE_SIZE)||LPAD(to_char(greatest(rownum+1,2)),const.k_STEP_CODE_SIZE,'0') as stage_step_code,
            stage_step_code  dependent_ss_code,
            'D' step_type,
            'CREATE_SCHEMA' stage_type,
            case when object_name is null then 0  else nvl(relational_level,-1) end  as aorder ,  --dt.cre_order
            replace (dest_schema,gp.tgt_prefix||'_','') owner,'TABLE',object_name,
            case when object_name is null then to_clob('null') else object_ddl end stmt
      from
      (
        select (select distinct ssl.dest_schema
                  from ss_schema_list ssl
                 where ssl.dest_schema = md.actual_owner
                   and ssl.dest_schema = r.owner) dest_schema
              , r.stage_step_code,md.object_name,md.object_ddl,md.relational_level --,dt.cre_order
          from md_ddl md 
         where md.object_type = 'TABLE'
           and md.dp_yn = 'N'  
           and md.owner =  replace(r.owner,gp.tgt_prefix||'_','')
      )
      order by relational_level;

      ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' CREATE TABLE stmts into ss_ctrl_stmts from md_ddl for schema '|| r.owner||'(for partitioned tables)',null,null,const_module);
      commit;

   end loop;

--
--    select substr(max(stage_step_code),2,const.k_STAGE_CODE_SIZE-1)
--    into v_nStage
--    from ss_ctrl_stmts
--    where ss_run_id = gp.ss_run_id and stage_step_code like 'C%';

  end load_dp_md_schema_create_stmts;

procedure load_src_metadata is
  const_module        CONSTANT  varchar2(62) := 'subsetting_control.load_src_metadata';
begin
  ut.log(const.k_subsys_subset,'deleting local OS copy of md_ddl_parts table',null,null,const_module);
  delete md_ddl_parts;
  ut.log(const.k_subsys_subset,'deleting local OS copy of md_ddl table',null,null,const_module);
  delete md_ddl;
  insert into md_ddl (md_ddl_id,
                      actual_owner,
                      owner,
                      object_type,
                      object_name,
                      base_object_name,
                      object_ddl,
                      object_ddl_length,
                      object_xml,
                      object_cre_seq,
                      relational_level,
                      partitioning_type,
                      subpartitioning_type,
                      ref_ptn_constraint_name,
                      view_name,
                      has_large_object,
                      created_ts,
                      modified_ts,
                      dp_yn)
               select md_ddl_id,
                      actual_owner,
                      owner,
                      object_type,
                      object_name,
                      base_object_name,
                      object_ddl,
                      object_ddl_length,
                      object_xml,
                      object_cre_seq,
                      relational_level,                      
                      partitioning_type,
                      subpartitioning_type,
                      ref_ptn_constraint_name,
                      view_name,
                      has_large_object,
                      created_ts,
                      modified_ts,
                      dp_yn
                 from src_md_ddl;
                 
  ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into md_ddl from src_md_ddl',null,null,const_module);
  
  insert into md_ddl_parts (part_seq_id,
                            md_ddl_id,
                            object_name,
                            object_ddl,
                            created_ts,
                            modified_ts)
                     select part_seq_id,
                            md_ddl_id,
                            object_name,
                            object_ddl,
                            created_ts,
                            modified_ts 
                       from src_md_ddl_parts;
                       
  ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into md_ddl_parts from src_md_ddl_parts',null,null,const_module);
  commit;

  if gp.src_prefix <> gp.tgt_prefix then
    ut.replace_md_ddl_actual_owner(gp.src_prefix,gp.tgt_prefix);
  end if;

  update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_src_metadata_loaded => 'Y');

end load_src_metadata;

procedure load_create_partition_stmts  is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_create_partition_stmts';

   v_nStep number;
   v_nOrder number;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_PARTITION||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_ADD_PARTITION||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_PARTITION||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_ADD_PARTITION||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

   --=========================================================
   -- Step 1 Create missing subset partitions for partition tables
   --=========================================================

    v_nStep := 1;
    v_nOrder := 1;


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
--          const.k_ADD_PARTITION||LPAD(to_char(dense_rank() over (partition by 1 order by cre_seq,dest_schema) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by dest_schema order by 1) +1),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,     
          const.k_ADD_PARTITION||LPAD(to_char(dense_rank() over (partition by 1 order by cre_seq,dest_schema,object_name) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by cre_seq,dest_schema,object_name order by 1) +1),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,       
          null  dependent_ss_code,
          'D' step_type,
          'ADD_PARTITION' stage_type,
          case when object_name is null then 0  else nvl(relational_level,-1) end  as aorder ,
          replace (dest_schema,gp.tgt_prefix||'_','') owner,'TABLE_PARTITION', object_name,
          case when object_name is null then null
            else 'ALTER TABLE ' || dest_schema||'.'||object_name || ' ADD PARTITION "'||partition_name||'" VALUES ('||chr(39)||comp_code||chr(39)||')' end stmt
    from
    (
      select sl.src_schema,sl.dest_schema,sc.comp_code,sc.partition_name,sl.cre_seq,md.object_name,md.object_ddl,md.relational_level
        from ss_schema_list sl join ss_companies sc on sl.ss_run_id = sc.ss_run_id
                               join md_ddl md on md.actual_owner = sl.dest_schema
       where sl.ss_run_id = gp.ss_run_id
         and object_type = 'TABLE'
         and partitioning_type = 'LIST'
         and sc.partition_name <> 'NULL_COMP_CODE'
    );

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' ALTER TABLE ADD PARTITION stmts into ss_ctrl_stmts (for tables with NULL partition only)',null,null,const_module);

     commit;

  end load_create_partition_stmts;


procedure load_disable_enable_r_cons_stmts  is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_disable_enable_r_cons_stmts';

   v_nStep number;
   v_nOrder number;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DISABLE_REF_CONS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_DISABLE_REF_CONS||' and gp.ss_run_id ='||gp.get_ss_run_id,null,null,const_module);

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ENABLE_REF_CONS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_ENABLE_REF_CONS||' and gp.ss_run_id ='||gp.get_ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DISABLE_REF_CONS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_DISABLE_REF_CONS||' and gp.ss_run_id ='||gp.get_ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ENABLE_REF_CONS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_ENABLE_REF_CONS||' and gp.ss_run_id ='||gp.get_ss_run_id,null,null,const_module);

   --=========================================================
   -- Step 1 Create enable and disable ref constraint stmts
   --=========================================================

    v_nStep := 1;
    v_nOrder := 1;


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_DISABLE_REF_CONS||LPAD(to_char(dense_rank() over (partition by 1 order by 1) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by 1 order by 1)),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'P' step_type,
          'DISABLE_REF_CONS' stage_type,
          to_char(v_nOrder) as aorder ,
          'ALL' owner,
          'ALL_REF_CONSTRAINTS' object_name,
          'ut.disable_r_cons('||chr(39)||const.k_subsys_subset||chr(39)||')' stmt
     from dual;

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' DISABLE_REF_CONS stmts into ss_ctrl_stmts',null,null,const_module);


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_ENABLE_REF_CONS||LPAD(to_char(dense_rank() over (partition by 1 order by 1) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by 1 order by 1)),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'P' step_type,
          'ENABLE_REF_CONS' stage_type,
          to_char(v_nOrder) as aorder ,
          'ALL' owner,
          'ALL_REF_CONSTRAINTS' object_name,
          'ut.enable_r_cons('||chr(39)||const.k_subsys_subset||chr(39)||')' stmt
     from dual;

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' ENABLE_REF_CONS stmts into ss_ctrl_stmts',null,null,const_module);

     commit;

  end load_disable_enable_r_cons_stmts;


  procedure load_create_trigger_stmts  is
     const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_create_trigger_stmts';

     v_nStep number;
     v_nOrder number;
      
     v_code      number;
     v_errm      varchar2(4000);   

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_TRIGGERS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_ADD_TRIGGERS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_TRIGGERS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_ADD_TRIGGERS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

   --=========================================================
   -- Step 1 Create missing triggers for partition tables
   --=========================================================
    
    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_ADD_TRIGGERS||LPAD(to_char(dense_rank() over (partition by 1 order by cre_seq,dest_schema) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by dest_schema order by 1) +1),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'D' step_type,
          'ADD_TRIGGERS' stage_type,
          1  as aorder ,
          replace (dest_schema,gp.tgt_prefix||'_','') owner,'TRIGGER',object_name,
          object_ddl stmt
    from
    (
      select sl.src_schema,sl.dest_schema,sl.cre_seq,md.object_name,md.object_ddl,md.object_cre_seq
        from ss_schema_list sl join md_ddl md on md.actual_owner = sl.dest_schema
       where sl.ss_run_id = gp.ss_run_id
         and object_type = 'TRIGGER'
    );

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' ADD_TRIGGERS stmts into ss_ctrl_stmts',null,null,const_module);

-- Below not needed (would not work even if changed as seperate iteration in execute_ss_Steps for each stmt_seq)
-- special case processing introduced in execute_ss_steps for 'ADD_TRIGGERS' to switch current user to compile trigger and switch back immediately after
--     begin
--        insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
--        select gp.get_ss_run_id as ss_run_id,
--              const.k_ADD_TRIGGERS||LPAD(to_char(stage_code),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD('1',const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
--              null  dependent_ss_code,
--              'D' step_type,
--              'ADD_TRIGGERS' stage_type,
--              1 as aorder,
--              owner,'TRIGGER',null,
--              'alter session set current_schema='||dest_schema stmt
--         
--        from (    
--          select distinct to_number(substr(stage_step_code,const.k_STAGE_CODE_ALPHA_SIZE +1,const.k_STAGE_CODE_SIZE)) stage_code, owner, gp.get_tgt_prefix||'_'||owner dest_schema
--            from ss_ctrl_stmts 
--           where stage_type = 'ADD_TRIGGERS' 
--          union
--          select  to_number(max(substr(stage_step_code,const.k_STAGE_CODE_ALPHA_SIZE +1,const.k_STAGE_CODE_SIZE))+1) stage_code,  gp.get_run_env owner, gp.get_run_env dest_schema
--            from ss_ctrl_stmts 
--           where stage_type = 'ADD_TRIGGERS'      
--          order by 1    
--        );
--    
--         ut.log(const.k_subsys_subset,'Added a further '||sql%rowcount||' ADD_TRIGGERS stmts into ss_ctrl_stmts for alter session set current_schema=',null,null,const_module);
--  
--     exception
--        when others then
--           ut.log(const.k_subsys_subset,substr('Unexpected Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),null,null,const_module);
--           ut.log(const.k_subsys_subset,'Resetting current_schema to '||gp.get_run_env,null,null,const_module);
--           execute immediate 'alter session set current_schema='||gp.get_run_env;
--   end;  

     commit;

  exception
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Unexpected Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      rollback;  

  end load_create_trigger_stmts;


procedure load_create_grants_stmts  is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_create_grants_stmts';

   v_nStep number;
   v_nOrder number;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_GRANTS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_ADD_GRANTS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_GRANTS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_ADD_GRANTS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

   --=========================================================
   -- Step 1 Create missing grants for partition tables
   --=========================================================

    v_nStep := 1;
    v_nOrder := 1;


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_ADD_GRANTS||LPAD(to_char(dense_rank() over (partition by 1 order by cre_seq,dest_schema) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by dest_schema order by 1) +1),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'D' step_type,
          'ADD_GRANTS' stage_type,
          case when object_name is null then 0  else nvl(object_cre_seq,0) end  as aorder ,
          replace (dest_schema,gp.tgt_prefix||'_','') owner,'OBJECT_GRANT',object_name,
          object_ddl stmt
    from
    (
      select sl.src_schema,sl.dest_schema,sl.cre_seq,md.object_name,md.object_ddl,md.object_cre_seq
        from ss_schema_list sl join md_ddl md on md.actual_owner = sl.dest_schema
       where sl.ss_run_id = gp.ss_run_id
         and object_type = 'OBJECT_GRANT'
    );

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' OBJECT_GRANT stmts into ss_ctrl_stmts',null,null,const_module);

     commit;

  end load_create_grants_stmts;


procedure load_create_index_stmts  is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_create_index_stmts';

   v_nStep number;
   v_nOrder number;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_INDEXES||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_ADD_INDEXES||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_INDEXES||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_ADD_INDEXES||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

   --=========================================================
   -- Step 1 Create missing indexes for partition tables
   --=========================================================

    v_nStep := 1;
    v_nOrder := 1;


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select gp.get_ss_run_id as ss_run_id,
          const.k_ADD_INDEXES||LPAD(to_char(dense_rank() over (partition by 1 order by cre_seq,dest_schema) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by dest_schema order by 1) +1),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'D' step_type,
          'ADD_INDEXES' stage_type,
          case when object_name is null then 0  else nvl(object_cre_seq,0) end  as aorder ,
          replace (dest_schema,gp.tgt_prefix||'_','') owner,'INDEX',object_name,
          object_ddl stmt
    from
    (
      select sl.src_schema,sl.dest_schema,sl.cre_seq,md.object_name,md.object_ddl,md.object_cre_seq
        from ss_schema_list sl join md_ddl md on md.actual_owner = sl.dest_schema
       where sl.ss_run_id = gp.ss_run_id
         and object_type = 'INDEX'
    );

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' INDEX stmts into ss_ctrl_stmts',null,null,const_module);

     commit;

  end load_create_index_stmts;


procedure load_missing_constraint_stmts  is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_missing_constraint_stmts';

   v_nStep number;
   v_nOrder number;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_MISSING_CONSTRAINTS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_MISSING_CONSTRAINTS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_MISSING_CONSTRAINTS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_MISSING_CONSTRAINTS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

    -- dd_constraints must be refreshed
    ut.load_dd_constraints(const.k_subsys_subset);

   --=========================================================
   -- Step 1 Create missing constraints
   --=========================================================

    v_nStep := 1;
    v_nOrder := 1;


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select gp.get_ss_run_id as ss_run_id,
          const.k_MISSING_CONSTRAINTS||LPAD(to_char(dense_rank() over (partition by 1 order by x.cre_seq,x.dest_schema) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by x.dest_schema order by 1) +1),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'D' step_type,
          'MISSING_CONSTRAINTS' stage_type,
          case when x.object_name is null then 0  else nvl(x.object_cre_seq,0) end  as aorder ,
          replace (x.dest_schema,gp.get_tgt_prefix||'_','') owner,'REF_CONSTRAINT',x.object_name,
          x.object_ddl stmt
    from
    (
      select sl.src_schema,sl.dest_schema,sl.cre_seq,mdp.object_name,mdp.object_ddl,md.object_cre_seq
        from ss_schema_list sl
        join md_ddl md on nvl(md.actual_owner,gp.get_tgt_prefix||'_'||md.owner) = sl.dest_schema
        join md_ddl_parts mdp on mdp.md_ddl_id = md.md_ddl_id
       where sl.ss_run_id = gp.get_ss_run_id
         and md.object_type = 'REF_CONSTRAINT'
         and exists ( select 1  -- Only for partitioned tables
                        from md_ddl t
                       where t.object_name = md.base_object_name
                         and t.object_type = 'TABLE' )
    ) x
    where not exists (select 1 from dd_constraints y
                       where x.dest_schema = y.actual_owner
                         and y.constraint_name = x.object_name);

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' MISSING_CONSTRAINTS stmts into ss_ctrl_stmts',null,null,const_module);

     commit;

  end load_missing_constraint_stmts;


procedure load_disable_enable_trg_stmts  is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_disable_enable_trg_stmts';

   v_nStep number;
   v_nOrder number;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DISABLE_TRIGGERS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_DISABLE_TRIGGERS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DISABLE_TRIGGERS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_DISABLE_TRIGGERS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ENABLE_TRIGGERS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_ENABLE_TRIGGERS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ENABLE_TRIGGERS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_ENABLE_TRIGGERS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

   --=========================================================
   -- Step 1 load disable and enable trigger stmts
   --=========================================================

    v_nStep := 1;
    v_nOrder := 1;


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_DISABLE_TRIGGERS||LPAD(to_char(dense_rank() over (partition by 1 order by 1) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by 1 order by 1)),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'P' step_type,
          'DISABLE_TRIGGERS' stage_type,
          to_char(v_nOrder) as aorder ,
          --dest_schema owner,dest_schema object_name,
          'ALL' owner,
          'ALL_TRIGGERS' object_name,
          'ut.disable_triggers('||chr(39)||const.k_subsys_subset||chr(39)||')' stmt
     from dual;

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' DISABLE_TRIGGERS stmts into ss_ctrl_stmts',null,null,const_module);


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_ENABLE_TRIGGERS||LPAD(to_char(dense_rank() over (partition by 1 order by 1) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by 1 order by 1)),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'P' step_type,
          'ENABLE_TRIGGERS' stage_type,
          to_char(v_nOrder) as aorder ,
          --dest_schema owner,dest_schema object_name,
          'ALL' owner,
          'ALL_TRIGGERS' object_name,
          'ut.enable_triggers('||chr(39)||const.k_subsys_subset||chr(39)||')' stmt
     from dual;

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' ENABLE_TRIGGERS stmts into ss_ctrl_stmts',null,null,const_module);

     commit;

  end load_disable_enable_trg_stmts;



  procedure load_data_stmts is
    const_module        CONSTANT  varchar2(62) := 'subsetting_control.load_data_stmts';
    v_nStep number;
    v_nOrder number;
  begin
    load_cons_load_rules;
    load_c_load_rules;  
    load_star_load_rules;
    load_full_table_data;
    load_ss_partition_data;
    load_rule_based_data_stmts;
  end load_data_stmts;


procedure load_full_table_data is

   const_module        CONSTANT  varchar2(62) := 'subsetting_control.load_full_table_data';

   v_sql       varchar2(4000);
   v_code      number;
   v_errm      varchar2(4000);
   v_nStep     number;
   v_nOrder    number;

begin

    v_nStep  := 1;
    v_nOrder := 1;

   begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_LOAD_DATA_ALL||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for TYPE_ALL DATA',null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_LOAD_DATA_ALL||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for TYPE_ALL DATA',null,null,const_module);

      insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
      select gp.ss_run_id as ss_run_id,
            const.k_LOAD_DATA_ALL||LPAD(to_char(dense_rank() over (partition by 1 order by owner) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by owner order by owner) +1)-1,const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
            null  dependent_ss_code,
            'S' step_type,
            'LOAD_DATA_ALL' stage_type,
            to_char(v_nOrder) as aorder,
            owner,'TABLE',table_name,
            stmt
      from
      (
        select ssc.ss_order, ssc.owner, ssc.table_name, 
         case when srs.full_sql is null then
                'insert into '||gp.tgt_prefix||'_'|| ssc.owner||'.'||ssc.table_name || ' select * from SRC_'||ssc.table_name 
              else
                srs.full_sql end stmt
          from ss_config ssc
          join src_dd_tables mdd on mdd.owner = ssc.owner and mdd.table_name = ssc.table_name
          left outer join ss_rules_sql srs on srs.owner = ssc.owner and srs.table_name = ssc.table_name
          left outer join ss_load_excl_config slec  on slec.owner = ssc.owner and slec.table_name = ssc.table_name 
         where ssc.ss_type = const.k_TYPE_ALL 
           and slec.table_name is null
         order by ssc.owner,ssc.ss_order
      );

      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records inserted into ss_ctrl_stmts for TYPE_ALL',null,null,const_module);

   exception
      when others then
         rollback;
         v_code := SQLCODE;
         v_errm := SUBSTR(SQLERRM,1,4000);
         ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
   end;

   commit;

  end load_full_table_data;
  

  procedure load_ss_partition_data is

    const_module        CONSTANT  varchar2(62) := 'subsetting_control.load_ss_partition_data';
    v_code          number;
    v_errm          varchar2(4000);
    
    v_nStep         number;
    v_nOrder        number;
    v_LS_job_count  number;
    v_sql           varchar2(4000);
    

    cursor c_has_large_object_to_merge
    is
      select ssc.owner, ssc.table_name, mdd.view_name, scs.stage_step_code
        from ss_config ssc
        join md_ddl mdd        on mdd.owner = ssc.owner and mdd.object_name = ssc.table_name
        join ss_ctrl_stmts scs on scs.owner = ssc.owner and scs.object_name = ssc.table_name
       where ssc.ss_type = const.k_TYPE_SS_SUBSET
         and ssc.partitioned_yn = 'Y'
         and mdd.has_large_object = 'Y'
         and scs.stage_step_code like const.k_LOAD_DATA_SUBSET||'%';


    cursor c_subset_load_tabs
    is
      select stage_step_code, owner, object_name
        from ss_ctrl_stmts
       where stage_type = 'LOAD_DATA_SUBSET' 
         and ss_run_id = gp.get_ss_run_id
      order by stage_step_code;
      
    v_dep_stage_step_code  ss_ctrl_stmts.dependent_ss_code%type;   

  begin

    v_nStep  := 1;
    v_nOrder := 1;

    begin

      delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_LOAD_DATA_SUBSET||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for TYPE_SS_SUBSET partitioned DATA',null,null,const_module);

      delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_LOAD_DATA_SUBSET||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for TYPE_SS_SUBSET partitioned DATA',null,null,const_module);

      insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
       select gp.get_ss_run_id     ss_run_id,
             -- 'LS0001S0000'        stage_step_code,
              const.k_LOAD_DATA_SUBSET||LPAD(to_char(1),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(0),const.k_STEP_CODE_SIZE,'0')   as stage_step_code,
              NULL                 dependent_ss_code,
              'S'                  step_type,
              'LOAD_DATA_SUBSET'   stage_type,
              1                    stmt_seq,
              'PRISM_CORE'         owner,
              'TABLE'              object_type,
              'COMPANIES'          object_name,
              'insert into '||gp.get_tgt_prefix||'_'||'PRISM_CORE.COMPANIES select * from SRC_COMPANIES where comp_code in ( select comp_code from SS_COMPANIES )' stmt
          from dual;
          
      ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_ctrl_stmts for COMPANIES (non-partitioned)',null,null,const_module);          

      insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
       select ss_run_id,
              stage_step_code,
              dependent_ss_code,
              step_type,
              stage_type,
              stmt_seq,
              owner,
              object_type,
              table_name,
              stmt               
         from (
                select gp.get_ss_run_id as ss_run_id,
                      const.k_LOAD_DATA_SUBSET||LPAD(to_char(1),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(dense_rank() over (partition by 1 order by relational_level nulls last,object_cre_seq desc)),const.k_STEP_CODE_SIZE,'0')   as stage_step_code ,    
                      null  as dependent_ss_code,
                      case when parallel_load = 'N' then 'S' when parallel_load = 'Y' then 'J' end step_type,
                      'LOAD_DATA_SUBSET' stage_type,
                      to_char(row_number() over (partition by table_name order by 1)) as stmt_seq,
                      owner,'TABLE' object_type,table_name,parallel_load,
                      
                      case when rule_load = 'Y' then
                           full_sql
                           when parallel_load = 'N' then
                              'insert into '||gp.get_tgt_prefix||'_'||owner||'.'||table_name|| ' select * from '|| 'SRC_'||view_name
                           when parallel_load = 'Y' then
                            'insert into '||gp.get_tgt_prefix||'_'||x.owner||'.'||x.table_name||' partition('||x.partition_name||')'||
                            ' select * from '|| 'SRC_'||x.view_name||'_'||x.partition_name 
                      end  stmt         
                from
                (
                  select ssc.owner, ssc.table_name, nvl(upper(pl.parallel_load),'N') parallel_load,
                  case when srs.table_name is null then 'N' else 'Y' end rule_load,mdd.view_name,
                         mdd.object_cre_seq,mdd.relational_level,tp.partition_name,srs.full_sql
                    from ss_config ssc              
                    join md_ddl mdd on mdd.owner = ssc.owner and mdd.object_name = ssc.table_name
                    join dba_synonyms syn on '_'||ssc.table_name = ltrim(syn.table_name,'VW')
                    left outer join ss_rules_sql srs on srs.owner = ssc.owner and srs.table_name = ssc.table_name 
                    left outer join ( select ssplc.owner,ssplc.table_name,ssplc.parallel_load 
                                      from  src_ss_parallel_load_config ssplc
                                      left outer join ss_rules_sql srs1  on srs1.owner = ssplc.owner and srs1.table_name = ssplc.table_name  
                                      where upper(parallel_load) = 'Y' and srs1.table_name is null) pl 
                                      on mdd.owner = pl.owner and mdd.object_name = pl.table_name
                    left outer join ( select dtp.table_owner, dtp.table_name, dtp.partition_name
                                        from src_dd_tab_partitions dtp 
                                        join ss_companies co on co.ss_run_id = gp.get_ss_run_id and dtp.partition_name = co.partition_name ) tp
                                 on tp.table_owner = pl.owner and tp.table_name = pl.table_name
                    left outer join ss_load_excl_config slec  on slec.owner = ssc.owner and slec.table_name = ssc.table_name                                  
                    --left outer join ss_schema_load_excl_config slec  on slec.owner = ssc.owner  
                   where ssc.ss_type = const.k_TYPE_SS_SUBSET 
                     and slec.owner is null   
                     and slec.table_name is null                     
                     and ssc.partitioned_yn = 'Y'
                     and '_'||upper(mdd.view_name) = ltrim(syn.synonym_name,'SRC')
                     and ssc.run_id = gp.get_ss_run_id           
                     and exists ( select 1
                                    from src_dd_tab_partitions dtp
                                    join ss_companies co on dtp.partition_name = co.partition_name
                                   where dtp.table_owner = mdd.owner
                                     and dtp.table_name = ssc.table_name
                                     and co.ss_run_id = gp.get_ss_run_id
                     )
                   order by ssc.owner, mdd.relational_level nulls last, mdd.object_cre_seq desc, ssc.ss_order
                ) x
             );

      ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_ctrl_stmts for TYPE_SS_SUBSET partitioned data',null,null,const_module);

      commit;

      for r in c_has_large_object_to_merge
      loop
         subsetting_control.merge_large_object_partition_data(r.owner,r.table_name,r.view_name,r.stage_step_code);
      end loop;
      
      begin
        select count(*)
          into v_LS_job_count
          from ss_ctrl_stmts
         where step_type = 'J'
           and stage_type = 'LOAD_DATA_SUBSET';
      exception
         when no_data_found then
            v_LS_job_count := 0;
      end;     
      
      --if parallel jobs then add stage to wait for completion
      if v_LS_job_count > 0
      then
         
         insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)     
            select gp.get_ss_run_id as ss_run_id,
                   ( select max(stage_step_code) from ss_ctrl_stmts where stage_type = 'LOAD_DATA_SUBSET' )  stage_step_code,
                   null  dependent_ss_code,
                   'P' step_type,
                   'LOAD_DATA_SUBSET' stage_type,
                   ( select max(stmt_seq)+1 
                       from ss_ctrl_stmts 
                      where stage_step_code = ( select max(stage_step_code) 
                                                  from ss_ctrl_stmts
                                                 where stage_type = 'LOAD_DATA_SUBSET' )) as aorder,
                   'OS1' owner, 
                   null object_type,
                   null object_name,  
                   'ut.wait_for_ss_jobs('||chr(39)||'SS_LOAD_'||chr(39)||')' stmt
              from dual;
  
         commit;
         
      end if;
      
      --finally set dependent_ss_code
      ut.log(const.k_subsys_subset,'Updating dependent_ss_code and populate temp_src_dd_constraints from src_dd_constraints',null,null,const_module);      
      
      delete temp_src_dd_constraints;
      insert into temp_src_dd_constraints select * from src_dd_constraints;
      commit;
      
      for r in c_subset_load_tabs
      loop
         v_dep_stage_step_code := subsetting_control.fn_get_dep_ref_part_stage_step_code(r.owner, r.object_name);   
         update ss_ctrl_stmts set dependent_ss_code = v_dep_stage_step_code 
          where stage_step_code = r.stage_step_code
            and ss_run_id = gp.get_ss_run_id;
      end loop;      
      commit;
      
    exception
       when others then
          rollback;
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

 end load_ss_partition_data;


  procedure merge_large_object_partition_data(p_owner varchar2, p_table_name varchar2,  p_view_name varchar2, p_stage_step_code varchar2)
  is

    const_module      CONSTANT  varchar2(62) := 'subsetting_control.merge_large_object_partition_data';

    v_code                   number;
    v_errm                   varchar2(4000);
    v_nStep                  number;
    v_nOrder                 number;
    v_merge_cols             varchar2(4000);
    v_join_col_list          varchar2(4000);
    v_match_col_str          varchar2(4000);
    v_match_on               varchar2(4000);
    v_stmt                   varchar2(4000);
    v_max_step_code          number;

    cursor c_pK (cp_owner varchar2, cp_table_name varchar2)
    is
      select listagg(dcc.column_name,',') within group ( order by dcc.position) 
        from src_dd_constraints dco 
        join src_dd_cons_columns dcc on dco.actual_owner    = dcc.actual_owner
                                    and dco.table_name      = dcc.table_name
                                    and dco.constraint_name = dcc.constraint_name                              
       where dco.owner = cp_owner
         and dco.table_name = cp_table_name
         and dco.constraint_type = 'P';  

    cursor c_uK (cp_owner varchar2, cp_table_name varchar2)
    is
      select listagg(dcc.column_name,',') within group ( order by dcc.position) 
        from src_dd_constraints dco 
        join src_dd_cons_columns dcc on dco.actual_owner    = dcc.actual_owner
                                    and dco.table_name      = dcc.table_name
                                    and dco.constraint_name = dcc.constraint_name                              
       where dco.owner = cp_owner
         and dco.table_name = cp_table_name
         and dco.constraint_type = 'U';
         
  begin

    --stmt_seq; merge after insert

    begin

       select listagg(column_name,',') within group ( order by column_name) as join_col_list
         into v_join_col_list
         from ( select dtc.column_name
                  from src_dd_tab_columns dtc
                 where owner = 'PRISM_UTILITIES' -- VIEWS ARE IN PRISM_UTILITIES schema
                   and table_name = 'VW_'||p_table_name ); -- e.g'VW_FR858_PROCESS_CONTROL')

       ut.log(const.k_subsys_subset,'Column_list for view '||'VW_'||p_table_name||'is '||v_join_col_list,null,null,const_module);

       v_stmt := 'insert into '||gp.get_tgt_prefix||'_'||p_owner||'.'||p_table_name||'('||v_join_col_list||')'||
                 ' select '||v_join_col_list||' from '|| 'SRC_'||p_view_name;

       ut.log(const.k_subsys_subset,'Built corrected column list stmt for insert: '||v_stmt,null,null,const_module);

       update ss_ctrl_stmts
          set stmt = v_stmt
        where stage_step_code = p_stage_step_code;

       ut.log(const.k_subsys_subset,'Updated (corrected) insert stmt for stage_step_code '||p_stage_step_code||' to exclude LOB columns which will be merged: '||sql%rowcount||' row updated',null,null,const_module);

        select LISTAGG(' tgt.'||regexp_substr(column_names, '[^,]+', 1, level)||' = src.'||regexp_substr(column_names, '[^,]+', 1, level),' ,') within group (order by 1)
         into v_merge_cols
         from ( select dtc.column_name  column_names
                  from src_dd_tab_columns dtc
                 where owner = p_owner
                   and table_name = p_table_name -- 'FR858_PROCESS_CONTROL'
                 minus
                select dtc.column_name  column_names
                  from src_dd_tab_columns dtc
                 where owner = 'PRISM_UTILITIES' -- VIEWS ARE IN PRISM_UTILITIES schema
                   and table_name = 'VW_'||p_table_name )--'VW_FR858_PROCESS_CONTROL')
       connect by level <= length(regexp_replace(column_names, '[^,]+')) + 1;

       ut.log(const.k_subsys_subset,'Retrieved merge columns '||v_merge_cols,null,null,const_module);

      -- First try to match on pK
       open c_pK(p_owner,p_table_name);
       fetch c_pK into v_match_col_str;
       close c_pK;
       
      -- Alternatively try uK 
       if v_match_col_str is null
       then
          open c_uK(p_owner,p_table_name);
          fetch c_uK into v_match_col_str;
          close c_uK;  
       end if;

       if v_match_col_str is not null
       then       
       
           ut.log(const.k_subsys_subset,'Built match col str expression for merge: '||v_match_col_str,null,null,const_module);
           
           select LISTAGG(' src.'||regexp_substr(v_match_col_str, '[^,]+', 1, level)||' = tgt.'||regexp_substr(v_match_col_str, '[^,]+', 1, level),' and ') within group (order by 1)
             into v_match_on
             from dual
           connect by level <= length(regexp_replace(v_match_col_str, '[^,]+')) + 1;    
           
       else -- If no pK or uK found then match on all columns (except LOB(s) for update)
         select listagg(col_expr,' and ') within group ( order by column_id) as match_col_str
           into v_match_on
           from ( 
                select case when dtc2.nullable = 'Y'
                             then 'nvl(src.'||dtc2.column_name||','|| case when dtc2.data_type = 'VARCHAR2'
                                                                         then chr(39)||'x'||chr(39)||')'
                                                                         when dtc2.data_type = 'NUMBER'
                                                                         then 0||')'
                                                                         when dtc2.data_type = 'DATE'
                                                                         then chr(39)||trunc(sysdate)||chr(39)||')'                                                                         
                                                                         when dtc2.data_type like 'TIMESTAMP%'
                                                                         then chr(39)||trunc(sysdate)||chr(39)||')'                                                                          
                                                                         else -1||')'
                                                                      end || '='
                                ||'nvl(tgt.'||dtc2.column_name||','|| case when dtc2.data_type = 'VARCHAR2'
                                                                         then chr(39)||'x'||chr(39)||')'
                                                                         when dtc2.data_type = 'NUMBER'
                                                                         then 0||')'
                                                                         when dtc2.data_type = 'DATE'
                                                                         then chr(39)||trunc(sysdate)||chr(39)||')'                                                                         
                                                                         when dtc2.data_type like 'TIMESTAMP%'
                                                                         then chr(39)||trunc(sysdate)||chr(39)||')'                                                                         
                                                                         else -1||')'
                                                                      end                                                                   
                        else 'src.'||dtc2.column_name||'='||'tgt.'||dtc2.column_name
                        end AS col_expr,
                        dtc2.column_id
                   from src_dd_tab_columns dtc1 
                   join src_dd_tab_columns dtc2  on dtc2.table_name = replace(dtc1.table_name,'VW_','') 
                                                and dtc2.column_name = dtc1.column_name
                  where dtc1.owner = 'PRISM_UTILITIES' -- VIEWS ARE IN PRISM_UTILITIES schema
                    and dtc1.table_name = 'VW_'||p_table_name ); -- e.g 'FR858_PROCESS_CONTROL')
       end if;

       v_match_on := rtrim(v_match_on,' and ');        
       ut.log(const.k_subsys_subset,'Built match on expression for merge: '||v_match_on,null,null,const_module);

       v_stmt := 'merge into '||gp.tgt_prefix||'_'||p_owner||'.'||p_table_name||' tgt
                  using (select *
                           from SRC_'||p_table_name||' ) src
                     on ('||v_match_on||')
                   when matched then update set '||v_merge_cols;

       ut.log(const.k_subsys_subset,'Built merge statement: '||v_stmt,null,null,const_module);

       insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
         select gp.ss_run_id as ss_run_id,
                p_stage_step_code  stage_step_code,
                null  dependent_ss_code,
                'S' step_type,
                'LOAD_DATA_SUBSET' stage_type,
               ( select max(stmt_seq)+1 
                   from ss_ctrl_stmts 
                  where stage_step_code = p_stage_step_code ) as aorder,  --  after insert
                p_owner,'TABLE',p_table_name,
                v_stmt
           from dual;

       ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' merge records inserted into ss_ctrl_stmts for '||p_owner||'.'||p_table_name,null,null,const_module);

     exception
        when others then
           rollback;
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM,1,4000);
           ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     end;

     commit;

  end merge_large_object_partition_data;
  
  
procedure load_report_stmts is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_report_stmts';

   v_nStep number;
   v_nOrder number;
  
  begin
 
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_SCHEMA_REPORTING||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for SCHEMA_REPORTING and gp.ss_run_id = '||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_SCHEMA_REPORTING||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for SCHEMA_REPORTING',null,null,const_module);
       
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DATA_REPORTING||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for DATA_REPORTING and gp.ss_run_id = '||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_DATA_REPORTING||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for DATA_REPORTING',null,null,const_module);
 
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_1||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for CHECK_POINT_1',null,null,const_module);
     
     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_1||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for CHECK_POINT_1',null,null,const_module);

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_2||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for CHECK_POINT_2',null,null,const_module);
     
     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_2||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for CHECK_POINT_2',null,null,const_module);
     
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_3||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for CHECK_POINT_3',null,null,const_module);
     
     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_3||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for CHECK_POINT_3',null,null,const_module);
     
      delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_4||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for CHECK_POINT_4',null,null,const_module);
     
     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_4||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for CHECK_POINT_4',null,null,const_module);
     
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_5||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for CHECK_POINT_5',null,null,const_module);
     
     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_5||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for CHECK_POINT_5',null,null,const_module);
     
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_6||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for CHECK_POINT_6',null,null,const_module);
     
     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_6||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for CHECK_POINT_6',null,null,const_module);
     
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_7||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for CHECK_POINT_7',null,null,const_module);
     
     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_7||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for CHECK_POINT_7',null,null,const_module);
 
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_8||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for CHECK_POINT_8',null,null,const_module);
     
     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_CHECK_POINT_8||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for CHECK_POINT_8',null,null,const_module); 
          
   --=========================================================
   -- Step 1 SCHEMA REPORTING Create Report Calls
   --=========================================================

   v_nStep := 1;
   v_nOrder := 1;

   insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_SCHEMA_REPORTING||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'SCHEMA_REPORTING' stage_type,
          to_char(v_nOrder) as aorder,
          'ALL' owner,
          null object_name,
          stmt
    from
    (
      select 1 seq, 'ss_reporting.gen_obj_count_diff_report' stmt 
      from dual
      union
      select 2 seq, 'ss_reporting.gen_missing_object_report' stmt
      from dual
      union
      select 3 seq, 'ss_reporting.gen_tab_col_diff_report' stmt
      from dual
      union
      select 4 seq, 'ss_reporting.gen_missing_constraints_report' stmt
      from dual
      union
      select 5 seq, 'ss_reporting.missing_privs_report' stmt
      from dual         
      union
      select 6 seq, 'ss_reporting.gen_missing_table_report' stmt 
      from dual  
      union
      select 7 seq, 'ss_reporting.gen_missing_syn_report' stmt 
      from dual     
    ) order by seq;  
   
     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' SS Reporting procedure call stmts into ss_ctrl_stmts for schema reporting',null,null,const_module);

 
   --=========================================================
   -- Step 1 DATA REPORTING Create Report Calls
   --=========================================================

   v_nStep := 1;
   v_nOrder := 1;

   insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_DATA_REPORTING||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'DATA_REPORTING' stage_type,
          to_char(v_nOrder) as aorder,
          'ALL' owner,
          null object_name,
          stmt
    from
    (   
      select 1 seq, 'ss_reporting.gen_ptn_count_diff_report' stmt
      from dual      
      union
      select 2 seq, 'ss_reporting.gen_load_all_count_diff_report' stmt
      from dual
      union
      select 3 seq, 'ss_reporting.gen_load_other_report' stmt
      from dual  
      union
      select 4 seq, 'ss_reporting.gen_missing_indexes_report' stmt
      from dual       
    ) order by seq;  
   
     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' SS Reporting procedure call stmts into ss_ctrl_stmts for data reporting',null,null,const_module);

 --=================================================================================
   -- CHECK POINT 1 (C1) Reporting/Fixing of missing or invalid table/grant/synonyms 
   --===============================================================================

   v_nOrder := 1;

   insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_CHECK_POINT_1||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'CHECK_POINT_1' stage_type,
          to_char(v_nOrder) as aorder,
          'ALL' owner,
          null object_name,
          stmt
    from
    (
      select 1 seq, 'ss_reporting.missing_privs_report' stmt from dual 
      union
      select 2 seq, 'ss_reporting.gen_missing_table_report' stmt from dual  
      union
      select 3 seq, 'ss_reporting.gen_missing_syn_report' stmt from dual  
      union
      select 4 seq, 'subsetting_control.fix_missing_tables' stmt from dual  
      union
      select 5 seq, 'subsetting_control.fix_missing_privs' stmt from dual 
      union      
      select 6 seq, 'subsetting_control.fix_missing_syns' stmt from dual      
    ) order by seq;  
   
     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' C1 Check-point  stmts into ss_ctrl_stmts for data reporting/fixing',null,null,const_module);


 --=========================================================
   -- CHECK POINT 2 (C2) Reporting/Fixing of missing ref constraints
   --=========================================================

   v_nOrder := 1;

   insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_CHECK_POINT_2||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'CHECK_POINT_2' stage_type,
          to_char(v_nOrder) as aorder,
          'ALL' owner,
          null object_name,
          stmt
    from
    (
      
      select 1 seq, 'ss_reporting.gen_missing_constraints_report' stmt from dual  
      union
      select 2 seq, 'subsetting_control.fix_missing_ref_cons' stmt from dual   
    ) order by seq;  
   
     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' C2 Check-point  stmts into ss_ctrl_stmts for data reporting/fixing',null,null,const_module);


    --=========================================================
   -- CHECK POINT 3 (C3) Reporting/Fixing of missing partitions
   --=========================================================

   v_nOrder := 1;

   insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_CHECK_POINT_3||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'CHECK_POINT_3' stage_type,
          to_char(v_nOrder) as aorder,
          'ALL' owner,
          null object_name,
          stmt
    from
    (
      select 1 seq, 'gp.set_fix_missing_part_attempts(0)' stmt from dual       
      union
      select 2 seq, 'subsetting_control.fix_missing_parts' stmt from dual 
      union      
      select 3 seq, 'ss_reporting.gen_missing_parts_report' stmt from dual        
    ) order by seq;  
   
    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' C3 Check-point  stmts into ss_ctrl_stmts for data reporting/fixing',null,null,const_module);		 
  
    --=========================================================
   -- CHECK POINT 5 (C5) Reload of failed SUBSET load
   --=========================================================

   v_nOrder := 1;

   insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_CHECK_POINT_5||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'CHECK_POINT_5' stage_type,
          to_char(v_nOrder) as aorder,
          'ALL' owner,
          null object_name,
          stmt
    from
    (
      select 1 seq, 'gp.set_subset_reload_attempts(0)' stmt from dual       
      union
      select 2 seq, 'subsetting_control.fix_subset_load' stmt from dual      
      union
      select 3 seq, 'ut.wait_for_ss_jobs('||chr(39)||'SS_LOAD_'||chr(39)||')'  from dual where const.k_max_parallel_jobs > 1 -- if Parallel execution enabled
    ) order by seq;  
   
    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' C5 Check-point  stmts into ss_ctrl_stmts for subset data reload',null,null,const_module);		 
    commit;    

    --==========================================================================
   -- CHECK POINT 6 (C6) Reload failed RULE data loads
   --==========================================================================
   null;
    --==================================================================================================================================================
   -- CHECK POINT 7 (C7)  Generate missing objects report and create any missing triggers and indexes (after partitioning, add_triggers, add_index stages)  
   --==================================================================================================================================================

   v_nOrder := 1;

   insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_CHECK_POINT_7||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'CHECK_POINT_7' stage_type,
          to_char(v_nOrder) as aorder,
          'ALL' owner,
          null object_name,
          stmt
    from
    (
      select 1 seq, 'ss_reporting.gen_missing_object_report' stmt from dual 
      union
      select 2 seq, 'subsetting_control.fix_missing_triggers' stmt from dual 
      union
      select 3 seq, 'subsetting_control.fix_missing_indexes' stmt from dual      
    ) order by seq;    

    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' C7 Check-point  stmts into ss_ctrl_stmts for missing triggers and indexes',null,null,const_module);		 
    commit;    
   
       
       --==========================================================================
   -- CHECK POINT 8 (C8)  fix invalid foreign keys
   --==========================================================================

   v_nOrder := 1;

   insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_CHECK_POINT_8||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'P' step_type,
          'CHECK_POINT_8' stage_type,
          to_char(v_nOrder) as aorder,
          'ALL' owner,
          null object_name,
          stmt
    from
    (
      select 1 seq, 'subsetting_control.get_inv_fk_from_log' stmt from dual   
      union
      select 2 seq, 'subsetting_control.fix_inv_fk'   stmt from dual     
    ) order by seq;  
   
    ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' C8 Check-point  stmts into ss_ctrl_stmts for fixing invalid foreign keys',null,null,const_module);		 
    commit;    

  end load_report_stmts;


  procedure load_final_stages is
     const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_final_stages';

     v_nStep number;
     v_nOrder number;
  
  begin
  
     null;
    -- Final stage for execution report, miscellaneous patches etc, although this will still come before manually running ut.recompile at end
 
     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_FINAL_STAGES||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for FINAL_STAGES and gp.ss_run_id = '||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_FINAL_STAGES||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for FINAL_STAGES',null,null,const_module);
 
     v_nOrder := 1;

     insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,stmt)
       select gp.ss_run_id as ss_run_id,
              const.k_FINAL_STAGES||LPAD(to_char(row_number() over (partition by 1 order by seq)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
              null  dependent_ss_code,
              'P' step_type,
             'FINAL' stage_type,
             to_char(v_nOrder) as aorder,
             'ALL' owner,
             null object_name,
             stmt
        from  
       (
         select 1 seq, 'subsetting_control.execution_report' stmt from dual 
         union
         select 2 seq, 'ut.recompile('||chr(39)||const.k_subsys_subset||chr(39)||')' stmt from dual                
       ) order by seq;    

     commit;
 
   end load_final_stages;      

--  k_TYPE_SS_SUBSET constant varchar2(20) := 'TYPE_SS_SUBSET';
--  k_TYPE_SS_NONE   constant varchar2(20) := 'TYPE_SS_NONE';
--  k_TYPE_ALL       constant varchar2(20) := 'TYPE_ALL';

  procedure load_subset_config is
      const_module        CONSTANT  varchar2(62) := 'subsetting_control.load_subset_config';
  begin

    delete from ss_config;
    ut.log(const.k_subsys_subset,'deleted all '||to_char(sql%rowcount)|| ' records from ss_config',null,null,const_module);


    insert into ss_config(run_id,ss_order,owner,table_name,ss_type,partitioned_yn )
    select gp.ss_run_id run_id,0 ss_order, owner,table_name,const.k_TYPE_SS_NONE ss_type,'N'
      from src_dd_tables;
    ut.log(const.k_subsys_subset,'inserted '||to_char(sql%rowcount)|| ' records into ss_config (ss_type '||const.k_TYPE_SS_NONE||')',null,null,const_module);

    -- Patch Test tables

    merge into ss_config sc
    using (select gp.ss_run_id run_id ,owner,table_name,const.k_TYPE_ALL ss_type
           from ss_pt_tabs_populated ) res
         on (sc.run_id = res.run_id and sc.owner = res.owner and sc.table_name = res.table_name)
          when matched
          then update set sc.ss_type = res.ss_type;
    ut.log(const.k_subsys_subset,'updated '||to_char(sql%rowcount)|| ' ss_config records by merge from ss_pt_tabs_populated for ss_type '||const.k_TYPE_ALL,null,null,const_module);

    commit;

    -- Company patitioned tables

    merge into ss_config sc
    using (select distinct gp.ss_run_id run_id,owner,object_name table_name,const.k_TYPE_SS_SUBSET ss_type 
             from src_md_ddl
            where object_type = 'TABLE'
              and partitioning_type is not null
              and dp_yn = 'N') res
         on (sc.run_id = res.run_id and sc.owner = res.owner and sc.table_name = res.table_name)
          when matched
          then update set sc.ss_type = res.ss_type,partitioned_yn = 'Y' ;
    ut.log(const.k_subsys_subset,'updated '||to_char(sql%rowcount)|| ' ss_config records by merge from src_md_ddl for partitioned data of ss_type '||const.k_TYPE_SS_SUBSET,null,null,const_module);

    commit;

    -- Rule based
    merge into ss_config sc
    using (select distinct gp.ss_run_id run_id,owner,table_name,partitioned_yn,const.k_TYPE_SS_SUBSET ss_type from ss_rule_tables
           ) res
         on (sc.run_id = res.run_id and sc.owner = res.owner and sc.table_name = res.table_name)
          when matched
          then update set sc.ss_type = res.ss_type,sc.partitioned_yn = res.partitioned_yn  ;
    ut.log(const.k_subsys_subset,'updated '||to_char(sql%rowcount)|| ' ss_config records by merge from ss_rule_tables for rule based (non-partitioned) data of ss_type '||const.k_TYPE_SS_SUBSET,null,null,const_module);

    -- Any tables that still designated as k_TYPE_SS_NONE to be redesignated as k_TYPE_SS_ALL 

    update ss_config set ss_type = const.k_TYPE_ALL where ss_type = const.k_TYPE_SS_NONE;


--    merge into ss_config sc
--    using ( select owner,table_name,ss_rule
--              from ss_rules_config ) rc
--       on (sc.run_id = gp.ss_run_id and sc.owner = rc.owner and sc.table_name = rc.table_name)
--     when matched
--     then update set sc.ss_rule = rc.ss_rule ;
     
    ut.log(const.k_subsys_subset,'updated '||to_char(sql%rowcount)|| ' ss_config records by merge from ss_rules_config for ss_rule',null,null,const_module);


    commit;



    update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_ss_config_loaded => 'Y');

  end load_subset_config;

  procedure load_create_md_synonym_stmts  is
   const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_create_md_synonym_stmts';

   v_nStep number;
   v_nOrder number;

  begin

     delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_SYNONYMS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for stage_step_code like '||const.k_ADD_SYNONYMS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

     delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_ADD_SYNONYMS||'%';
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for stage_step_code like '||const.k_ADD_SYNONYMS||' and gp.ss_run_id ='||gp.ss_run_id,null,null,const_module);

   --=========================================================
   -- Step 1 Create synonyms partition tables
   --=========================================================

    v_nStep := 1;
    v_nOrder := 1;


    insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_type,object_name,stmt)
    select gp.ss_run_id as ss_run_id,
          const.k_ADD_SYNONYMS||LPAD(to_char(dense_rank() over (order by dest_schema) ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(row_number() over (partition by dest_schema order by 1)),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
          null  dependent_ss_code,
          'D' step_type,
          'ADD_SYNONYMS' stage_type,
          1  aorder ,
          replace (dest_schema,gp.tgt_prefix||'_','') owner,'SYNONYM',synonym_name,
          stmt
    from
    (
       select md.actual_owner dest_schema, synonym_name,'CREATE OR REPLACE SYNONYM '||replace(sds.actual_owner,gp.src_prefix,gp.tgt_prefix)||'.'||sds.synonym_name||' FOR '||replace(sds.table_owner,gp.src_prefix,gp.tgt_prefix)||'.'||table_name stmt
         from md_ddl md
         join src_dd_synonyms sds on replace(sds.table_owner,gp.src_prefix||'_',null) = md.owner and sds.table_name = md.object_name
        where md.object_type = 'TABLE'
    );

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' ADD_SYNONYMS stmts into ss_ctrl_stmts',null,null,const_module);

     commit;

  end load_create_md_synonym_stmts;
  
  
  procedure fix_missing_syns is
    
    cursor cGetSyns is select sms.owner,sms.synonym_name,'CREATE OR REPLACE SYNONYM '||gp.tgt_prefix||'_'||sms.owner||'.'||sms.synonym_name||' FOR '||sms.table_owner||'.'||sms.table_name stmt  
                              from ss_missing_synonyms sms ;
    v_cStmt clob;
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.fix_missing_syns';
  
  begin

    for cGetSynsRec in cGetSyns loop
       begin
          ut.log(const.k_subsys_subset,'Executing: '|| cGetSynsRec.stmt,null,null,const_module);      
          execute immediate cGetSynsRec.stmt;
      
       exception when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr('Execute Fail: '|| cGetSynsRec.stmt,1,4000),v_code,v_errm,const_module);
 
       end;
    end loop;
    
  exception when others then
    commit;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
       
  end fix_missing_syns;


  procedure fix_missing_privs is
    
    cursor cGetPrivs is 
       select  smp.owner, smp.table_name, smp.grantee, smp.grantor, smp.privilege,
              'GRANT '||smp.privilege||' ON '||smp.grantor||'.'||smp.table_name||' TO '||smp.grantee ||
               case grantable
                 when 'YES' then ' WITH GRANT OPTION'
               end
            AS stmt
         from ss_missing_privs smp;

    v_cStmt clob;
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.fix_missing_privs';
  
  begin
 
    for cGetPrivsRec in cGetPrivs loop
      begin
        ut.log(const.k_subsys_subset,'Executing: '||cGetPrivsRec.stmt,null,null,const_module);
        execute immediate cGetPrivsRec.stmt;

      exception when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,substr('Execute Fail: '|| cGetPrivsRec.stmt,1,4000),v_code,v_errm,const_module);
      end;
    end loop;
    
  exception when others then
      commit;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  end fix_missing_privs;
  
  
  procedure fix_missing_triggers is
  
     const_module  CONSTANT  varchar2(62) := 'subsetting_control.fix_missing_triggers';
  
     cursor c_missing_triggers
     is
       select gp.get_tgt_prefix||'_'||owner actual_owner, object_name 
         from ss_missing_objects mo
        where object_type = 'TRIGGER'
          and object_name not like'SYS%' 
          and object_name not like'%_TMP';
                              
     v_code          number;
     v_errm          varchar2(4000);
      
     v_job_name      varchar2(32);
     v_job_action    varchar2(4000);    
     
     v_stmt          clob;
     v_md_ddl_id     number;
    
  begin  
  
     for r in c_missing_triggers
     loop
  
        begin  
           v_job_name   := substr('POP_MISSING_TRIGGER_DDL_'||r.object_name,1,32); 
            
           v_job_action := 'BEGIN
                              gen_metadata.pop_trigger_ddl('||chr(39)||r.actual_owner||chr(39)||','||chr(39)||r.object_name||chr(39)||');' ||
                          ' END;';
            
           ut.run_remote_md_utilities_job( v_job_name,v_job_action,null,null );   
       
           v_md_ddl_id := ut.ins_md_ddl(r.actual_owner,'TRIGGER',r.object_name);   
          
           select object_ddl into v_stmt from md_ddl where md_ddl_id = v_md_ddl_id;
           ut.log(const.k_subsys_subset,substr('Executing v_stmt: '|| to_char(v_stmt),1,4000),null,null,const_module);       
           execute immediate v_stmt;
       
        exception when others then    
          rollback;
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        end;
       
     end loop; 
     
  exception when others then    
    rollback;
    v_code := SQLCODE;
    v_errm := SUBSTR(SQLERRM,1,4000);
    ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
         
  end fix_missing_triggers;


  procedure fix_missing_indexes is
  
     const_module  CONSTANT  varchar2(62) := 'subsetting_control.fix_missing_indexes';
                       
     cursor c_missing_indexes
     is
       select gp.get_tgt_prefix||'_'||owner actual_owner, object_name 
         from ss_missing_objects mo
        where object_type = 'INDEX'
          and object_name not like'SYS%' 
          and object_name not like'%TMP%';    

     v_code          number;
     v_errm          varchar2(4000);
      
     v_job_name      varchar2(32);
     v_job_action    varchar2(4000);    
  
     v_stmt          clob;
     v_md_ddl_id     number;  
                             
  begin       
  
    for r in c_missing_indexes
    loop
  
      begin
         v_job_name   := substr('POP_MISSING_INDEX_DDL_'||r.object_name,1,32); 
            
         v_job_action := 'BEGIN
                            gen_metadata.pop_index_ddl('||chr(39)||r.actual_owner||chr(39)||','||chr(39)||r.object_name||chr(39)||');' ||
                        ' END;';
            
         ut.log(const.k_subsys_subset,'Executing remote job: '|| v_job_name||'; job action: ' ||v_job_action,null,null,const_module);                   
         ut.run_remote_md_utilities_job( v_job_name,v_job_action,null,null );   
      
         v_md_ddl_id := ut.ins_md_ddl(r.actual_owner,'INDEX',r.object_name);   
          
         select object_ddl into v_stmt from md_ddl where md_ddl_id = v_md_ddl_id;
         ut.log(const.k_subsys_subset,substr('Executing v_stmt: '|| to_char(v_stmt),1,4000),null,null,const_module);       
         execute immediate v_stmt;
      
      exception 
         when no_data_found then    
            ut.log(const.k_subsys_subset,'No DDL found for INDEX '||r.actual_owner||'.'||r.object_name,SQLCODE,SQLERRM,const_module);     
         when others then    
            v_code := SQLCODE;
            v_errm := SUBSTR(SQLERRM,1,4000);
            ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      end;
     
    end loop; 
    
  exception when others then    
    rollback;
    v_code := SQLCODE;
    v_errm := SUBSTR(SQLERRM,1,4000);
    ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  
  end fix_missing_indexes;

  
  procedure fix_missing_tables is
    
    cursor cGetTabs is 
      select mt.owner,mt.table_name,'TABLE' atype,table_ddl 
        from ss_missing_tables mt
        left outer join dd_tables dt on mt.owner = dt.owner and mt.table_name = dt.table_name
       where dt.table_name is null
      order by obj_cre_seq asc;    
    
    v_cStmt clob;

    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.fix_missing_tables';
  
  begin
       
    for i in 1..const.k_max_fix_missing_tables_retries
    loop
     
       ut.log(const.k_subsys_subset,'Calling resolve_ref_seq',null,null,const_module);
       resolve_ref_seq;
    
       ut.log(const.k_subsys_subset,'Starting iteration '||i||' of running ddl for any remaining missing tables',null,null,const_module);
       
       for cGetTabsRec in cGetTabs loop
     
          if cGetTabsRec.table_ddl is null or cGetTabsRec.table_ddl = 'UNRESOLVED'
          then
            ut.log(const.k_subsys_subset,substr('Cannot create: Table '||cGetTabsRec.owner||'.'||cGetTabsRec.table_name||' has no corresponding ddl',1,4000),null,null,const_module);
          
          else
               begin
      
                  execute immediate cGetTabsRec.table_ddl;
                  ut.log(const.k_subsys_subset,substr('Missing Table: '|| cGetTabsRec.owner||'.'||cGetTabsRec.table_name||' created.',1,4000),null,null,const_module);
                                                                
                  exception when others then  
                  v_code := SQLCODE;
                  v_errm := SUBSTR(SQLERRM,1,4000);
                  ut.log(const.k_subsys_subset,substr('Execute Fail: '|| cGetTabsRec.table_ddl,1,4000),v_code,v_errm,const_module);																																   
              end;
          end if;
        
       end loop;
       
       ss_reporting.gen_missing_table_report;
    end loop;   
    
  exception when others then
    rollback;
    v_code := SQLCODE;
    v_errm := SUBSTR(SQLERRM,1,4000);
    ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);

  end fix_missing_tables;


  procedure fix_missing_ref_cons is

    cursor cGetCons is 
       select smc.owner,smc.table_name,smc.constraint_name,mdp.object_ddl 
         from ss_missing_constraints smc
         left outer join  md_ddl md on md.owner = smc.owner and  md.base_object_name = smc.table_name and object_type = 'REF_CONSTRAINT'
         left outer join  md_ddl_parts mdp on mdp.object_name =  smc.constraint_name
        where smc.constraint_type = 'R';

    v_cStmt clob;
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.fix_missing_ref_cons';
  
  begin
    
    for cGetConsRec in cGetCons loop
   
        if cGetConsRec.object_ddl is null or cGetConsRec.object_ddl = 'UNRESOLVED'
        then
          ut.log(const.k_subsys_subset,substr('Cannot create: Constraint '||cGetConsRec.owner||'.'||cGetConsRec.table_name||':'||cGetConsRec.constraint_name ||' has no corresponding ddl',1,4000),null,null,const_module);
        
        else
           begin
    
             execute immediate cGetConsRec.object_ddl;
             ut.log(const.k_subsys_subset,substr('Missing Ref Constraint : '|| cGetConsRec.owner||'.'||cGetConsRec.table_name||':'||cGetConsRec.constraint_name||' created.',1,4000),null,null,const_module);
																  													  
           exception 
             when excep.x_ref_cons_already_exists then  -- CHECK_POINT_2 'subsetting_control.fix_missing_ref_cons'ORA-02275: such a referential constraint already exists in the table
               ut.log(const.k_subsys_subset,'Ref constraint already exists: '|| cGetConsRec.owner||'.'||cGetConsRec.table_name||':'||cGetConsRec.constraint_name,SQLCODE,SQLERRM,const_module);
             when others then  
               v_code := SQLCODE;
               v_errm := SUBSTR(SQLERRM,1,4000);
               ut.log(const.k_subsys_subset,substr('Execute Fail: '|| cGetConsRec.object_ddl,1,4000),v_code,v_errm,const_module);																																   
           end;
        end if;
      
    end loop;
    
  exception when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);

  end fix_missing_ref_cons;
  
  
  procedure fix_missing_parts is

      const_module  CONSTANT  varchar2(62) := 'subsetting_control.fix_missing_parts';

      v_code        number;
      v_errm        varchar2(4000);
      
      cursor c_missing_parts
      is
        select s.stage_step_code--,r.ss_run_id,r.execution_id --s.*, r.*,l.*
          from ss_ctrl_exec_result r 
          join ss_ctrl_stmts s on r.stage_step_code = s.stage_step_code and r.ss_run_id = s.ss_run_id
          join ss_log l on r.ss_log_id = l.log_id
         where s.stage_type = 'CONV_TO_PART' 
           and r.status <> const.k_FAILED
           and r.ss_run_id = gp.get_ss_run_id
           and r.execution_id = gp.get_ss_execution_id
           and l.err_code = '-14653';
          -- and l.errm = 'ORA-14653: parent table of a reference-partitioned table must be partitioned';      
      
      v_stage_step_code  ss_ctrl_stmts.stage_step_code%type;
      v_attempts         number;
      
    begin

      ut.log(const.k_subsys_subset,'Processing c_missing_parts cursor to re-execute CONV_TO_PART stage_step_codes which failed due to ORA-14653: parent table of a reference-partitioned table must be partitioned',null,null,const_module);	
      v_attempts := gp.get_fix_missing_part_attempts + 1;
      gp.set_fix_missing_part_attempts(v_attempts);
      
      if c_missing_parts%isopen then
         close c_missing_parts;
      end if;
 
      open c_missing_parts;
            
      loop
         fetch c_missing_parts into v_stage_step_code;
         exit when c_missing_parts%notfound;
          
         execute_ss_steps(v_stage_step_code, v_stage_step_code,'N',gp.ss_run_id,gp.ss_execution_id,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version);

      end loop;   
      
      if c_missing_parts%isopen then
         close c_missing_parts;
      end if;
      
      ut.log(const.k_subsys_subset,'Checking to see if failed CONV_TO_PART stage_step_codes were re-executed successfully.  If not call fix_missing_parts again (recursively).',null,null,const_module);	
      
      if gp.get_fix_missing_part_attempts < const.k_max_fix_missing_part_retries
      then
      
        open c_missing_parts;
        fetch c_missing_parts into v_stage_step_code;
        close c_missing_parts;
        
        if v_stage_step_code is not null 
        then
           subsetting_control.fix_missing_parts;
        end if;  
      
      end if;
      
    exception when others then
      if c_missing_parts%isopen then
         close c_missing_parts;
      end if;

      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);

      rollback;

    end fix_missing_parts;							 

   procedure fix_subset_load is

      const_module  CONSTANT  varchar2(62) := 'subsetting_control.fix_subset_load';

      v_code        number;
      v_errm        varchar2(4000);
      
      cursor c_subset_reload
      is
        select s.stage_step_code, s.step_type, s.stmt_seq
          from ss_ctrl_exec_result r 
          join ss_ctrl_stmts s on r.stage_step_code = s.stage_step_code and r.stmt_seq = s.stmt_seq and r.ss_run_id = s.ss_run_id
          left outer join ss_log l on r.ss_log_id = l.log_id
         where s.stage_type = 'LOAD_DATA_SUBSET' 
           and r.status <> const.k_COMPLETED
           and r.ss_run_id = gp.get_ss_run_id
           and r.execution_id = gp.get_ss_execution_id
        order by stage_step_code,stmt_seq;
           
           --and l.err_code = -2291;
           --and l.errm like'%ORA-02291%';
           --ORA-02291: integrity constraint violated - parent key not found
 
      v_attempts         number;
      v_stage_step_code  ss_ctrl_stmts.stage_step_code%type;      
      v_step_type        ss_ctrl_stmts.step_type%type;
      v_stmt_seq         ss_ctrl_stmts.stmt_seq%type;
      
   begin

      ut.log(const.k_subsys_subset,'Processing c_subset_reload cursor to re-execute failed LOAD_DATA_SUBSET stage_step_codes',null,null,const_module);	
      v_attempts := gp.get_subset_reload_attempts + 1;
      gp.set_subset_reload_attempts(v_attempts);
      ut.log(const.k_subsys_subset,'subset_reload_attempt number '||gp.get_subset_reload_attempts,null,null,const_module);
      
      if c_subset_reload%isopen then
         close c_subset_reload;
      end if;
 
      open c_subset_reload;
            
      loop
         fetch c_subset_reload into v_stage_step_code, v_step_type, v_stmt_seq;
         if c_subset_reload%notfound
         then
            ut.log(const.k_subsys_subset,'c_subset_reload%notfound',null,null,const_module);	
            exit;
            --exit when c_subset_reload%notfound;
         end if;
          
         ut.log(const.k_subsys_subset,'Calling execute_ss_steps with stage_step_code '   ||v_stage_step_code ||
                                                                   ' stmt_seq '          ||v_stmt_seq        ||         
                                                                   ' gp.ss_run_id '      ||gp.ss_run_id      ||
                                                                   ' gp.ss_execution_id '||gp.ss_execution_id||
                                                                   ' gp.src_prefix '     ||gp.src_prefix     ||
                                                                   ' gp.tgt_prefix '     ||gp.tgt_prefix     ||
                                                                   ' gp.run_env    '     ||gp.run_env        ||
                                                                   ' gp.anon_version '   ||gp.anon_version,null,null,const_module,v_stage_step_code,'LOAD_DATA_SUBSET');	 
         execute_ss_steps(v_stage_step_code, v_stage_step_code,'N',gp.ss_run_id,gp.ss_execution_id,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version);
         
         if v_step_type = 'J'
         then
            ut.wait_for_ss_jobs('SS_LOAD_'||v_stage_step_code); 
         end if;
         
      end loop;   
      
      if c_subset_reload%isopen then
         close c_subset_reload;
      end if;
      
      ut.log(const.k_subsys_subset,'Checking to see if failed LOAD_DATA_SUBSET stage_step_codes were re-executed successfully.  If not call fix_subset_load again (recursively).',null,null,const_module);	
      
      if gp.get_subset_reload_attempts < const.k_max_subset_load_retries
      then
      
        open c_subset_reload;
        fetch c_subset_reload into v_stage_step_code, v_step_type, v_stmt_seq;
        close c_subset_reload;
        
        if v_stage_step_code is not null 
        then
           subsetting_control.fix_subset_load;
        end if;  
      
      end if;
      
    exception when others then
      if c_subset_reload%isopen then
         close c_subset_reload;
      end if;

      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);

      rollback;

   end fix_subset_load;		
    
    
  procedure resolve_ref_seq is

    cursor GetInvalidOrderItems is select par_table_actual_owner,parent_table,parent_table_order,child_table_actual_owner,child_table,child_table_order 
                                from ss_graph_sort
                                where parent_table_order > child_table_order;
    v_nLoopCounter number;
    v_nExit number;
    
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.resolve_ref_seq';
  begin


      delete   ss_table_list_sort;
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows deleted from ss_table_list_sort',v_code,v_errm,const_module);	
      
      insert into ss_table_list_sort(table_id,owner,actual_owner,table_name,table_order)
      
      select table_id,owner,actual_owner,table_name,rownum table_order
      from
      (
        select table_id, actual_owner,owner,table_name ,max_tab_id,dbms_random.value(1,max_tab_id) rnd_order
        from
        (
          select table_id,actual_owner,owner,table_name,max(table_id) over (partition by 1) max_tab_id 
          from
          (
            select rownum table_id,gp.src_prefix||'_'||smt.owner actual_owner,smt.owner ,smt.table_name 
            from ss_missing_tables smt 
          )   
        )  order by rnd_order -- random order
      );
      
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows inserted into ss_table_list_sort',v_code,v_errm,const_module);	      


      delete from ss_graph_sort;
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows deleted from ss_graph_sort',v_code,v_errm,const_module);	

      insert into ss_graph_sort(child_table_actual_owner ,child_table_owner,child_table , Par_table_actual_owner,Parent_table_owner,Parent_table) 

       select  dc_child.actual_owner child_table_actual_owner,dc_child.OWNER child_table_owner,dc_child.table_name child_table,
       dc_parent.actual_owner parent_table_actual_owner,dc_parent.owner parent_table_owner,dc_parent.table_name parent_table 
       from ss_missing_tables mt_child
       join dd_constraints dc_child on dc_child.owner = mt_child.owner and mt_child.table_name = dc_child.table_name and dc_child.constraint_type = 'R'
       join dd_constraints dc_parent on dc_parent.constraint_name  = dc_child.r_constraint_name  and dc_parent.owner  = dc_child.R_OWNER and dc_parent.constraint_type = 'P'
       join ss_missing_tables mt_parent on mt_parent.owner = dc_parent.owner and mt_parent.table_name = dc_parent.table_name;
     
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows inserted into ss_graph_sort',v_code,v_errm,const_module);	
      
      update ss_graph_sort sgs set child_table_order =  
      (select table_order from ss_table_list_sort stls where stls.actual_owner = sgs.child_table_actual_owner and stls.table_name = sgs.child_table);

      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' ss_graph_sort rows updated',v_code,v_errm,const_module);	

    
    v_nLoopCounter := 1;
    v_nExit := 0;

    update ss_missing_tables smt 
       set obj_cre_seq = ( select table_order 
                             from ss_table_list_sort stls 
                            where stls.owner = smt.owner 
                              and stls.table_name = smt.table_name);
  
    ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' ss_missing_tables rows updated for obj_cre_seq',v_code,v_errm,const_module);	
  
    update ss_missing_tables smt 
       set table_ddl =  ( select object_ddl 
                            from md_ddl  md
                           where md.owner = smt.owner 
                             and md.object_name = smt.table_name 
                             and md.object_type = 'TABLE' );
  
    ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' ss_missing_tables rows updated for table_ddl',v_code,v_errm,const_module);	
  
    commit;
    
  exception when others then
      commit;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);


  end resolve_ref_seq;
  
  
  procedure swap_order (p_parent_table_actual_owner varchar2,p_parent_table_name varchar2,p_parent_table_order varchar2,
                        p_child_table_actual_owner varchar2,p_child_table_name varchar2,p_child_table_order varchar2)
  is
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.swap_order';  
  begin
    update ss_table_list_sort set table_order = p_child_table_order where actual_owner = p_parent_table_actual_owner and table_name = p_parent_table_name;
    update ss_table_list_sort set table_order = p_parent_table_order where actual_owner = p_child_table_actual_owner and table_name = p_child_table_name;
      
    update ss_graph_sort set parent_table_order = p_child_table_order where par_table_actual_owner = p_parent_table_actual_owner and parent_table = p_parent_table_name;
    update ss_graph_sort set child_table_order = p_parent_table_order where child_table_actual_owner = p_child_table_actual_owner and child_table = p_child_table_name;
  
    commit;
  end swap_order;


 procedure load_c_load_rules is
  
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_c_load_rules';
  begin
  
      delete from ss_c_rules_list;
      
      insert into ss_c_rules_list(rule_id,actual_owner,owner,table_name,ss_rule,sel_sql)
      select src.rule_id,gp.tgt_prefix||'_'||src.owner actual_owner ,src.owner,src.table_name,src.ss_rule,
      subsetting_control.fget_c_rule_sql(gp.ss_run_id ,src.table_name ,sc.partitioned_YN)   sel_sql 
      from  ss_rules_config src 
      left outer join ss_load_excl_config slec  on slec.owner = src.owner and slec.table_name = src.table_name 
      left outer join ss_config sc  on src.owner = sc.owner and src.table_name = sc.table_name 
      where  slec.table_name is null and src.ss_rule is not null and src.ss_rule =  'C'; 


      insert into ss_rules_sql(owner,table_name,full_sql)
      select  ssrl.owner,ssrl.table_name,
      'insert into '||gp.tgt_prefix||'_'||ssrl.owner||'.'||ssrl.table_name ||' '||ssrl.sel_sql full_sql
      from  ss_c_rules_list ssrl; 
   
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows inserted into ss_rules_sql',v_code,v_errm,const_module);	           
      
      commit;
      
    exception when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    
  end load_c_load_rules;
  
 procedure load_star_load_rules is
  
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_star_load_rules';
  begin
  
      delete from ss_star_rules_list;
      
      insert into ss_star_rules_list(rule_id,actual_owner,owner,table_name,ss_rule,sel_sql)
      select src.rule_id,gp.tgt_prefix||'_'||src.owner actual_owner ,src.owner,src.table_name,src.ss_rule,
      subsetting_control.fget_star_rule_sql(gp.ss_run_id ,src.table_name ,sc.partitioned_YN)   sel_sql 
      from  ss_rules_config src
      left outer join ss_load_excl_config slec  on slec.owner = src.owner and slec.table_name = src.table_name 
      left outer join ss_config sc on src.owner = sc.owner and src.table_name = sc.table_name 
      where  slec.table_name is null and src.ss_rule is not null and src.ss_rule =  '*'; 


      insert into ss_rules_sql(owner,table_name,full_sql)
      select  ssrl.owner,ssrl.table_name,
      'insert into '||gp.tgt_prefix||'_'||ssrl.owner||'.'||ssrl.table_name ||' '||ssrl.sel_sql full_sql
      from  ss_star_rules_list ssrl; 
   
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows inserted into ss_rules_sql',v_code,v_errm,const_module);	           
      
      commit;
      
    exception when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    
  end load_star_load_rules;
  

  procedure load_cons_load_rules is
  
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.load_cons_load_rules';
  begin
  
      delete from ss_constraint_rules_list;
      
      insert into ss_constraint_rules_list(rule_id,actual_owner,owner,table_name,ss_rule)
      select src.rule_id,gp.tgt_prefix||'_'||src.owner actual_owner ,src.owner,src.table_name,src.ss_rule 
      from  ss_rules_config src --ss_config sc
      --left outer join ss_rules_config src on src.owner = sc.owner and src.table_name = sc.table_name 
      left outer join ss_load_excl_config slec  on slec.owner = src.owner and slec.table_name = src.table_name      
      where src.ss_rule is not null and slec.table_name is null and src.ss_rule not in ('C','*'); 


       delete from ss_constraint_rules_list_cons;
       for GetRulesRec in (select rule_id,ss_rule from ss_constraint_rules_list) loop
          for GetConsRec in (select regexp_substr(GetRulesRec.ss_rule,'[^,]+', 1, level) fk_component,level fk_seq
                             from dual
                             connect by regexp_substr(GetRulesRec.ss_rule, '[^,]+', 1, level) IS NOT NULL)  loop
              insert into ss_constraint_rules_list_cons(rule_id,actual_owner,owner,table_name,constraint_name,fk_seq)
              select GetRulesRec.rule_id,dc.actual_owner,dc.owner,dc.table_name,dc.constraint_name,GetConsRec.fk_seq
              from src_dd_constraints dc
              where dc.constraint_name = GetConsRec.fk_component;
          end loop;
          null;
      end loop;
  
      merge into ss_constraint_rules_list_cons tgt
      using (select rule_id,max(fk_seq) fk_max_seq from ss_constraint_rules_list_cons group by rule_id) src
      on (tgt.rule_id = src.rule_id)
      when matched then update set tgt.fk_max_seq = src.fk_max_seq;
      
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' ss_constraint_rules_list_cons rows updated from ss_constraint_rules_list_cons',v_code,v_errm,const_module);	
     
      delete from ss_rules_det;
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows deleted from ss_rules_det',v_code,v_errm,const_module);	
      
      insert into ss_rules_det(rule_id,fk_seq,fk_max_seq,det_type,constraint_name,owner,table_name,ri_cols,pk_cols )
        select rule_id ,fk_seq,fk_max_seq,det_type,constraint_name,owner,table_name,ri_cols,pk_cols from
        (
                select 'C' det_type ,res.rule_id,res.fk_seq,res.fk_max_seq,res.owner,res.table_name,res.constraint_name,res.ri_cols ,
                listagg (sdcc_pk.column_name,',') within group (order by sdcc_pk.position) pk_cols    
                from
                ( 
                  select scrlc.rule_id,scrlc.fk_seq,scrlc.fk_max_seq,sdc.owner,sdc.table_name,sdc.constraint_name ,
                  listagg (sdcc.column_name,',') within group (order by sdcc.position) ri_cols
                  from ss_constraint_rules_list_cons  scrlc
                  join (select owner,constraint_name,table_name from src_dd_constraints
                        union
                        select owner,constraint_name,table_name from src_pseudo_constraints
                  ) sdc  on sdc.owner = scrlc.owner and sdc.constraint_name = scrlc.constraint_name
                  join (select owner,constraint_name,column_name,position from src_dd_cons_columns
                        union
                        select owner,constraint_name,column_name,position from src_pseudo_cons_columns
                        ) sdcc on sdcc.owner = sdc.owner  and sdcc.constraint_name = sdc.constraint_name 
                  group by scrlc.rule_id,scrlc.fk_seq,scrlc.fk_max_seq,sdc.owner,sdc.table_name,sdc.constraint_name 
                ) res
                join (select owner,constraint_name,table_name,constraint_type from src_dd_constraints
                        union
                        select owner,constraint_name,table_name,constraint_type from src_pseudo_constraints ) sdc_pk  on sdc_pk.owner = res.owner and sdc_pk.table_name = res.table_name 
                and sdc_pk.constraint_type = 'P'
                join (select owner,constraint_name,column_name,position from src_dd_cons_columns
                        union
                        select owner,constraint_name,column_name,position from src_pseudo_cons_columns )sdcc_pk 
                        on sdcc_pk.owner = res.owner  and sdcc_pk.constraint_name = sdc_pk.constraint_name      
                group by res.rule_id,res.fk_seq,res.fk_max_seq,res.owner,res.table_name,res.constraint_name,res.ri_cols
                union all
                select 'P' det_type,res.rule_id,res.fk_seq,res.fk_max_seq,res.r_owner,res.table_name,res.constraint_name,res.ri_cols ,
                listagg (sdcc_pk.column_name,',') within group (order by sdcc_pk.position) pk_cols    
                from
                ( 
                  select scrlc.rule_id,scrlc.fk_seq,scrlc.fk_max_seq,sdc.r_owner,sdcc.table_name,sdc.constraint_name ,
                  listagg (sdcc.column_name,',') within group (order by sdcc.position) ri_cols
                  from ss_constraint_rules_list_cons  scrlc
                  join src_dd_constraints sdc  on sdc.owner = scrlc.owner and sdc.constraint_name = scrlc.constraint_name
                  join (select owner,constraint_name,table_name,column_name,position from src_dd_cons_columns
                        union
                        select owner,constraint_name,table_name,column_name,position from src_pseudo_cons_columns )
                    sdcc on sdcc.owner = sdc.r_owner  and sdcc.constraint_name = sdc.r_constraint_name 
                  group by scrlc.rule_id,scrlc.fk_seq,scrlc.fk_max_seq,sdc.r_owner,sdcc.table_name,sdc.constraint_name
                ) res
                join (select owner,constraint_name,table_name,constraint_type from src_dd_constraints
                        union
                        select owner,constraint_name,table_name,constraint_type from src_pseudo_constraints ) sdc_pk  on sdc_pk.owner = res.r_owner and sdc_pk.table_name = res.table_name 
                and sdc_pk.constraint_type = 'P'
                join src_dd_cons_columns sdcc_pk on sdcc_pk.owner = res.r_owner  and sdcc_pk.constraint_name = sdc_pk.constraint_name      
                group by res.rule_id,res.fk_seq,res.fk_max_seq,res.r_owner,res.table_name,res.constraint_name,res.ri_cols
                
        ) ;
      
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows inserted into ss_rules_det',v_code,v_errm,const_module);	
      
      delete from ss_rules_part_sql;
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows deleted from ss_rules_part_sql',v_code,v_errm,const_module);	

      insert into ss_rules_part_sql (constraint_name,owner,table_name,fk_max_seq,fk_seq,rule_id,det_type,sel_sql,sql_filter,sql_seq) 
        select constraint_name,owner,table_name,fk_max_seq,fk_seq,rule_id,det_type,sel_sql,sql_filter,row_number() over (partition by rule_id order by det_type,fk_seq) sql_seq
        from
        (
          select srd.constraint_name,srd.owner,srd.table_name,srd.fk_max_seq,srd.fk_seq,srd.rule_id,srd.det_type,
          'select   *  from '||gp.get_src_prefix||'_'||srd.owner||'.'||srd.table_name||'@'||gp.ss_db_link||' ' sel_sql,
          'where ('||ri_cols||') in ' sql_filter
          from ss_rules_det srd
          where   fk_seq = 1 and det_type = 'C'
          union all
          select srd_c.constraint_name,srd_c.owner,srd_c.table_name,srd_c.fk_max_seq,srd_c.fk_seq,srd_c.rule_id,srd_c.det_type,
          'select  '||srd_p.ri_cols||'  from '||gp.get_src_prefix||'_'||srd_c.owner||'.'||srd_c.table_name||'@'||gp.ss_db_link||' ' sel_sql,
          'where ('||srd_c.ri_cols||') in ' sql_filter
          from ss_rules_det srd_c 
          join  ss_rules_det srd_p on srd_p.rule_id = srd_c.rule_id and srd_p.owner = srd_c.owner and srd_p.table_name = srd_c.table_name and srd_p.det_type = 'P'          
          where   srd_c.fk_seq <> 1 and srd_c.det_type = 'C'
          union all
          select srd.constraint_name,srd.owner,srd.table_name,srd.fk_max_seq,srd.fk_seq,srd.rule_id,srd.det_type,
          'select  '||ri_cols||'  from '||gp.get_src_prefix||'_'||srd.owner||'.'||srd.table_name||'@'||gp.ss_db_link||' ' sel_sql,
          'where ('||srd.pk_cols||') in ('||fget_part_clause(gp.ss_run_id ,srd.table_name ,sc.partitioned_YN,srd.pk_cols)||')'   sql_filter
          from ss_rules_det srd
          left outer join  ss_config sc on sc.owner = srd.owner and sc.table_name = srd.table_name
          where   fk_seq = fk_max_seq and det_type = 'P'      
        );
        
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows inserted into ss_rules_part_sql',v_code,v_errm,const_module);	        
      
      delete from ss_rules_sql;
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows deleted from ss_rules_sql',v_code,v_errm,const_module);	      
            
      insert into ss_rules_sql(table_name,owner,full_sql)
      
      select table_name,owner,'insert into '||gp.tgt_prefix||'_'||owner||'.'||table_name ||listagg (full_sql1, ' union ') within group (order by rule_id) full_sql2
      from
      (  
          select  scrl.rule_id,scrl.owner,scrl.table_name,
          listagg (case when sql_seq <> 1 then ' (' else ' ' end||res.sel_sql ||' '||res.sql_filter) within group (order by res.det_type,res.fk_seq)|| rpad(' ',res.no_queries,')') full_sql1
          from  (select rule_id,owner,table_name,sql_filter,sel_sql,det_type,fk_seq,sql_seq,max(sql_seq) over (partition by rule_id) no_queries 
                from ss_rules_part_sql) res
          join ss_constraint_rules_list scrl on res.rule_id = scrl.rule_id
          group by scrl.rule_id,res.no_queries,scrl.owner,scrl.table_name
      ) group by owner,table_name;
   
      ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows inserted into ss_rules_sql',v_code,v_errm,const_module);	           
      
      commit;
      
    exception when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Error rolled back: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    
  end load_cons_load_rules;
  
  
  function fget_part_clause(p_ss_run_id number ,p_table_name varchar2,p_partitioned_YN varchar2,p_pk_cols varchar2) return varchar2 is
  
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.fget_part_clause';
    v_sFilter varchar2(4000);
  begin
  
      if p_partitioned_YN = 'N' then 
      begin 
      
        select listagg (''''||comp_code||'''',',') within group (order by 1) 
        into v_sFilter
        from ss_companies
        where ss_run_id = p_ss_run_id
        group by ss_run_id;
        
        return v_sFilter;
        
        exception when others then
          return null;
      end;
      end if;
      
      if p_partitioned_YN = 'Y' then 
        begin  
          select listagg ('select '||p_pk_cols||' from SRC_VW_'|| p_table_name ||'_'||partition_name,' union all ') within group (order by 1)
          into v_sFilter
          from ss_companies
          where  ss_run_id = p_ss_run_id;
      
          return v_sFilter;
          
          exception when others then
            return null;
        end;
      end if;
      
      exception when others then

        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,'Error',v_code,v_errm,const_module);
  
  end fget_part_clause;
  
  
function fget_c_rule_sql(p_ss_run_id number ,p_table_name varchar2,p_partitioned_YN varchar2) return varchar2 is
  
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.fget_c_rule_sql';
    v_sSql varchar2(4000);
  begin
  
      if p_partitioned_YN = 'N' or p_partitioned_YN is null then 
      begin 
      
        select 'select * from SRC_VW_'||p_table_name|| ' where comp_code in ('|| listagg (''''||comp_code||'''',',') within group (order by 1)||')' 
        into v_sSql
        from ss_companies
        where ss_run_id = p_ss_run_id
        group by ss_run_id;
        
        return v_sSql;
        
        exception when others then
          return null;
      end;
      end if;
      
      if p_partitioned_YN = 'Y' then 
        begin  
          select listagg ('select '||'*'||' from SRC_VW_'|| p_table_name ||'_'||partition_name,' union all ') within group (order by 1)
          into v_sSql
          from ss_companies
          where  ss_run_id = p_ss_run_id;
      
          return v_sSql;
          
          exception when others then
            return null;
        end;
      end if;
      
      exception when others then

        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,'Error',v_code,v_errm,const_module);
  
  end fget_c_rule_sql;
  
  function fget_star_rule_sql(p_ss_run_id number ,p_table_name varchar2,p_partitioned_YN varchar2) return varchar2 is
  
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'subsetting_control.fget_star_rule_sql';
  begin
      
        return 'select * from SRC_VW_'||p_table_name;
        
      exception when others then

        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,'Error',v_code,v_errm,const_module);
  
  end fget_star_rule_sql;  
  
  procedure load_rule_based_data_stmts is
  
    const_module      CONSTANT  varchar2(62) := 'subsetting_control.load_rule_based_data_stmts';

    v_nStep number;
    v_nOrder number;
    v_code        number;
    v_errm        varchar2(4000);
    

  begin

      ut.log(const.k_subsys_subset,'Processing..  ',null,null,const_module);	

      delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_LOAD_DATA_RULE||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for LOAD_DATA_RULE rules-based data',null,null,const_module);

      delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_LOAD_DATA_RULE||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for k_LOAD_DATA_RULE views  for rules-based data',null,null,const_module);   

      delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_GEN_VIEW_RULE||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for GEN_VIEW_RULE ',null,null,const_module);


      delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_GEN_VIEW_RULE||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for GEN_VIEW_RULE views  for rules-based data',null,null,const_module);   
    
      delete ss_ctrl_exec_result where ss_run_id = gp.ss_run_id and stage_step_code like const.k_GEN_SYN_RULE||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_exec_result for GEN_SYN_RULE ',null,null,const_module);

      delete ss_ctrl_stmts where ss_run_id = gp.ss_run_id and stage_step_code like const.k_GEN_SYN_RULE||'%';
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from ss_ctrl_stmts for GEN_SYN_RULE synonyms for rules-based data',null,null,const_module);   
  
  
     v_nStep := 1;
     v_nOrder := 1;
  
     insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,object_type,stmt)
      
      select gp.ss_run_id as ss_run_id,
            const.k_GEN_VIEW_RULE||LPAD(to_char(row_number() over (partition by 1 order by 1)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
            null  dependent_ss_code,
            'P' step_type,
            'GEN_VIEW_RULE' stage_type,
            to_char(v_nOrder) as aorder,
            res.owner,
            res.table_name object_name,'TABLE',
            'gen_metadata.create_partition_view@'||gp.ss_db_link||'('''||gp.tgt_prefix||'_'||res.owner||''','''||res.table_name||''','''||res.comp_list ||''')' stmt
      from   
            
      (with get_part_list as (select  listagg (partition_name,',') within group (order by 1) comp_list from ss_companies 
                             where  ss_run_id = gp.ss_run_id)
      select srs.owner, srs.table_name ,gcl.comp_list  
      from ss_rules_sql srs
      join ss_config sc on sc.owner = srs.owner and sc.table_name = srs.table_name  and partitioned_YN = 'Y'
      join get_part_list gcl on 1=1
      
      union all
    
      select srs.owner, srs.table_name ,'' comp_list  
      from ss_rules_sql srs
      ) res;
      
      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records inserted into ss_ctrl_stmts for GEN_VIEW_RULE',null,null,const_module);
      
      insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,object_type,stmt)
      select gp.ss_run_id as ss_run_id,
            const.k_GEN_SYN_RULE||LPAD(to_char(row_number() over (partition by 1 order by 1)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
            null  dependent_ss_code,
            'D' step_type,
            'GEN_SYN_RULE' stage_type,
            to_char(v_nOrder) as aorder,
            owner,
            table_name object_name,'TABLE',
            stmt
      from
      (select srs.owner,srs.table_name, 
            'CREATE OR REPLACE SYNONYM '||gp.run_env||'.'||'SRC_VW_'||srs.table_name||'_'||c.partition_name||' FOR '|| 
             ' VW_'||srs.table_name||'_'||partition_name||'@'||gp.ss_db_link stmt 
        from ss_rules_sql srs
        join ss_companies c on c.ss_run_id = gp.ss_run_id
        join ss_config sc on sc.owner = srs.owner and sc.table_name = srs.table_name  and partitioned_YN = 'Y'
        
        union all
    
      select srs.owner, srs.table_name ,'CREATE OR REPLACE SYNONYM '||gp.run_env||'.'||'SRC_VW_'||table_name||' FOR '|| 
             ' VW_'||table_name||'@'||gp.ss_db_link stmt 
      from ss_rules_sql srs
        
        
        
        );
     
      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records inserted into ss_ctrl_stmts for GEN_SYN_RULE',null,null,const_module);
     
     
      insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,object_name,object_type,stmt)
        select gp.ss_run_id as ss_run_id,
            const.k_LOAD_DATA_RULE||LPAD(to_char(row_number() over (partition by 1 order by 1)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
            null  dependent_ss_code,
            'S' step_type,
            'LOAD_DATA_RULE' stage_type,
            to_char(v_nOrder) as aorder,
            srs.owner ,
            srs.table_name object_name,
            'TABLE',
           srs.full_sql stmt 
      from ss_rules_sql srs
      left outer join ss_config sc on sc.owner = srs.owner and sc.table_name = srs.table_name
      where sc.table_name is null or sc.ss_type = 'TYPE_SS_NONE';

      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records inserted into ss_ctrl_stmts for LOAD_DATA_RULE',null,null,const_module);
      
  exception 
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module); 
      
  end load_rule_based_data_stmts;
  

  procedure get_inv_fk_from_log is
 
    const_module      CONSTANT  varchar2(62) := 'subsetting_control.get_inv_fk_from_log';
 
    v_code        number;
    v_errm        varchar2(4000);
    
  begin
  
      ut.log(const.k_subsys_subset,'Processing..  ',null,null,const_module);	

      --ORA-02298: cannot validate (PSM_INTEGRATION.FP_FHD_FK) - parent keys not found
  
      delete from ss_ref_data_failures_list;
      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records deleted from ss_ref_data_failures_list',null,null,const_module);
      
      insert into ss_ref_data_failures_list(owner,table_name,aconstraint,actual_cons_owner,cons_name)
      select distinct dc.actual_owner,dc.table_name, aconstraint,actual_cons_owner,cons_name from
      (
        select regexp_substr(errm ,'[^(]+(\.)[^)]+' ) aconstraint,
        replace(regexp_substr(errm ,'[^(]+(\.)' ),'.','') actual_cons_owner,
        replace(regexp_substr(errm ,'(\.)[^)]+' ),'.','') cons_name
        from ss_log sl
        where sl.err_code = const.k_const_ORA_ERROR_PARENT_KEY_NOT_FOUND  -- -02298 
      ) res join dd_constraints dc on dc.actual_owner = res.actual_cons_owner and dc.constraint_name = res.cons_name ;
  
      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records inserted into ss_ref_data_failures_list',null,null,const_module);
  
      commit;
      
      delete from ss_ref_data_failures_det;
      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records deleted from ss_ref_data_failures_det',null,null,const_module);

      insert into ss_ref_data_failures_det (CONSTRAINT_NAME,CHILD_TABLE_OWNER, CHILD_TABLE_NAME, PARENT_TABLE_OWNER, PARENT_TABLE_NAME, CHILD_COLS,PARENT_COLS)   
      select  ac_child2.constraint_name,ac_child2.child_table_owner,ac_child2.child_table_name,
      ac_child2.parent_table_owner,ac_parent.table_name parent_table_name,
      ac_child2.child_cols,listagg (ac_parent_cols.column_name,',') within group (order by 1) parent_cols from
      (
          select ac_child.constraint_name,ac_child.owner child_table_owner,ac_child.table_name child_table_name,
          ac_child.r_owner parent_table_owner,ac_child.r_constraint_name parent_constraint_name,--ac_parent.table_name parent_table_name,
          listagg (ac_child_cols.column_name,',') within group (order by 1) child_cols
          from dd_constraints ac_child 
          join dd_cons_columns ac_child_cols on ac_child_cols.constraint_name = ac_child.constraint_name and ac_child_cols.owner = ac_child.owner
          where (ac_child.actual_owner ,ac_child.constraint_name) in (select actual_cons_owner,cons_name from ss_ref_data_failures_list)
          group by ac_child.constraint_name,ac_child.owner ,ac_child.table_name ,
          ac_child.r_owner ,ac_child.r_constraint_name 
      ) ac_child2
      join dd_constraints ac_parent on ac_parent.constraint_name = ac_child2.parent_constraint_name and ac_parent.owner = ac_child2.parent_table_owner 
      join dd_cons_columns ac_parent_cols on ac_parent_cols.constraint_name = ac_parent.constraint_name and ac_parent_cols.owner = ac_parent.owner
      group by ac_child2.constraint_name,ac_child2.child_table_owner,ac_child2.child_table_name,
      ac_child2.parent_table_owner,ac_child2.parent_constraint_name,ac_parent.owner,ac_parent.table_name,
      ac_child2.child_cols;
      
      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records inserted into ss_ref_data_failures_det',null,null,const_module);
      
      delete from ss_ref_data_failures_fix_sql;
      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records deleted from ss_ref_data_failures_fix_sql',null,null,const_module);
      
      insert into ss_ref_data_failures_fix_sql(child_table_owner,child_table_name,stmt,aorder )
      select replace(child_table_owner,gp.tgt_prefix||'_',''),child_table_name,stmt,aorder from
      (
        select child_table_owner,child_table_name, stmt1 ||' ('||stmt2||' minus '||stmt3||')' stmt,1 aorder from
        (
          select gp.tgt_prefix||'_'||p1.child_table_owner child_table_owner ,p1.child_table_name,'delete from '||gp.tgt_prefix||'_'||p1.child_table_owner||'.'||p1.child_table_name||'  where  ('||p1.child_cols||')  in' stmt1,p2.stmt2,p3.stmt3
          from ss_ref_data_failures_det p1 
          join 
          (select gp.tgt_prefix||'_'||child_table_owner child_table_owner ,constraint_name ,'select  '||child_cols||' from '||gp.tgt_prefix||'_'||child_table_owner||'.'||child_table_name ||' where '||REPLACE(child_cols,',','||')||' is not null' stmt2
          from ss_ref_data_failures_det ) p2 on  gp.tgt_prefix||'_'||p1.child_table_owner = p2.child_table_owner and p1.constraint_name = p2.constraint_name
          join
          (select gp.tgt_prefix||'_'||child_table_owner child_table_owner,constraint_name,'select  '||parent_cols||' from '||gp.tgt_prefix||'_'||parent_table_owner||'.'||parent_table_name stmt3
          from ss_ref_data_failures_det ) p3 on p2.child_table_owner=  p3.child_table_owner and p2.constraint_name = p3.constraint_name
        )
        union
        select gp.tgt_prefix||'_'||child_table_owner,child_table_name,'alter table '||gp.tgt_prefix||'_'||child_table_owner||'.'||child_table_name||' enable constraint '||constraint_name
        ,2 aorder
        from ss_ref_data_failures_det srdf 
        union
        
          select gp.tgt_prefix||'_'||child_table_owner,child_table_name,'alter table '||gp.tgt_prefix||'_'||child_table_owner||'.'||child_table_name || ' disable all triggers', 0 aorder
          from ss_ref_data_failures_det srdf
          union
          select gp.tgt_prefix||'_'||child_table_owner,child_table_name,'alter table '||gp.tgt_prefix||'_'||child_table_owner||'.'||child_table_name || ' enable all triggers', 3 aorder
          from ss_ref_data_failures_det srdf
      );
      
      ut.log(const.k_subsys_subset,to_char(sql%rowcount) || ' records inserted into ss_ref_data_failures_fix_sql',null,null,const_module);

  exception 
     when others then      
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module); 

  end get_inv_fk_from_log;
  
  
  procedure fix_inv_fk is
  
    v_code        number;
    v_errm        varchar2(4000);

    cursor cGetStmts is  
       select child_table_owner,child_table_name,stmt,aorder
         from ss_ref_data_failures_fix_sql
        order by aorder;
                       
    const_module      CONSTANT  varchar2(62) := 'subsetting_control.fix_inv_fk';
  
  begin
  
    ut.log(const.k_subsys_subset,'Processing..  ',null,null,const_module);	
    
    for cGetStmtsRec in cGetStmts loop
    
       begin
      
          ut.log(const.k_subsys_subset,substr('Executing '||cGetStmtsRec.stmt,1,4000),null,null,const_module);   
          execute immediate  cGetStmtsRec.stmt;   
          if cGetStmtsRec.stmt like'delete%'
          then
             ut.log(const.k_subsys_subset,to_char(sql%rowcount)||' rows deleted from '||cGetStmtsRec.child_table_owner||'.'||cGetStmtsRec.child_table_name,null,null,const_module);   
          end if;

       exception when others then
          
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr('Error '||cGetStmtsRec.stmt,1,4000),v_code,v_errm,const_module); 
          
          If v_Code = const.k_const_ORA_ERROR_PARENT_KEY_NOT_FOUND then
            ut.log(const.k_subsys_subset,'Error is '||const.k_const_ORA_ERROR_PARENT_KEY_NOT_FOUND ||' so calling fix_inv_fk_recur',v_code,v_errm,const_module); 
            fix_inv_fk_recur (cGetStmtsRec.child_table_owner,cGetStmtsRec.child_table_name,v_errm,0);
          end if;

        end;
      
    end loop;
     
  exception 
    when others then
       v_code := SQLCODE;
       v_errm := SUBSTR(SQLERRM,1,4000);
       ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module); 
  end fix_inv_fk;
      
      
  procedure fix_inv_fk_recur (p_child_table_owner varchar2 ,p_child_table_name varchar2,p_errm varchar2,p_level number) is

      v_code        number;
      v_errm        varchar2(4000);
      const_module      CONSTANT  varchar2(62) := 'subsetting_control.fix_inv_fk_recur';
  
      v_constraint varchar2(128);
      v_actual_cons_owner varchar2(128);
      v_cons_name varchar2(128);
      
      
      v_parent_table_owner varchar2(128);
      v_parent_table_name varchar2(128);
      v_child_cols varchar2(128);
      v_parent_cols  varchar2(128);
      
      v_Stmt clob;
      v_Stmt1 varchar2(4000);
      v_Stmt2 varchar2(4000);
      v_Stmt3 varchar2(4000);

  begin

      ut.log(const.k_subsys_subset,'Processing p_child_table_owner :'||p_child_table_owner||' p_child_table_name : '||p_child_table_owner||' p_level :'||p_level,null,null,const_module);	

      v_constraint := regexp_substr(p_errm ,'[^(]+(\.)[^)]+' );
      v_actual_cons_owner := replace(regexp_substr(p_errm ,'[^(]+(\.)' ),'.','');
      v_cons_name :=  replace(regexp_substr(p_errm ,'(\.)[^)]+' ),'.','');
    
      ut.log(const.k_subsys_subset,'Processing v_constraint: '||v_constraint||' v_actual_cons_owner: '||v_actual_cons_owner||' v_cons_name: '||v_cons_name,null,null,const_module);	      
      --dbms_output.put_line (v_constraint||' '||v_actual_cons_owner||' '||v_cons_name);
      
      select  gp.tgt_prefix||'_'||ac_child2.parent_table_owner,ac_parent.table_name parent_table_name,
      ac_child2.child_cols,listagg (ac_parent_cols.column_name,',') within group (order by 1) parent_cols 
      into  v_parent_table_owner,v_parent_table_name,v_child_cols,v_parent_cols 
      from
      (
          select ac_child.constraint_name,ac_child.owner child_table_owner,ac_child.table_name child_table_name,
          ac_child.r_owner parent_table_owner,ac_child.r_constraint_name parent_constraint_name,--ac_parent.table_name parent_table_name,
          listagg (ac_child_cols.column_name,',') within group (order by 1) child_cols
          from dd_constraints ac_child 
          join dd_cons_columns ac_child_cols on ac_child_cols.constraint_name = ac_child.constraint_name and ac_child_cols.owner = ac_child.owner
          where (ac_child.actual_owner ,ac_child.constraint_name) in (select v_actual_cons_owner,v_cons_name from dual)
          group by ac_child.constraint_name,ac_child.owner ,ac_child.table_name ,
          ac_child.r_owner ,ac_child.r_constraint_name 
      ) ac_child2
      join dd_constraints ac_parent on ac_parent.constraint_name = ac_child2.parent_constraint_name and ac_parent.owner = ac_child2.parent_table_owner 
      join dd_cons_columns ac_parent_cols on ac_parent_cols.constraint_name = ac_parent.constraint_name and ac_parent_cols.owner = ac_parent.owner
      group by ac_child2.constraint_name,ac_child2.child_table_owner,ac_child2.child_table_name,
      ac_child2.parent_table_owner,ac_child2.parent_constraint_name,ac_parent.owner,ac_parent.table_name,
      ac_child2.child_cols;

      ut.log(const.k_subsys_subset,'Processing v_parent_table_owner: '||v_parent_table_owner||' v_parent_table_name: '||v_parent_table_name||' v_child_cols: '||v_child_cols||' v_parent_cols: '||v_parent_cols,null,null,const_module);	      
      --dbms_output.put_line (v_parent_table_owner||' '||v_parent_table_name||' '||v_child_cols||' '||v_parent_cols); 
      
      v_Stmt := 'alter table '||p_child_table_owner||'.'||p_child_table_name || ' disable all triggers';
      ut.log(const.k_subsys_subset,'Executing: '||v_Stmt,null,null,const_module);	      
      --dbms_output.put_line (v_Stmt); 
      execute immediate (v_Stmt);
   
      v_Stmt1 :=  'delete from '||p_child_table_owner||'.'||p_child_table_name||'  where  ('||v_child_cols||')  in ' ;
      v_Stmt2 := 'select  '||v_child_cols||' from '||p_child_table_owner||'.'||p_child_table_name ||' where '||REPLACE(v_child_cols,',','||')||' is not null';
      v_Stmt3 := 'select  '||v_parent_cols||' from '||v_parent_table_owner||'.'||v_parent_table_name ;    
      v_Stmt := v_Stmt1 ||' ('||v_Stmt2||' minus '||v_Stmt3||')' ;      
      --dbms_output.put_line (v_Stmt); 
      
      begin
         ut.log(const.k_subsys_subset,'Executing: '||v_Stmt,null,null,const_module);	  
         execute immediate (v_Stmt);
      exception 
         when others then
            v_code := SQLCODE;
            v_errm := SUBSTR(SQLERRM,1,4000);
            ut.log(const.k_subsys_subset,substr('Error '||v_Stmt,1,4000),v_code,v_errm,const_module); 
          
            if v_Code = const.k_const_ORA_ERROR_PARENT_KEY_NOT_FOUND then
               if p_level > 9 then
                  null;
               else
                  ut.log(const.k_subsys_subset,'Executing recursive call: fix_inv_fk_recur ('||v_parent_table_owner||','||v_parent_table_name||','||v_errm||','||p_level||'+1)',null,null,const_module);	
                  fix_inv_fk_recur (v_parent_table_owner,v_parent_table_name,v_errm,p_level+1);
 
 -- CP: not sure if block below is required after recursive call ?                     
                  begin
                     execute immediate (v_Stmt);
                  exception when others then   
                     v_code := SQLCODE;
                     v_errm := SUBSTR(SQLERRM,1,4000);
                     ut.log(const.k_subsys_subset,substr('An unexpected Error Occurred',1,4000),v_code,v_errm,const_module); 
                  end;
               end if;
            end if;

      end;
      
      v_Stmt := 'alter table '||p_child_table_owner||'.'||p_child_table_name||' enable constraint '||v_cons_name;
      ut.log(const.k_subsys_subset,'Executing: '||v_Stmt,null,null,const_module);	  
      --dbms_output.put_line (v_Stmt); 
      execute immediate (v_Stmt);
      
      v_Stmt := 'alter table '||p_child_table_owner||'.'||p_child_table_name || ' enable all triggers';
      ut.log(const.k_subsys_subset,'Executing: '||v_Stmt,null,null,const_module);	  
      --dbms_output.put_line (v_Stmt); 
      execute immediate (v_Stmt);
         
   exception 
      when others then
         v_code := SQLCODE;
         v_errm := SUBSTR(SQLERRM,1,4000);
         ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          
   end fix_inv_fk_recur;

end subsetting_control;
/