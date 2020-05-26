create or replace PACKAGE BODY obfuscation_control
AS

  procedure switch_obfus_on_off ( p_on_off                varchar2,
                                  p_obfus_run_id          NUMBER   DEFAULT NULL,
                                  p_src_prefix            VARCHAR2 DEFAULT NULL,
                                  p_tgt_prefix            VARCHAR2 DEFAULT NULL,
                                  p_run_env               VARCHAR2 DEFAULT NULL,
                                  p_anon_version          VARCHAR2 DEFAULT NULL)
  is
    pragma autonomous_transaction;
    v_code        number;
    v_errm        varchar2(4000);
    v_on_off      varchar2(3);
    const_module  CONSTANT  varchar2(62) := 'obfuscation_control.switch_obfus_on_off';
  begin
    select on_off into v_on_off from obfus_onoff_switch;
    if upper(v_on_off) = upper(p_on_off)
    then
      obfuscation_control.obfus_log('obfus_onoff_switch already turned ' || upper(p_on_off),p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,const_module);
    else
      delete obfus_onoff_switch; -- only ever allow one row
      insert into obfus_onoff_switch (on_off, description)  -- on_off check constraint
      values (upper(p_on_off), 'OFF disables obfuscation and if obfuscation is switched OFF while running then obfuscation is TERMINATED at completion of current stage. ON allows obfuscation as normal.');
      commit;
      obfus_log('obfus_onoff_switch turned ' || p_on_off,null,null,null,SQLCODE,SQLERRM,const_module);

      if (p_obfus_run_id is not null and p_src_prefix is not null and p_tgt_prefix is not null and p_run_env is not null and p_anon_version is not null)
      then
        obfuscation_control.update_obfus_control(p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, upper(p_on_off));
      else
        if upper(p_on_off) = 'OFF'
        then
          update obfus_control set obfus_status = 'OFF', updated_time = SYSDATE where obfus_status IN ( 'ON', 'RUNNING');
        elsif upper(p_on_off) = 'ON'
        then
          update obfus_control set obfus_status = 'ON', updated_time = SYSDATE where obfus_status IN ( 'OFF', 'RUNNING');
        end if;
      end if;
    end if;
    
    commit;    
  exception
    when excep.x_CHK_ONOFF_SWITCH_violated then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      rollback;
      obfuscation_control.obfus_log(const_module || ' error ',null,null,null,v_code,v_errm,const_module);
      RAISE_APPLICATION_ERROR(-20002, 'obfus_onoff_switch must be either ''ON'' or ''OFF''.' || v_errm);
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      rollback;
      obfuscation_control.obfus_log(const_module || ' error ',null,null,null,v_code,v_errm,const_module);
  end switch_obfus_on_off;

  function can_continue
    return boolean
  is
    v_code       number;
    v_errm       varchar2(4000);
    v_on_off     varchar2(3);
    bln_continue boolean;
    const_module  CONSTANT  varchar2(62) := 'obfuscation_control.can_continue';
  begin

    begin
      select on_off into v_on_off from obfus_onoff_switch;
      if upper(v_on_off) = 'OFF'
      then
        bln_continue := FALSE;
      else
        bln_continue := TRUE;
      end if;

    exception when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      obfuscation_control.obfus_log(const_module || ' error ',null,null,null,v_code,v_errm,const_module);
      bln_continue := FALSE;
    end;
    return bln_continue;
  end can_continue;

  procedure init_audit_events
  is
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'obfuscation_control.init_audit_events';
  begin
    dbms_session.set_identifier ('adcfs\ksheehan1' || ':' || '1');
    execute immediate 'insert into tgt_audit_events(EVENT_ID,COMP_CODE  ) VALUES(1,null)';
    commit;
  exception
    when dup_val_on_index then
      null;
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      obfuscation_control.obfus_log(const_module || ' error ',null,null,null,v_code,v_errm,const_module);
  end init_audit_events;
  

  procedure truncate_report_tables
  is
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'obfuscation_control.truncate_report_tables';
  begin
    obfuscation_control.obfus_log('truncating tables qa_exceptions,qa_results_pivot,qa_results_tmp,stats_results_pivot1,stats_results_pivot2,stats_results_1,stats_results_2,stats_results_tmp,partition_update_counts',null,null,null,null,null,const_module);  
    execute immediate 'truncate table qa_exceptions reuse storage';
    execute immediate 'truncate table qa_results_pivot reuse storage';
    execute immediate 'truncate table qa_results_tmp reuse storage';
    execute immediate 'truncate table stats_results_pivot1 reuse storage';
    execute immediate 'truncate table stats_results_pivot2 reuse storage';
    execute immediate 'truncate table stats_results_1 reuse storage';      
    execute immediate 'truncate table stats_results_2 reuse storage';     
    execute immediate 'truncate table stats_results_tmp reuse storage'; 
    execute immediate 'truncate table partition_update_counts reuse storage';
  exception
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      obfuscation_control.obfus_log(const_module || ' error ',null,null,null,v_code,v_errm,const_module);
  end truncate_report_tables;  

  
  procedure merge_obfus_ctrl_exec_result( p_obfus_run_id     number,
                                          p_stage_step_code  varchar2,
                                          p_stmt_seq         number,
                                          p_execution_id     number,
                                          p_start_timestamp  timestamp,
                                          p_end_timestamp    timestamp,
                                          p_status           varchar2,
                                          p_obfus_log_id     number)
  is
      pragma autonomous_transaction;
  begin

      merge into obfus_control_exec_result x
      using (select 1 from dual) y
         on (x.obfus_run_id = p_obfus_run_id and x.stage_step_code = p_stage_step_code and x.stmt_seq = p_stmt_seq and x.execution_id = p_execution_id )
          when matched
          then
              update set x.start_timestamp = nvl(x.start_timestamp,p_start_timestamp),
                         x.end_timestamp   = nvl(x.end_timestamp,p_end_timestamp),
                         x.status          = p_status,
                         obfus_log_id      = p_obfus_log_id
          when not matched
          then
              insert (obfus_run_id,stage_step_code,stmt_seq,execution_id,start_timestamp,end_timestamp,status,obfus_log_id)
              values (p_obfus_run_id,p_stage_step_code,p_stmt_seq,p_execution_id,p_start_timestamp,p_end_timestamp,p_status,p_obfus_log_id);
      commit;

  end merge_obfus_ctrl_exec_result;

  procedure  obfus_log(p_log_msg VARCHAR2,p_src_prefix VARCHAR2,p_anon_version VARCHAR2,p_tgt_prefix VARCHAR2, p_code NUMBER,p_errm varchar2,p_module varchar2) is
   v_nLogID NUMBER;
  begin

    v_nLogID := obfus_log(p_log_msg ,p_src_prefix, p_anon_version , p_tgt_prefix , p_code ,p_errm ,p_module );

  end;


  function  obfus_log(p_log_msg VARCHAR2,p_src_prefix VARCHAR2,p_anon_version VARCHAR2,p_tgt_prefix VARCHAR2, p_code NUMBER,p_errm varchar2,p_module varchar2)  return number is
    pragma autonomous_transaction;

    v_nLogID NUMBER;
  begin

     v_nLogID := obfuscation_log_seq.nextval;
     insert into obfuscation_log (log_id,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module, mod_timestamp)
     values (v_nLogID ,p_log_msg ,p_src_prefix,p_anon_version,p_tgt_prefix,p_code,p_errm, p_module, systimestamp);
     commit;

     return v_nLogID;

  end obfus_log;


  function create_obfus_control
    return number is
     pragma autonomous_transaction;
     v_run_id_seq NUMBER := NULL;
     const_module  CONSTANT  varchar2(62) := 'obfuscation_control.create_obfus_control';
  begin
     begin
        insert into obfus_control(obfus_run_id,src_prefix,tgt_prefix,run_env,anon_version,obfus_status,
                                  setup_triggers,setup_synonyms,setup_indexes,setup_cheque_ranges,setup_stats,
                                  checked,run_start_time,run_completed_date,created_time,updated_time)
                          values (OBFUS_RUN_ID_SEQ.nextval,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version,'PENDING',
                                  'N','N','N','N','N',
                                  'N',null,null,sysdate,sysdate);
        commit;

        v_run_id_seq := OBFUS_RUN_ID_SEQ.currval;
        
        obfuscation_control.obfus_log('truncate_report_tables ready for new run_id: '||to_char(v_run_id_seq),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);      
        obfuscation_control.truncate_report_tables;
        
     exception
        when dup_val_on_index then
           select obfus_run_id
             into v_run_id_seq
             from obfus_control
            where src_prefix = gp.src_prefix
              and tgt_prefix = gp.tgt_prefix
              and run_env = gp.run_env;

           obfus_log('Obfuscation control record already exists for source: '|| gp.src_prefix || ' , target: ' || gp.tgt_prefix || ' ,run_env: ' || gp.run_env,
                      gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
     end;

     return v_run_id_seq;
  end create_obfus_control;


  function fn_existing_obfus_run_id (p_src_prefix in varchar2,p_run_env in varchar2, p_tgt_prefix in varchar2, p_obfus_run_id number)
    return number
  is
    v_code          number;
    v_errm          varchar2(4000);
    v_obfus_run_id  obfus_control.obfus_run_id%TYPE;
    const_module    CONSTANT  varchar2(62) := 'obfuscation_control.fn_existing_obfus_run_id';
  begin
    begin
        
      select obfus_run_id
        into v_obfus_run_id
        from obfus_control
       where src_prefix   = p_src_prefix
         and run_env      = p_run_env
         and tgt_prefix   = p_tgt_prefix
         and obfus_run_id = nvl(p_obfus_run_id,obfus_run_id);

    exception
      when no_data_found then
         v_obfus_run_id := null;
      when others then
         v_code := SQLCODE;
         v_errm := SUBSTR(SQLERRM,1,4000);
         obfuscation_control.obfus_log(const_module || ' error ',null,null,null,v_code,v_errm,const_module);
    end;
    obfuscation_control.obfus_log('Check for existing obfus_run_id returns: ' || to_char(v_obfus_run_id),p_src_prefix,null,p_tgt_prefix,SQLCODE,SQLERRM,const_module);
    
    return v_obfus_run_id;
  end fn_existing_obfus_run_id;


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
                                  p_rnd_data_generated    VARCHAR2 DEFAULT NULL)
  is
     pragma autonomous_transaction;
     const_module  CONSTANT  varchar2(62) := 'obfuscation_control.update_obfus_control';
  begin

     obfuscation_control.obfus_log('Updating obfus_control for p_obfus_run_id: '|| to_char(p_obfus_run_id) || ' and p_run_env: ' || p_run_env,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,const_module);

     update obfus_control
        set obfus_status         = nvl(p_obfus_status,obfus_status),
            setup_triggers       = nvl(p_setup_triggers,setup_triggers),
            setup_synonyms       = nvl(p_setup_synonyms,setup_synonyms),
            setup_indexes        = nvl(p_setup_indexes,setup_indexes),
            setup_cheque_ranges  = nvl(p_setup_cheque_ranges,setup_cheque_ranges),
            setup_stats          = nvl(p_setup_stats,setup_stats),
            checked              = nvl(p_checked,checked),
            peripheral_tables    = nvl(p_peripheral_tables,peripheral_tables),
            auto_stages_loaded   = nvl(p_auto_stages_loaded,auto_stages_loaded),
            environ_stages_loaded = nvl(p_environ_stages_loaded,environ_stages_loaded),
            manual_stages_loaded = nvl(p_manual_stages_loaded,manual_stages_loaded),
            per_stages_loaded    = nvl(p_per_stages_loaded,per_stages_loaded),
            final_stages_loaded  = nvl(p_final_stages_loaded,final_stages_loaded),
            stats_stmts_loaded   = nvl(p_stats_stmts_loaded,stats_stmts_loaded),
            pc_transform_loaded  = nvl(p_pc_transform_loaded,pc_transform_loaded),
            dd_loaded            = nvl(p_dd_loaded,dd_loaded),												  
            rnd_data_generated   = nvl(p_rnd_data_generated,rnd_data_generated),
            run_start_time       = DECODE(p_obfus_status,'RUNNING',sysdate,run_start_time),
            run_completed_date   = DECODE(p_obfus_status,'COMPLETED',sysdate,'RUNNING',null,'FAILED',null,run_completed_date),
            updated_time         = sysdate
      where obfus_run_id = p_obfus_run_id
        and src_prefix   = p_src_prefix
        and run_env      = p_run_env
        and anon_version = p_anon_version
        and tgt_prefix   = p_tgt_prefix;

     if sql%rowcount = 0 then
        obfus_log('No matching obfus_control record found to update',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,const_module);
     end if;
     obfuscation_control.obfus_log('Updated obfus_control',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,const_module);

     commit;
  end update_obfus_control;

  procedure insert_peripheral_tables( p_obfus_run_id NUMBER )
  is
     const_module  CONSTANT  varchar2(62) := 'obfuscation_control.insert_peripheral_tables';
     v_code        number;
     v_errm        varchar2(4000);
     v_ddl         varchar2(4000);
     v_nCount      number;

  begin

     v_ddl := 'alter table per_col_mask_overide disable constraint fk_pcmo_pt';
     obfuscation_control.obfus_log(v_ddl,gp.src_prefix,null,gp.tgt_prefix,null,null,const_module);
     execute immediate v_ddl;

      v_ddl := 'truncate table ' || gp.run_env || '.' || 'per_col_mask_overide';
     obfuscation_control.obfus_log(v_ddl,gp.src_prefix,null,gp.tgt_prefix,null,null,const_module);
     execute immediate v_ddl;

      v_ddl := 'truncate table ' || gp.run_env || '.' || 'peripheral_tables';
      obfuscation_control.obfus_log(v_ddl,gp.src_prefix,null,gp.tgt_prefix,null,null,const_module);
      execute immediate v_ddl;

      obfuscation_control.obfus_log('Inserting table peripheral_tables',gp.src_prefix,null,gp.tgt_prefix,null,null,const_module);

      -- Load Manually defined tables - (History + Others tables that have no masking requirement but still may require truncation)
      insert into peripheral_tables ( owner, table_name, table_type,related_owner,related_table_name,single_column, num_rows,load_mechanism )

      select ptl.owner, ptl.table_name table_name,ptl.table_type,ptl.related_owner,ptl.related_table_name,dtc.column_name single_column,dt.num_rows,const.k_PER_TABLE_LOAD_MECH_MAN load_mechanism
      from peripheral_tables_load  ptl
      join dd_tables dt on dt.owner = ptl.owner and dt.table_name = ptl.table_name
      join dd_tab_columns dtc on dt.owner = dtc.owner and dt.table_name = dtc.table_name and dtc.column_id = 1 ;

      commit;

--    Load Audit tables
      insert into peripheral_tables ( owner, table_name, table_type,related_owner,related_table_name,single_column, num_rows,load_mechanism )
      select dt.owner, dt.table_name table_name,const.k_PER_TABLE_AUD_TYPE,'' related_owner,'' related_table_name,dtc.column_name single_column,dt.num_rows,const.k_PER_TABLE_LOAD_MECH_AUTO load_mechanism
      from dd_tables dt
      join dd_tab_columns dtc on dt.owner = dtc.owner and dt.table_name = dtc.table_name and dtc.column_id = 1
      left outer join peripheral_tables pt on pt.owner = dt.owner and pt.table_name = dt.table_name
      where dt.owner = 'AUDIT' and (dt.table_name LIKE 'A\_%' escape const.k_escape or dt.table_name LIKE 'T\_%' escape const.k_escape )
      and pt.owner is null;

         commit;
--      -- Load Temp tables 1
      insert into peripheral_tables ( owner, table_name, table_type,related_owner,related_table_name,single_column, num_rows,load_mechanism )
      select dt.owner, dt.table_name,const.k_PER_TABLE_OTHER_TYPE,null related_owner,null related_table_name,dtc.column_name single_column,dt.num_rows,const.k_PER_TABLE_LOAD_MECH_AUTO load_mechanism
      from dd_tables dt
      join dd_tab_columns dtc on dt.owner = dtc.owner and dt.table_name = dtc.table_name and dtc.column_id = 1
      left outer join peripheral_tables pt on pt.owner = dt.owner and pt.table_name = dt.table_name
      where REGEXP_LIKE(dt.table_name, '^(RR){1}[0-9]{2,3}')
      and dt.table_name not like'%TYPE%'
      and dt.temporary <> 'Y'
      and pt.owner is  null;

      commit;
      -- Load Temp tables 2
      insert into peripheral_tables ( owner, table_name, table_type,related_owner,related_table_name,single_column, num_rows,load_mechanism )

      select dt.owner, dt.table_name,const.k_PER_TABLE_OTHER_TYPE,null related_owner,null related_table_name,dtc.column_name single_column,dt.num_rows,const.k_PER_TABLE_LOAD_MECH_AUTO load_mechanism
      from dd_tables dt
      join dd_tab_columns dtc on  dt.owner = dtc.owner and dt.table_name = dtc.table_name and dtc.column_id = 1
      left outer join peripheral_tables pt on pt.owner = dt.owner and pt.table_name = dt.table_name
       where REGEXP_LIKE(dt.table_name, '^(FR){1}[0-9]{2,3}')
         and dt.table_name not like'%TYPE%'
         and dt.temporary <> 'Y'
         and dt.table_name like '%\_TEMP%' escape const.k_escape
         and pt.owner is  null;

      commit;

      -- Load Other tables defined in PER_COL_MASK_OVERIDE_LOAD

      insert into peripheral_tables ( owner, table_name, table_type,related_owner,related_table_name,single_column, num_rows,load_mechanism )
      select distinct pcmol.owner, pcmol.table_name table_name,pcmol.table_type,dtc.column_name single_column,null related_owner,null related_table_name,dt.num_rows,const.k_PER_TABLE_LOAD_MECH_MAN load_mechanism
      from per_col_mask_overide_load  pcmol
      join dd_tables dt on dt.owner = pcmol.owner and dt.table_name = pcmol.table_name
      join dd_tab_columns dtc on  dt.owner = dtc.owner and dt.table_name = dtc.table_name and dtc.column_id = 1
      left outer join peripheral_tables pt on pt.owner = dt.owner and pt.table_name = dt.table_name
      where pt.owner is  null;

      commit;
      
      select count(*) into v_nCount from peripheral_tables;

      obfuscation_control.obfus_log(const_module||': '|| v_nCount|| ' rows inserted into peripheral_tables',gp.src_prefix,null,gp.tgt_prefix,v_code,v_errm,const_module);

      -- Load the per_col_mask_overide table

      obfuscation_control.obfus_log('Inserting table per_col_mask_overide',null,null,gp.tgt_prefix,null,null,const_module);

      insert into per_col_mask_overide(owner,table_name,column_name,table_type)
      select owner,table_name,column_name,table_type from per_col_mask_overide_load;

      obfuscation_control.obfus_log(const_module||': '|| SQL%ROWCOUNT || ' rows inserted into per_col_mask_overide',gp.src_prefix,null,gp.tgt_prefix,v_code,v_errm,const_module);

     commit;

     v_ddl := 'alter table per_col_mask_overide enable constraint fk_pcmo_pt';
     obfuscation_control.obfus_log(v_ddl,gp.src_prefix,null,gp.tgt_prefix,null,null,const_module);
     execute immediate v_ddl;

     obfuscation_control.update_obfus_control(p_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_peripheral_tables => 'Y');

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfuscation_control.obfus_log(const_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,gp.tgt_prefix,v_code,v_errm,const_module);

  end insert_peripheral_tables;

  procedure obfus_precheck is
     v_ddl VARCHAR2(4000);
     v_sql VARCHAR2(4000);
     v_code        number;
     v_errm        varchar2(4000);
     v_count integer;
     v_obfus_run_id obfus_control.obfus_run_id%TYPE;
     v_mig_version table_populated.mig_version%TYPE;
     const_module  CONSTANT  varchar2(62) :=  'obfuscation_control.obfus_precheck';

  begin

     v_obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,null);
     if v_obfus_run_id is null then
        v_obfus_run_id := obfuscation_control.create_obfus_control;
     end if;
     obfuscation_control.obfus_log('Running ' || const_module || ' with obfus_control.obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);

     obfus_log('Constructing v_mig_version by appending date string if gp.tgt_prefix is PSM',gp.src_prefix,gp.anon_version,gp.tgt_prefix,SQLCODE,SQLERRM,const_module);
     select case gp.tgt_prefix
            when 'PSM'
            then gp.tgt_prefix || '_'|| to_char(sysdate,'DDMONYYYY')
            else gp.tgt_prefix
            end case
      into v_mig_version
      from dual;

     obfus_log('Re-creating table_populated_bu',gp.src_prefix,gp.anon_version,gp.tgt_prefix,SQLCODE,SQLERRM,const_module);
     begin
        execute immediate 'drop table table_populated_bu purge';
     exception
        when others then
           null;
     end;

     execute immediate  'create table table_populated_bu as select * from table_populated';

     obfus_log('Updating table_populated',gp.src_prefix,gp.anon_version,gp.tgt_prefix,SQLCODE,SQLERRM,const_module);
     update table_populated
        set prev_populated = case when prev_populated ='Y' then 'Y'
                                  else populated end,
            prev_mig_version = mig_version, populated = null, mig_version = null;

     v_sql := 'update table_populated tp
                  set exist = ''N''
               where not exists ( select 1
                                    from dd_tab_columns dtc
                                    join dd_tables dt on dt.OWNER = dtc.OWNER and dt.TABLE_NAME = dtc.TABLE_NAME
                                   where dt.temporary =  ''N''
                                     and dtc.table_name = tp.table_name
                                     and dtc.column_name = tp.column_name )';
     --dbms_output.put_line( v_sql );
     execute immediate v_sql;

     obfus_log('Merging into table_populated',gp.src_prefix,gp.anon_version,gp.tgt_prefix,SQLCODE,SQLERRM,const_module);
     v_sql := 'merge into table_populated tgt
                  using (select dtc.owner,dtc.table_name,dtc.column_name,dtc.data_type,
                                case when dtc.num_distinct = 0 then ''N'' else ''Y'' end populated
                           from dd_tab_columns dtc
                           join dd_tables dt on dt.owner = dtc.owner and dt.table_name = dtc.table_name
                          where dt.temporary =  ''N'') res
                     on (tgt.owner = res.owner and  tgt.table_name = res.table_name and tgt.column_name = res.column_name)
                   when matched then update set tgt.populated = res.populated,
                                            new_col = ''N'',tgt.data_type = res.data_type,mig_version = '||''''||v_mig_version||''''||
                  ' when not matched then insert (owner,table_name, column_name,data_type,populated,new_col,mig_version)
                                          values (res.owner,res.table_name, res.column_name,res.data_type,res.populated,''Y'','''||v_mig_version||''')' ;

     --dbms_output.put_line( v_sql );
     execute immediate v_sql;

     commit;

     obfus_log('Re-creating table keys',gp.src_prefix,gp.anon_version,gp.tgt_prefix,SQLCODE,SQLERRM,const_module);
     begin
        execute immediate 'drop table keys purge';
     exception
        when others then
           null;
     end;

     v_ddl := 'create table keys as
                  select dcc.owner, dcc.table_name, dcc.column_name
                    from dd_cons_columns dcc
                    join dd_constraints dc on dc.owner = dcc.owner
                                            and dc.table_name = dcc.table_name
                                            and dc.constraint_name = dcc.constraint_name
                   where dc.constraint_type IN (''P'',''R'')';

     --dbms_output.put_line(v_ddl);
     execute immediate v_ddl;

     execute immediate 'create index keys_idx1 on keys(table_name,column_name)';

     obfus_log('Re-populating table obfus_pre_check',gp.src_prefix,gp.anon_version,gp.tgt_prefix,SQLCODE,SQLERRM,const_module);
     v_ddl := 'truncate table ' || gp.run_env || '.' || 'obfus_pre_check';
     execute immediate v_ddl;

     insert into obfus_pre_check(owner,table_name,column_name,populated,prev_populated,new_col,in_pc,mig_version,prev_mig_version,data_type,exist,notes)
           select tp.owner,tp.table_name,tp.column_name,tp.populated,tp.prev_populated,tp.new_col,
                   case when pc.column_name is null then 'N' else 'Y'||'('||pc.technique||')'  end  in_pc,tp.mig_version,tp.prev_mig_version,tp.data_type,tp.exist,
                   case when pt.table_name is not null then 'ok - truncated'
                        when tp.table_name like'%\_TYPE%'  escape '\' then 'ok - type table'
                        when keys.column_name is not null then 'ok - identifier'
                        when tp.column_name like'%\_ID%'  escape '\' then 'ok - identifier'
                        when tp.column_name like'%\_YN%' escape '\' then 'ok - flag'
                        when tp.column_name like'%\_CODE%'  escape '\'
                           and ( tp.column_name not like'%POST\_CODE%' escape '\' and tp.column_name not like'%POSTCODE%' )
                           and ( tp.column_name not like'%SORT\_CODE%' escape '\' and tp.column_name not like'%SORTCODE%' ) then 'ok - code'
                        when tp.column_name ='MODIFIED_BY' or tp.column_name ='CREATED_BY'  then 'ok - audit by'
                        when tp.column_name like'%DATE%' or tp.column_name like'%TIMESTAMP%'  then 'ok - dates'
                        when (tp.data_type = 'DATE' or tp.data_type like'TIMESTAMP%')  then 'ok - dates' end notes
            from table_populated tp
            left join pc_transform pc on pc.table_name = tp.table_name and pc.column_name = tp.column_name
            left join keys
               on keys.table_name = tp.table_name and keys.column_name = tp.column_name
            left join peripheral_tables pt on pt.table_name = tp.table_name and pt.owner = tp.owner
             where tp.table_name not like 'A\_%' escape const.k_escape;

     obfuscation_control.obfus_log(const_module||': '|| SQL%ROWCOUNT || ' rows inserted into obfus_pre_check',gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
     commit;

     update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_checked => 'Y');

  end obfus_precheck;

  procedure check_obfus_ready( p_obfus_run_id       IN OUT NUMBER,
                               p_src_prefix         IN OUT VARCHAR2,
                               p_tgt_prefix         IN OUT VARCHAR2,
                               p_run_env            IN OUT VARCHAR2,
                               p_anon_version       IN OUT VARCHAR2,
                               p_pre_check_required IN     VARCHAR2,
                               p_refresh_stats      IN     VARCHAR2 ) is
     const_module  CONSTANT   varchar2(62) := 'obfuscation_control.check_obfus_ready';
  begin

     select obfus_run_id, src_prefix, tgt_prefix, run_env, anon_version
       into p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version
       from (
              select obfus_run_id, src_prefix, tgt_prefix, run_env, anon_version
                from obfus_control
               where obfus_status IN ( 'PENDING', 'FAILED', 'COMPLETED', 'ON' ) -- allow recovery and re-runs of specified stage ranges
                 and setup_triggers = 'Y'
                 and setup_synonyms = 'Y'
                 and setup_indexes = 'Y'
                 and setup_cheque_ranges = 'Y'
                 and ( setup_stats = 'Y' or p_refresh_stats = 'N' )
                 and ( checked = 'Y' or p_pre_check_required = 'N' )
                 and src_prefix = nvl(p_src_prefix,src_prefix)       
                 and tgt_prefix = nvl(p_tgt_prefix,tgt_prefix)  
                 and run_env    = nvl(p_run_env,run_env) 
                 and anon_version = nvl(p_anon_version,anon_version)
                 and obfus_run_id = nvl(p_obfus_run_id,obfus_run_id)
              order by obfus_run_id desc
       )
      where rownum = 1;
  exception
     when no_data_found
     then
        p_obfus_run_id := null;
        p_src_prefix   := null;
        p_tgt_prefix   := null;
        p_run_env      := null;
        p_anon_version := null;
  end check_obfus_ready;


   procedure update_cheque_ranges (p_src_prefix in varchar2)
   is
      const_module  CONSTANT  varchar2(62) := 'obfuscation_control.update_cheque_ranges';
   begin

      obfus_log('UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES set end_no',p_src_prefix,null,null,null,null,const_module);
      execute immediate 'UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR
                            set cr.end_No= nvl((select cr1.start_no -1 from '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR1
                                                where cr1.bank_account_id = cr.bank_account_id and CR1.cheque_range_type_code = ''REISSUE''),999999)
                          where CR.cheque_range_type_code = ''AUTO''';

      obfus_log('UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES set start_no',p_src_prefix,null,null,null,null,const_module);
      execute immediate 'UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR
                            set cr.start_no = nvl((select cr1.end_no+1 from '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR1
                                                    where cr1.bank_account_id = cr.bank_account_id and CR1.cheque_range_type_code = ''MANUAL''),1)
                          where CR.cheque_range_type_code = ''AUTO''';

      commit;
   end update_cheque_ranges;


   procedure setup_synonyms_and_grants ( p_obfus_run_id IN OUT NUMBER )
   is

      v_obfus_run_id   obfus_control.obfus_run_id%TYPE := null;
      v_synonym_count  integer := 0;
      v_ddl            varchar2(4000);
      v_code           number;
      v_errm           varchar2(4000);

 cursor get_tabs(p_prefix VARCHAR2,p_escape VARCHAR2) is select replace(dt.owner,p_prefix||'_','') owner ,dt.owner actual_owner, dt.table_name
                                                              from all_tables dt
                                                             where ( iot_type IS NULL or iot_type != 'IOT_OVERFLOW' )
                                                               --ORA-25191: cannot reference overflow table of an index-organized table ANONPRE8201_INTEGRATION.SYS_IOT_OVER_2055452
                                                               -- This section needs to be removed - hardcoded references
                                                               and (    dt.table_name not like'RS2365_00000001_A_TMP_NT%'
                                                                    and dt.table_name not like'RS2365_00000001_B_TMP_NT%'
                                                                    and dt.table_name not like'RS2856_00000001_EXTRACT_TMPNT%' )
                                                               --22812. 00000 -  "cannot reference nested table column's storage table"
                                                               and (    dt.table_name not like 'DD\_%' escape '\'
                                                                    and dt.table_name not like 'MD\_%' escape '\'
                                                                    and dt.table_name not like 'SS\_%' escape '\' )
                                                               and  (NESTED <> 'YES')
                                                               and  (owner like p_prefix||'\_%' escape '\');


    cursor get_synonyms (p_env VARCHAR2,p_escape VARCHAR2) is
      select owner,synonym_name 
        from all_synonyms where owner = p_env;

    const_module  CONSTANT  varchar2(62) := 'obfuscation_control.setup_synonyms_and_grants';
  begin
     if p_obfus_run_id is not null
     then v_obfus_run_id := p_obfus_run_id;
     else
        v_obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,p_obfus_run_id);
        if v_obfus_run_id is null then
           v_obfus_run_id := obfuscation_control.create_obfus_control;
        end if;
     end if;
     obfuscation_control.obfus_log('Running ' || const_module || ' with obfus_control.obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);

     -- Clear Out all Synonyms in the run environment
     obfuscation_control.obfus_log('Dropping synonyms in run env: ' || gp.run_env,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       for get_synonyms_rec in get_synonyms(gp.run_env,const.k_escape) loop
         begin
           v_ddl := 'drop synonym  '||get_synonyms_rec.owner||'.'||get_synonyms_rec.synonym_name;
           execute immediate v_ddl;
           --dbms_output.put_line('drop synonym  '||get_synonyms_rec.owner||'.'||get_synonyms_rec.synonym_name);
         exception
            when excep.x_synonym_not_exist then
               null;
         end;
       end loop;

       -- Grant 'Select' on all tables in the source database
       -- Create synonyms for all tables in the source database
       obfuscation_control.obfus_log('Creating synonyms and grants for source env: ' || gp.src_prefix,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       for get_tabs_rec in get_tabs(gp.src_prefix,const.k_escape) loop
         begin

           v_ddl := 'create or replace synonym '||gp.run_env ||'.'||get_tabs_rec.table_name||' for '|| get_tabs_rec.actual_owner||'.'||get_tabs_rec.table_name;
           execute immediate v_ddl;

           v_synonym_count := v_synonym_count + 1;

           v_ddl := 'grant select on '||get_tabs_rec.actual_owner||'.'||get_tabs_rec.table_name||' to '|| gp.run_env;
           execute immediate v_ddl;

         exception
            when others then
               raise;
         end;
       end loop;

       -- Grant 'all' on all tables in the target database
       obfuscation_control.obfus_log('Granting all on tables in target env: ' || gp.tgt_prefix,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       for get_tabs_rec in get_tabs(gp.tgt_prefix,const.k_escape) loop
          begin
             v_ddl := 'grant all on '||get_tabs_rec.actual_owner||'.'||get_tabs_rec.table_name||' to '|| gp.run_env;
             execute immediate v_ddl;
         exception
            when others then
               raise;
         end;
       end loop;

       -- Create synonyms for all tables in the target database
       obfuscation_control.obfus_log('Creating synonyms for target env: ' || gp.src_prefix,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       for get_tabs_rec in get_tabs(gp.src_prefix,const.k_escape) loop
         begin
            v_ddl := 'create or replace synonym '||gp.run_env ||'.TGT_'||get_tabs_rec.table_name||' for '|| get_tabs_rec.actual_owner||'.'||get_tabs_rec.table_name;
            execute immediate v_ddl;
            v_synonym_count := v_synonym_count + 1;
         exception when others then
            raise;
         end;
       end loop;

       obfuscation_control.obfus_log('Granting select, update on '||gp.src_prefix||'_CASH_MANAGEMENT.cheque_ranges to '||gp.run_env,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       v_ddl := 'grant select, update on '||gp.src_prefix||'_CASH_MANAGEMENT.cheque_ranges to '||gp.run_env;
       execute immediate v_ddl;

       if v_synonym_count > 1 then
         obfuscation_control.update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_setup_synonyms => 'Y');
         obfuscation_control.obfus_log('setup completed successfully for ' || v_synonym_count || ' synonyms with obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       end if;

  exception
    when others then
       v_code := SQLCODE;
       v_errm := SUBSTR(SQLERRM,1,4000);
       obfuscation_control.obfus_log('Error: ' || v_ddl,gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
       raise;
  end setup_synonyms_and_grants;

  procedure setup_obfus_env ( p_obfus_run_id       IN OUT NUMBER,
                              p_triggers                  BOOLEAN  DEFAULT TRUE,
                              p_indexes                   BOOLEAN  DEFAULT TRUE,
                              p_cheque_ranges             BOOLEAN  DEFAULT TRUE,
                              p_stats                     BOOLEAN  DEFAULT TRUE,
                              p_stats_est_percent         INTEGER  DEFAULT 10,
                              p_days_since_last_analyzed  INTEGER  DEFAULT 1)
  as

    v_src_prefix       varchar2(50);  -- Prism Source Prefix
    v_tgt_prefix       varchar2(50);  -- Prism Target Prefix
    v_obfus_run_id     obfus_control.obfus_run_id%TYPE := null;

    v_code             number;
    v_errm             varchar2(4000);
    const_module       varchar2(62) := 'obfuscation_control.setup_obfus_env';

    v_trigger_count    integer := 0;
    v_synonym_count    integer := 0;
    v_index_count      integer := 0;
    v_indexes_dropped  integer := 0;
    v_stats_count      integer := 0;

    v_ddl              varchar2(4000);
    v_index_name       varchar2(128);

    cursor c_tgt_table_stats (p_days_since_last_analyzed INTEGER) is
       select distinct dt.actual_owner owner,dt.table_name
         from dd_tables dt
         join dd_tab_statistics dts on dt.owner = dts.owner
                                    and dt.table_name = dts.table_name
        where dt.table_name NOT LIKE'SYS_IOT%'
          and TRUNC(NVL(dts.last_analyzed,SYSDATE-(p_days_since_last_analyzed+1))) < TRUNC(SYSDATE-p_days_since_last_analyzed);

    -- get_indexes cursor must still use all_ dd views as the indexes are in the obfuscation run env
    cursor get_indexes (p_env VARCHAR2) is
       select atc.owner,atc.table_name,atc.column_name,
              row_number() over (partition by atc.table_name order by atc.table_name,atc.column_name) as iseq
         from all_tab_columns  atc
         join all_tables at on at.owner = atc.owner and at.table_name = atc.table_name
        where atc.owner = p_env
          and (atc.column_name like '%HASH%' or atc.column_name like '%KEY_NS%');

    cursor get_triggers (p_env VARCHAR2,p_escape VARCHAR2) is
      select owner,trigger_name from all_triggers where table_owner like p_env escape p_escape;

  begin

    v_tgt_prefix := gp.tgt_prefix || '\_%';
    v_src_prefix := gp.src_prefix || '\_%';

    if p_obfus_run_id is not null
    then v_obfus_run_id := p_obfus_run_id;
    else
      v_obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,p_obfus_run_id);
      if v_obfus_run_id is null then
         v_obfus_run_id := obfuscation_control.create_obfus_control;
      end if;
    end if;
    obfuscation_control.obfus_log('Running ' || const_module || ' with obfus_control.obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);

    if p_triggers then
      -- Disable all Triggers in the Target Environment
      obfuscation_control.obfus_log('opening get_triggers cursor with ' || ' v_tgt_prefix: ' || gp.tgt_prefix || ' const.k_escape: ' || const.k_escape,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
      for get_triggers_rec in get_triggers(v_tgt_prefix,const.k_escape) loop
        begin
           --obfuscation_control.obfus_log(const_module || ': Disabling trigger ' || get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name,gp.src_prefix,c,gp.tgt_prefix,null,null,const_module);
           v_ddl := 'alter trigger  '||get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name||' disable';
           execute immediate v_ddl;
           v_trigger_count := v_trigger_count + 1;
        exception when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM,1,4000);
           obfuscation_control.obfus_log('Error disabling trigger ' || get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name,gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
           raise;
        end;
      end loop;

      if v_trigger_count > 1 then
        obfuscation_control.update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_setup_triggers => 'Y');
        obfuscation_control.obfus_log(const_module || ' completed successfully for disabling ' || v_trigger_count || ' triggers with obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
      end if;
    end if; -- p_triggers


    if p_stats then
    -- Analyse Stats in the target

       obfuscation_control.obfus_log('opening c_tgt_table_stats cursor with p_days_since_last_analyzed: ' || p_days_since_last_analyzed,
                                      gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       for rec in c_tgt_table_stats(p_days_since_last_analyzed) loop
          begin

            dbms_stats.gather_table_stats(ownname => rec.owner,
                                          tabname => rec.table_name,
                                          estimate_percent => p_stats_est_percent );

            --calc_stats (rec.owner, rec.table_name, p_stats_est_percent);

            v_stats_count := v_stats_count + 1;
         exception when others then
            v_code := SQLCODE;
            v_errm := SUBSTR(SQLERRM,1,4000);
            obfuscation_control.obfus_log('Error gathering stats for '||rec.owner||'.'||rec.table_name,gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
         end;
       end loop;

       if v_stats_count > 1
       then
         obfuscation_control.load_dd_stats;
         obfuscation_control.update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_setup_stats => 'Y');
         obfuscation_control.obfus_log(const_module || ' completed successfully gathering stats for ' || v_stats_count || ' target tables with obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       end if;
    end if; -- p_stats

    if p_indexes then
       --
       --  -- Create indexes for all  HASH and KEY_NS columns in the run environment (ensure the table creation script is run first)
       --
       obfuscation_control.obfus_log('opening get_indexes cursor with ' || ' gp.run_env: ' || gp.run_env,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       for get_indexes_rec in get_indexes(gp.run_env) loop
          begin

             v_index_name := get_indexes_rec.owner||'.'||get_indexes_rec.table_name||'_'||RTRIM(LTRIM(NVL(substr(get_indexes_rec.column_name,11,20),substr(get_indexes_rec.column_name,1,10)),'_'),'_')||'_'||get_indexes_rec.iseq;

             begin
                v_ddl := 'drop index '||v_index_name;
                execute immediate v_ddl;
                v_indexes_dropped := v_indexes_dropped + 1;
             exception
               when excep.x_index_not_exist then
                 null;
             end;

             v_ddl := 'create index '||v_index_name||' on '||get_indexes_rec.owner||'.'||get_indexes_rec.table_name||'('|| get_indexes_rec.column_name||')';
             execute immediate v_ddl;
             v_index_count := v_index_count + 1;
          exception
             when excep.x_columns_already_indexed then
                v_index_count := v_index_count + 1;
                obfuscation_control.obfus_log('Error columns_already_indexed - index DDL: ' || v_ddl,gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
             when excep.x_idx_name_already_used then
                v_code := SQLCODE;
                v_errm := SUBSTR(SQLERRM,1,4000);
                obfuscation_control.obfus_log('Error idx_name_already_used - index DDL: '  || v_ddl,gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
             when others then
                v_code := SQLCODE;
                v_errm := SUBSTR(SQLERRM,1,4000);
                obfuscation_control.obfus_log('Error - index DDL: '  || v_ddl,gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
          end;
       end loop;

       if (v_indexes_dropped > 0) then
          obfuscation_control.obfus_log('Dropped ' || v_indexes_dropped || ' indexes with obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       end if;

       if (v_index_count > 1) then
          obfuscation_control.update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_setup_indexes => 'Y');
          obfuscation_control.obfus_log(const_module || ' completed successfully for creating ' || v_index_count || ' indexes with obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
       end if;
    end if; -- p_indexes


    if p_cheque_ranges then
      -- Cheque Range Adjustment Patch

      obfuscation_control.obfus_log(const_module || ': alter trigger '||gp.src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD disable',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
      execute immediate 'alter trigger '||gp.src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD disable';

      obfuscation_control.update_cheque_ranges(gp.src_prefix);

      obfuscation_control.obfus_log(const_module || ': alter trigger '||gp.src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD enable',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
      execute immediate 'alter trigger '||gp.src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD enable';

      obfuscation_control.update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_setup_cheque_ranges => 'Y');
      obfuscation_control.obfus_log(const_module || ' completed successfully for cheque_ranges with obfus_run_id = ' || v_obfus_run_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);

    end if; --p_cheque_ranges

    p_obfus_run_id := v_obfus_run_id;

  exception when others then
       v_code := SQLCODE;
       v_errm := SUBSTR(SQLERRM,1,4000);
       obfuscation_control.obfus_log('Error: ' || v_ddl,gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
       raise;

  end setup_obfus_env;

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
                        p_check_dependency    VARCHAR2 DEFAULT 'Y')  -- Only execute steps with dependency set to 'Y'
  is
    const_module      CONSTANT  varchar2(62) := 'obfuscation_control.obfuscate';
    
    v_obfus_run_id    obfus_control.obfus_run_id%TYPE := null;
    v_code            number;
    v_errm            varchar2(4000);
    rec_obfus_control obfus_control%ROWTYPE;
	
	  v_stage_tab  string_list_257;
    v_msg_tab  string_list_4000; 
	  v_col_mask_row_threshold number;

  begin

    gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(p_src_prefix,p_run_env,p_tgt_prefix,p_obfus_run_id);

	  begin
       select config_value into v_col_mask_row_threshold from system_configuration where config_type = const.k_const_CTYPE_COL_MASK_ROW_THRESHOLD;
       exception when others then
       v_col_mask_row_threshold := const.k_const_COL_MASK_ROW_THRESHOLD_DEF;
    end;
           
    gp.set_col_mask_row_threshold  (v_col_mask_row_threshold);           
    gp.set_src_prefix(p_src_prefix );
    gp.set_tgt_prefix(p_tgt_prefix) ;
    gp.set_run_env(p_run_env);
    gp.set_anon_version(p_anon_version);

    if gp.obfus_run_id IS NOT NULL
    then
       begin
          select * into rec_obfus_control from obfus_control  where obfus_run_id = gp.obfus_run_id;
       exception
         when no_data_found then
           RAISE_APPLICATION_ERROR(-20006,' obfus_run_id ' || gp.obfus_run_id || ' does not exist');
       end;
       if (    gp.src_prefix <> rec_obfus_control.src_prefix
            or gp.run_env    <> rec_obfus_control.run_env
            or gp.tgt_prefix <> rec_obfus_control.tgt_prefix )
       then
          --x_obfus_run_param_mismatch
          RAISE_APPLICATION_ERROR(-20005,'src, tgt or run_env does not match existing obfus_run_id.');
       end if;
    else   
      gp.obfus_run_id := obfuscation_control.create_obfus_control;
      --initialise
      obfuscation_control.obfus_log('initialising obfus_control record',gp.src_prefix,gp.anon_version,gp.tgt_prefix,SQLCODE,SQLERRM,const_module);
       begin
          select * into rec_obfus_control from obfus_control  where obfus_run_id = gp.obfus_run_id;
       exception
         when no_data_found then
           RAISE_APPLICATION_ERROR(-20006,' obfus_run_id ' || gp.obfus_run_id || ' does not exist');
       end;

--      rec_obfus_control.src_prefix := gp.src_prefix;
--      rec_obfus_control.tgt_prefix := gp.tgt_prefix;
--      rec_obfus_control.run_env := gp.run_env;
--      rec_obfus_control.anon_version := gp.anon_version; 
--      rec_obfus_control.setup_synonyms := 'N';
--      rec_obfus_control.setup_triggers := 'N';
--      rec_obfus_control.setup_indexes := 'N';
--      rec_obfus_control.setup_cheque_ranges := 'N';
--      rec_obfus_control.setup_stats := 'N';
--      rec_obfus_control.checked := 'N';
--      rec_obfus_control.peripheral_tables := 'N';
--      rec_obfus_control.environ_stages_loaded := 'N';
--      rec_obfus_control.auto_stages_loaded := 'N';
--      rec_obfus_control.manual_stages_loaded := 'N';
--      rec_obfus_control.per_stages_loaded := 'N';
--      rec_obfus_control.stats_stmts_loaded := 'N';
--      rec_obfus_control.rnd_data_generated := 'N';
--      rec_obfus_control.pc_transform_loaded := 'N';
--	    rec_obfus_control.dd_loaded := 'N';								 
--	    rec_obfus_control.final_stages_loaded := 'N';
									   
    end if;

	  if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.dd_loaded = 'N') then
          obfuscation_control.obfus_log('load_dd '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          load_dd;
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    -- SYNONYMS and GRANTS
    if can_continue then
      if gp.obfus_run_id is null OR (gp.obfus_run_id is not null and rec_obfus_control.setup_synonyms = 'N') then
        obfuscation_control.obfus_log('Calling setup_synonyms_and_grants for gp.obfus_run_id: ' || to_char(gp.obfus_run_id),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);      
        setup_synonyms_and_grants(gp.obfus_run_id);
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    -- TRIGGERS
    if can_continue then
      if gp.obfus_run_id is null OR (gp.obfus_run_id is not null and rec_obfus_control.setup_triggers = 'N') then
        obfuscation_control.obfus_log('Calling setup_obfus_env for triggers with gp.obfus_run_id: ' || to_char(gp.obfus_run_id),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);       
        setup_obfus_env(gp.obfus_run_id, p_triggers => TRUE, p_indexes => FALSE, p_cheque_ranges => FALSE, p_stats => FALSE );
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    -- INDEXES
    if can_continue then
      if gp.obfus_run_id is null OR (gp.obfus_run_id is not null and rec_obfus_control.setup_indexes = 'N') then
        obfuscation_control.obfus_log('Calling setup_obfus_env for indexes with gp.obfus_run_id: ' || to_char(gp.obfus_run_id),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);            
        setup_obfus_env(gp.obfus_run_id, p_triggers => FALSE, p_indexes => TRUE, p_cheque_ranges => FALSE, p_stats => FALSE );
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    -- CHEQUE RANGES
    if can_continue then
      if gp.obfus_run_id is null OR (gp.obfus_run_id is not null and rec_obfus_control.setup_cheque_ranges = 'N') then
        obfuscation_control.obfus_log('Calling setup_obfus_env for cheque_ranges with gp.obfus_run_id: ' || to_char(gp.obfus_run_id),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);                 
        setup_obfus_env(gp.obfus_run_id, p_triggers => FALSE, p_indexes => FALSE, p_cheque_ranges => TRUE, p_stats => FALSE );
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    -- STATS
    if can_continue then
      if p_refresh_stats = 'Y' then
        if gp.obfus_run_id is null OR (gp.obfus_run_id is not null and rec_obfus_control.setup_stats = 'N') then
          obfuscation_control.obfus_log('Calling setup_obfus_env for stats with gp.obfus_run_id: ' || to_char(gp.obfus_run_id),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);                        
          setup_obfus_env(gp.obfus_run_id, p_triggers => FALSE, p_indexes => FALSE, p_cheque_ranges => FALSE, p_stats => TRUE );
        end if;
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    --PERIPHERAL TABLES
    if can_continue then
      if gp.obfus_run_id is null OR (gp.obfus_run_id is not null and rec_obfus_control.peripheral_tables = 'N') then
        obfus_log('Calling insert_peripheral_tables with gp.obfus_run_id: ' || to_char(gp.obfus_run_id),gp.src_prefix,gp.anon_version,gp.tgt_prefix,SQLCODE,SQLERRM,const_module);
        insert_peripheral_tables(gp.obfus_run_id);
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    -- PRECHECK
    if p_pre_check_required = 'Y' then
      if can_continue then
        if gp.obfus_run_id is null OR (gp.obfus_run_id is not null and rec_obfus_control.checked = 'N') then
           obfus_precheck();
           obfuscation_control.switch_obfus_on_off('OFF',gp.obfus_run_id,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version);
        end if;
      else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;
      -- Switch obfuscation off to allow analysis of prechecks
    end if;

    if can_continue then
      obfuscation_control.check_obfus_ready(gp.obfus_run_id,rec_obfus_control.src_prefix,rec_obfus_control.tgt_prefix,rec_obfus_control.run_env,rec_obfus_control.anon_version,p_pre_check_required,p_refresh_stats);

      dbms_output.put_line('gp.obfus_run_id: ' || gp.obfus_run_id || ' rec_obfus_control.src_prefix: ' || rec_obfus_control.src_prefix || ' rec_obfus_control.tgt_prefix: ' || rec_obfus_control.tgt_prefix || ' rec_obfus_control.run_env: ' || rec_obfus_control.run_env || ' rec_obfus_control.anon_version: ' || rec_obfus_control.anon_version);

      if ( gp.obfus_run_id is null or rec_obfus_control.src_prefix is null or rec_obfus_control.tgt_prefix is null or rec_obfus_control.run_env is null or rec_obfus_control.anon_version is null )
      then
         obfuscation_control.obfus_log('RAISE excep.x_obfus_not_ready: ' || 'gp.obfus_run_id: ' || gp.obfus_run_id || ' rec_obfus_control.src_prefix: '|| rec_obfus_control.src_prefix || ' rec_obfus_control.tgt_prefix: ' || rec_obfus_control.tgt_prefix || ' rec_obfus_control.run_env: ' || rec_obfus_control.run_env || ' rec_obfus_control.anon_version: ' || rec_obfus_control.anon_version,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
         RAISE excep.x_obfus_not_ready;
      else
         obfuscation_control.update_obfus_control(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_obfus_status => 'RUNNING');
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.pc_transform_loaded = 'N') then
          obfuscation_control.obfus_log('load_pc_transform '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          load_pc_transform();
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;
	
    if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.environ_stages_loaded = 'N') then
          obfuscation_control.obfus_log('load_environ_stages '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          load_environ_stages();
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.auto_stages_loaded = 'N') then
          obfuscation_control.obfus_log('load_auto_stages '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          load_auto_stages(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version);
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.manual_stages_loaded = 'N') then
          obfuscation_control.obfus_log('load_manual_stages '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          load_manual_stages(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version);
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

    if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.per_stages_loaded = 'N') then
          obfuscation_control.obfus_log('load_per_trans_cols '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          load_per_trans_cols(gp.obfus_run_id,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version);
          obfuscation_control.obfus_log('load_per_stages '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          load_per_stages(gp.obfus_run_id,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version);
       end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

	if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.final_stages_loaded = 'N') then
          obfuscation_control.obfus_log('load_final_stages '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          load_final_stages(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version);
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;					
	if can_continue then
      if (gp.obfus_run_id is not null) then 
        obfuscation_control.obfus_log('check_stages '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
        If check_stages(gp.obfus_run_id,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version,v_stage_tab ,v_msg_tab) = const.k_Fail then  
          for  i IN  1..v_stage_tab.count loop
            if i = 1 then
              obfuscation_control.switch_obfus_on_off('OFF',gp.obfus_run_id,gp.src_prefix,gp.tgt_prefix,gp.run_env,gp.anon_version);
            end if;
            obfuscation_control.obfus_log('check_stages violation :'||' table ( '||v_stage_tab(i)||') and '||v_msg_tab(i) ,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          end loop;
        end if;
      end if;   
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;
    if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.stats_stmts_loaded = 'N') then
           obfuscation_control.obfus_log('load_stats_stmts '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
           load_stats_stmts(gp.obfus_run_id,gp.src_prefix,const.k_REP_SRC_SYN_PREFIX,const.k_REP_TGT_SYN_PREFIX, gp.tgt_prefix, gp.run_env, gp.anon_version);
      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

     if can_continue then
      if (gp.obfus_run_id is not null and rec_obfus_control.rnd_data_generated = 'N') then
          obfuscation_control.obfus_log('anonymisation_process.reset_schema '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          anonymisation_process.reset_schema;

          obfuscation_control.obfus_log('anonymisation_process.gen_rnd_notes '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          anonymisation_process.gen_rnd_notes;

          obfuscation_control.obfus_log('anonymisation_process.gen_rnd_addresses '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          anonymisation_process.gen_rnd_addresses;

          obfuscation_control.obfus_log('anonymisation_process.gen_rnd_names '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
          anonymisation_process.gen_rnd_names;

          update_obfus_control(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_rnd_data_generated => 'Y');

      end if;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;


   if can_continue then
      obfuscation_control.obfus_log('anonymisation_process.set_globals '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
      anonymisation_process.set_globals( gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version );
      
      obfuscation_control.obfus_log('init_audit_events '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
      obfuscation_control.init_audit_events;
      
      obfuscation_control.obfus_log('execute_obfus_steps '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
      execute_obfus_steps(p_start_step, p_end_step,p_check_dependency);

      obfuscation_control.obfus_log('apply_temp_patches '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
      anonymisation_process.apply_temp_patches;

      obfuscation_control.update_obfus_control(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_obfus_status => 'COMPLETED');
      obfuscation_control.obfus_log('Finish'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
   else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;

 exception
    when excep.x_obfus_not_ready then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      obfuscation_control.obfus_log('Obfuscation not ready to run: check obfus_control table. ' ||
                'The PRE, ANON and POST schema prefixes must be configured and PENDING to run with setup and checked flags Y',
                gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
      RAISE_APPLICATION_ERROR(-20001,'Obfuscation not ready to run: check obfus_control table.');
    when excep.x_cannot_continue
    then
       raise;
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
      obfuscation_control.update_obfus_control(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_obfus_status => 'FAILED');
 end obfuscate;


procedure execute_obfus_steps(p_start_step varchar2,p_end_step varchar2,p_check_dependency varchar2) as

cursor get_stmts(p_start_step varchar2,p_end_step varchar2) is
      select obfus_run_id,stage_step_code,dependent_ss_code,stmt_seq,step_type,stmt,stmt_overflow,stmt_overflow2,stmt_overflow3,
      stmt_overflow4,stmt_overflow5,stmt_overflow6,stmt_overflow7
      from obfus_ctrl_stmts
      where  TRANSLATE(stage_step_code,const.k_Stmt_Order_Chr,const.k_Stmt_Order_Num) >= NVL(TRANSLATE(p_start_step,const.k_Stmt_Order_Chr,const.k_Stmt_Order_Num),TRANSLATE(stage_step_code,const.k_Stmt_Order_Chr,const.k_Stmt_Order_Num))
      and  TRANSLATE(stage_step_code,const.k_Stmt_Order_Chr,const.k_Stmt_Order_Num) <=  NVL(TRANSLATE(p_end_step,const.k_Stmt_Order_Chr,const.k_Stmt_Order_Num),TRANSLATE(stage_step_code,const.k_Stmt_Order_Chr,const.k_Stmt_Order_Num))
      and  obfus_run_id = gp.obfus_run_id
      order by TRANSLATE(stage_step_code,const.k_Stmt_Order_Chr,const.k_Stmt_Order_Num),stmt_seq;

      v_tsStart TIMESTAMP;
      v_tsEnd TIMESTAMP;

      v_execution_id NUMBER;
      vLogID NUMBER;
      v_code NUMBER;
      v_errm  varchar2(4000);
      v_sStmt  varchar2(32676 );

      const_module  CONSTANT  varchar2(62) := 'obfuscation_control.execute_obfus_steps';

      v_sDependentStatus varchar2(10);
	  v_prev_stage_step_code  obfus_ctrl_stmts.stage_step_code%type;															

begin

  v_execution_id := execution_seq.nextval;
  gp.set_execution_id(v_execution_id);  								

  v_prev_stage_step_code := null;
  for get_stmts_rec in get_stmts(p_start_step, p_end_step) loop

    if substr(get_stmts_rec.stage_step_code,1,const.k_STAGE_CODE_SIZE)  <> substr(v_prev_stage_step_code,1,const.k_STAGE_CODE_SIZE) then
      commit;
    end if;
    if can_continue
    then
      begin

        --dbms_output.put_line('executing '||get_stmts_rec.stage_step_code ||' stmt_seq '||get_stmts_rec.stmt_seq );

        v_sStmt := get_stmts_rec.stmt;
        v_sStmt := v_sStmt || get_stmts_rec.stmt_overflow;
        v_sStmt := v_sStmt || get_stmts_rec.stmt_overflow2;
        v_sStmt := v_sStmt || get_stmts_rec.stmt_overflow3;
        v_sStmt := v_sStmt || get_stmts_rec.stmt_overflow4;
        v_sStmt := v_sStmt || get_stmts_rec.stmt_overflow5;
        v_sStmt := v_sStmt || get_stmts_rec.stmt_overflow6;
        v_sStmt := v_sStmt || get_stmts_rec.stmt_overflow7;

        -- Only attempt to execute if dependent stage_step code has been executed

        if get_stmts_rec.dependent_ss_code is not null then

          -- retrieve the dependent ss_code status

          begin
            select nvl(res.status,'NONE') status into v_sDependentStatus
            from
            dual left outer join
            (
              select ocer.status
              from obfus_control_exec_result ocer
              where ocer.stage_step_code = get_stmts_rec.dependent_ss_code
              and  ocer.obfus_run_id = gp.obfus_run_id
              and  execution_id = (select max(ocer1.execution_id)
                                    from obfus_control_exec_result ocer1
                                    where ocer1.stage_step_code = get_stmts_rec.dependent_ss_code
                                    and  ocer1.obfus_run_id = gp.obfus_run_id
                                    group by ocer1.stage_step_code,ocer1.obfus_run_id)
            ) res on 1 = 1 ;
            exception when no_data_found then
              v_sDependentStatus := 'NONE';
          end;
        else
          v_sDependentStatus := const.k_COMPLETED;
        end if;

        if p_check_dependency = 'N' or (p_check_dependency = 'Y' and  v_sDependentStatus = const.k_COMPLETED)  then
          case when get_stmts_rec.step_type = 'P' then

            v_tsStart := systimestamp;
            obfus_log('merge_obfus_ctrl_exec_result: v_execution_id: ' || v_execution_id,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
            merge_obfus_ctrl_exec_result(gp.obfus_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,v_execution_id,v_tsStart,null,const.k_STARTED,null);
            obfus_log('executing: '||get_stmts_rec.stage_step_code ||' stmt : ' ||substr(v_sStmt,1,4000 - length('executing: '||get_stmts_rec.stage_step_code ||' stmt : ')),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
            execute immediate 'begin '|| v_sStmt ||'; end;';
            v_tsEnd := systimestamp;
            merge_obfus_ctrl_exec_result(gp.obfus_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,v_execution_id,v_tsStart,v_tsEnd,const.k_COMPLETED,null);

          when get_stmts_rec.step_type = 'S' then

            v_tsStart := systimestamp;
            obfus_log('merge_obfus_ctrl_exec_result',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
            merge_obfus_ctrl_exec_result(gp.obfus_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,v_execution_id,v_tsStart,null,const.k_STARTED,null);
            obfus_log('executing: '||get_stmts_rec.stage_step_code ||' stmt : ' ||substr(v_sStmt,1,4000 - length('executing: '||get_stmts_rec.stage_step_code ||' stmt : ')),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
            execute immediate v_sStmt;
            v_tsEnd := systimestamp;

            merge_obfus_ctrl_exec_result(gp.obfus_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,v_execution_id,v_tsStart,v_tsEnd,const.k_COMPLETED,null);

          when get_stmts_rec.step_type = 'D' then

            v_tsStart := systimestamp;
            obfus_log('merge_obfus_ctrl_exec_result',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
            merge_obfus_ctrl_exec_result(gp.obfus_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,v_execution_id,v_tsStart,null,const.k_STARTED,null);
            obfus_log('executing: '||get_stmts_rec.stage_step_code ||' stmt : ' ||substr(v_sStmt,1,4000 - length('executing: '||get_stmts_rec.stage_step_code ||' stmt : ')),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);

            begin
               execute immediate v_sStmt;
            exception
               when excep.x_table_not_exist then null;
            end;

            v_tsEnd := systimestamp;

            merge_obfus_ctrl_exec_result(gp.obfus_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,v_execution_id,v_tsStart,v_tsEnd,const.k_COMPLETED,null);

          end case;
        else
            v_tsStart := systimestamp;
            v_tsEnd := systimestamp;

            merge_obfus_ctrl_exec_result(gp.obfus_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,v_execution_id,v_tsStart,v_tsEnd,const.k_DEP_IMCOMPLETE,null);

        end if;
  

      exception when others then

        v_tsEnd := systimestamp;

        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        vLogID := obfuscation_control.obfus_log(substr('Error: '||get_stmts_rec.stage_step_code ||': '|| dbms_utility.format_error_backtrace(),1,4000)
        ,gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);

        merge_obfus_ctrl_exec_result(gp.obfus_run_id,get_stmts_rec.stage_step_code,get_stmts_rec.stmt_seq,v_execution_id,v_tsStart,v_tsEnd,const.k_FAILED,vLogID);

        rollback;

      end;
    else RAISE_APPLICATION_ERROR(-20003, const.k_cant_continue_err_msg);  end if;
	v_prev_stage_step_code := get_stmts_rec.stage_step_code;														
  end loop;
   commit;

exception when others then
  v_code := SQLCODE;
  v_errm := SUBSTR(SQLERRM,1,4000);
  obfuscation_control.obfus_log('Error: ' ||'Unexpected',gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
  raise;
end execute_obfus_steps;

procedure load_environ_stages
as

  v_nStep number;
  v_nOrder number;

begin

  delete obfus_control_exec_result where obfus_run_id = gp.obfus_run_id and stage_step_code like 'E%';
  delete obfus_ctrl_stmts where obfus_run_id = gp.obfus_run_id and stage_step_code like 'E%';

--===================
--STEP 1 CREATE STAGE
--===================

  v_nStep := 1;
  v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select gp.obfus_run_id as obfus_run_id,
          'E'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          null  dependent_ss_code,
          'D' step_type,
          to_char(v_nOrder) as aorder ,
          owner,table_name,
          stmt
    from
    (
      select  owner,table_name,'DROP TABLE ' ||gp.run_env||'.'||'S_STG_'||res1.table_name stmt
      from
      (
          select distinct pc.owner,pc.table_name
          from pc_transform pc
      ) res1
    );

  v_nStep := 2;
  v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select gp.obfus_run_id as obfus_run_id,
      'E'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'E'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code ,
      'S' step_type,
      to_char(v_nOrder) as aorder ,
      owner,table_name,
      stmt
    from
    (
      select   owner,table_name,'CREATE TABLE ' ||gp.run_env||'.'|| 'S_STG_'||res1.table_name ||'( '||LISTAGG(res1.column_name ||' '||res1.data_type, ', ')  WITHIN GROUP (ORDER BY res1.table_name,res1.column_id)||')' stmt
      from
      (
        select
          dtc.owner,dtc.table_name,dtc.column_name,
          dtc.column_id,
          case when dtc.data_type in('VARCHAR2','CHAR') then 'VARCHAR2'||'('|| dtc.char_length ||' CHAR)' else null end ||
          case when dtc.data_type in('NUMBER','DATE') then dtc.data_type else null end data_type
        from
        (
          select dcc.owner,dcc.table_name,dcc.column_name
          from dd_cons_columns  dcc
          join dd_constraints dc on dc.owner = dcc.owner and dc.constraint_name = dcc.constraint_name
          where (dc.owner,dc.table_name) in (select distinct owner,table_name from pc_transform_2)
          and dc.constraint_type in ('P','U','R')

          union
          select pc.owner,pc.table_name,pc.column_name
          from pc_transform pc

          union
          select did.table_owner,did.table_name,did.column_name
          from dd_ind_columns did
          where (did.table_owner,did.table_name) in (select distinct owner,table_name from pc_transform_2)
          --and did.table_name not in ('HOLDER_ADDRESSES')
--          union
--          select  gp.src_prefix||'_'||sac.owner,sac.table_name,sac.column_name
--          from stg_additional_cols sac
        ) res
        join dd_tab_columns dtc on dtc.owner = res.owner and dtc.table_name = res.table_name  and dtc.column_name = res.column_name
      ) res1
      group by res1.owner,res1.table_name
    );

    v_nStep := 3;
    v_nOrder := 1;

   insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)

   select gp.obfus_run_id as obfus_run_id,
   'E'||LPAD(to_char(dense_rank() over (partition by 1 order by table_owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S' ||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
   'E'||LPAD(to_char(dense_rank() over (partition by 1 order by table_owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S' ||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
   'S' step_type,
   to_char(row_number() over (partition by table_name order by table_owner,table_name) -1) + v_nOrder  as aorder ,
   table_owner,table_name,
   nvl2(res.col_list,'CREATE INDEX ' ||res.index_name ||' on '||gp.run_env||'.'||'S_STG_'||res.table_name||'(' ||col_list||')',null)  stmt
   from
   (
      with ind_cols as (select dic.index_name ,dcc.owner,dcc.table_name,dcc.column_name,dic.column_position
                        from dd_cons_columns dcc 
                        join dd_constraints dc on dc.owner = dcc.owner and dc.constraint_name = dcc.constraint_name
                        join dd_ind_columns dic on dic.table_owner = dcc.owner and dic.table_name = dcc.table_name and dic.column_name = dcc.column_name and dic.index_name = dcc.constraint_name
                        and dic.column_name not like 'SYS' || '\_' || '%' escape '\'
                        and dc.constraint_type in ('P','U'))    
      select  pt2.table_owner,pt2.table_name ,ic.index_name,listagg(ic.column_name,',') within group (ORDER BY ic.column_position ) col_list
      from  (select distinct owner table_owner,table_name from pc_transform_2) pt2
      left outer join ind_cols ic on ic.owner = pt2.table_owner and ic.table_name = pt2.table_name
      group by pt2.table_owner,pt2.table_name ,ic.index_name   
    ) res;

     v_nStep := 4;
     v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select gp.obfus_run_id as obfus_run_id,
          'E'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
           'E'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(2),const.k_STEP_CODE_SIZE,'0') as  dependent_ss_code,
          'S' step_type,
          to_char(v_nOrder) as aorder ,
          owner,table_name,
          stmt
    from
    (
      select owner,table_name ,'CREATE OR REPLACE SYNONYM '||gp.run_env||'.'||'S_RPT_'||res1.table_name|| ' FOR '||gp.run_env||'.'||'S_STG_'||res1.table_name stmt
      from
      (
          select  distinct pc.owner,pc.table_name
          from pc_transform pc
      ) res1
    );


  v_nStep := 5;
  v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select gp.obfus_run_id as obfus_run_id,
          'E'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
           'E'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(2),const.k_STEP_CODE_SIZE,'0') as  dependent_ss_code,
          'S' step_type,
          to_char(v_nOrder) as aorder ,
          owner,table_name,
          stmt
    from
    (
      select owner,table_name ,'CREATE OR REPLACE SYNONYM  '||gp.run_env||'.'||'T_RPT_'||res1.table_name || ' FOR '||gp.tgt_prefix||'_'||res1.owner||'.'||res1.table_name stmt
      from
      (
          select  distinct pc.owner,pc.table_name
          from pc_transform pc
      ) res1
    );
 
   commit;

   update_obfus_control(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_environ_stages_loaded => 'Y');

end load_environ_stages;


  procedure load_final_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2)
as

  v_nStep number;
  v_nOrder number;
  v_nStage number;

begin

  delete obfus_control_exec_result where obfus_run_id = p_obfus_run_id and stage_step_code like 'F%';
  delete obfus_ctrl_stmts where obfus_run_id = p_obfus_run_id and stage_step_code like 'F%';


  v_nStep := 1;
  v_nOrder := 1;
  v_nStage :=  1;
  
     insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
              select p_obfus_run_id,
              'F'||LPAD(to_char(v_nStage),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code  ,
              null as   dependent_ss_code,
              'P' step_type,
              to_char(1) as stmt_seq ,
              null,null,
              p_run_env||'.'||'anonymisation_process.apply_temp_patches' stmt
              from dual ;

  
  v_nStep := 1;
  v_nOrder := 1;
  v_nStage :=  2;
  
     insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
              select p_obfus_run_id,
              'F'||LPAD(to_char(v_nStage),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code  ,
              null as   dependent_ss_code,
              'P' step_type,
              to_char(1) as stmt_seq ,
              null,null,
              p_run_env||'.'||'obfuscation_control.execution_report('|| p_obfus_run_id ||','''||p_src_prefix||''',''' ||p_tgt_prefix||''','''||p_run_env||''','''||p_anon_version||''')' stmt
              from dual ;
  
  v_nStep := 1;
  v_nOrder := 1;
  v_nStage :=  3;
  
     insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
              select p_obfus_run_id,
              'F'||LPAD(to_char(v_nStage),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code  ,
              null as   dependent_ss_code,
              'P' step_type,
              to_char(1) as stmt_seq ,
              null,null,
              p_run_env||'.'||'ut.enable_triggers' stmt
              from dual ;

  commit;

  update_obfus_control(p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_final_stages_loaded => 'Y');

end load_final_stages;


procedure load_auto_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2)

AS
  v_nStep number;
  v_nOrder number;

  v_sStmt1 varchar2(4000);
  v_sStmt2 varchar2(4000);
  v_sStmt3 varchar2(4000);
  v_sStmt4 varchar2(4000);
  const_module  CONSTANT  varchar2(62) := 'obfuscation_control.load_auto_stages';

  cursor  cGetMerge(p_src_prefix varchar2,p_nStep number) is
       with merge_parts as
        (select p_src_prefix||'_'||res.owner owner,res.table_name table_name, 'merge into tgt_'||res.table_name  ||' tgt using ' as merge_txt  ,
            --listagg(column_name, ', ') within group (ORDER BY column_name) as select_cols,
            listagg(res.trans_function ||' '||res.column_name , ', ') within group (order by res.column_name)  as trans_functions,
             table_name as select_table,
             col_group,
            ' when matched then update set ' || listagg('TGT.'||res.column_name ||'=' ||'SRC.'||res.column_name, ', ') within group (ORDER BY res.column_name)  as set_txt,
            max(res.col_group)over (partition by res.owner ,res.table_name) max_col_group,
            max(res.manual_only)over (partition by res.owner ,res.table_name) manual_only
        from
        (
            select  pct1.owner,pct1.table_name,pct1.column_name,
            pct1.trans_function,trunc((pct1.x_column_id - 1)/ (max(pct1.x_column_id) over (partition by pct1.owner,pct1.table_name) /2),0)  col_group,
            manual_only
            from
            (
              select pct.owner,pct.table_name,pct.column_name, pct.trans_function,
              row_number() over (partition by atc.owner,atc.table_name order by column_id) x_column_id,
              manual_only
              from pc_transform_2  pct
              join all_tab_columns atc on atc.owner = p_src_prefix||'_'||pct.owner and atc.table_name = pct.table_name  and atc.column_name = pct.column_name
              ) pct1
          ) res
        where res.column_name <> res.trans_function  group by res.owner,res.table_name,res.col_group,res.manual_only),

        pk as (select p_src_prefix||'_'||res.owner owner,res.table_name,
            ' on ('||listagg('tgt.'||res.column_name ||'=' ||'src.'||res.column_name, ' and ') within group (ORDER BY res.column_name)||')'  as on_txt,
            listagg(res.column_name,',') within group (ORDER BY res.column_name) as pk_cols
            from
            (

                  select pct1.owner,pct1.table_name,acc.column_name
                  from
                    (select distinct owner,table_name from pc_transform_2)  pct1
                    left outer join all_constraints ac  on  p_src_prefix||'_'||pct1.owner = ac.owner and pct1.table_name = ac.table_name and ac.constraint_type='P' and  NVL(ac.owner,p_src_prefix) like p_src_prefix||'\_'||'%' escape '\'
                    left outer join all_cons_columns  acc on ac.constraint_name = acc.constraint_name and ac.owner = acc.owner
                    where ac.constraint_type is not null
                  union

                  select pcc.owner owner,pcc.table_name,pcc.column_name
                  from (select distinct owner,table_name from pc_transform_2)  pct2
                  join  pseudo_cons_columns pcc  on pcc.owner = pct2.owner and  pcc.table_name = pct2.table_name

            ) res
            group by res.owner,res.table_name)
        select merge_parts.owner,merge_parts.table_name,merge_parts.select_table,merge_parts.merge_txt,merge_parts.set_txt,
        merge_parts.trans_functions,merge_parts.col_group,merge_parts.max_col_group,pk.on_txt,pk.pk_cols,
        'A'||LPAD(to_char(dense_rank() over (partition by 1 order by merge_parts.owner,merge_parts.table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(p_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
        'A'||LPAD(to_char(dense_rank() over (partition by 1 order by merge_parts.owner,merge_parts.table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(p_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
        manual_only
        from merge_parts,pk
        where merge_parts.owner = pk.owner and merge_parts.table_name = pk.table_name
        order by merge_parts.owner  ,merge_parts.table_name, merge_parts.col_group;


begin

  delete obfus_control_exec_result where obfus_run_id = p_obfus_run_id and stage_step_code like 'A%';
  delete obfus_ctrl_stmts where obfus_run_id = p_obfus_run_id and stage_step_code like 'A%';

    --==================
    --STEP  LOAD SOURCE
    --==================

  v_nStep := 1;
  v_nOrder := 1;

   insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)

   select p_obfus_run_id as obfus_run_id,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
       null as  dependent_ss_code,
      'S' step_type,
       to_char(v_nOrder) as aorder ,
       replace(owner,p_src_prefix||'_',null) owner,table_name,
       stmt
  from
  (

      select owner ,table_name,substr('INSERT INTO '||p_run_env||'.'||'S_STG_'||table_name||'('||acutal_cols||')  SELECT '||acutal_cols ||' FROM '||table_name,1,4000) || ' NOOGGING ' stmt

      from
      (
        select src.owner,src.table_name,listagg(src.column_name, ', ') within group (order by src.column_id) as acutal_cols
        from
        (
          select  atc.owner,atc.table_name,atc.column_name,atc.column_id
          from
          (
              select  acc.owner,acc.table_name,acc.column_name
              from all_cons_columns  acc
              join all_constraints ac on ac.owner = acc.owner and ac.constraint_name = acc.constraint_name
              where (ac.owner,ac.table_name) in (select distinct p_src_prefix||'_'||owner,table_name from pc_transform_2)
              and ac.constraint_type in ('P','U','R')
              union
              select  p_src_prefix||'_'||pc.owner,pc.table_name,pc.column_name
              from pc_transform pc
              union
              select  aid.table_owner,aid.table_name,aid.column_name
              from all_ind_columns aid
              where  (aid.table_owner,aid.table_name) in (select distinct p_src_prefix||'_'||owner,table_name from pc_transform_2)
--              union
--              select  p_src_prefix||'_'||sac.owner,sac.table_name,sac.column_name
--              from stg_additional_cols sac
          ) res
           join all_tab_columns atc on atc.owner = res.owner and atc.table_name = res.table_name  and atc.column_name = res.column_name

        ) src
        group by owner,table_name
      )
    );
	    
  --  --============================
  --  --STEP  - Commit
  -- --============================

  v_nStep := 2;
  v_nOrder := 1;
  
    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
      select p_obfus_run_id as obfus_run_id,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
      'P' step_type,
       to_char(v_nOrder) as aorder ,
       owner,table_name,
       'commit' stmt
       from
       (        
            select distinct pct.owner ,pct.table_name
            from pc_transform_2 pct
       );


  --  --============================
  --  --STEP  - Obfuscate to Target
  -- --============================

  v_nStep := 3;
  v_nOrder := 1;


    for cGetMergeRec in cGetMerge(p_src_prefix,v_nStep) loop


        if cGetMergeRec.max_col_group = 0 then
          v_sStmt1 := cGetMergeRec.merge_txt||' ( select '||cGetMergeRec.pk_cols||','||cGetMergeRec.trans_functions||' from '||cGetMergeRec.select_table||') src';
          v_sStmt2 := ' '||cGetMergeRec.on_txt||' '||cGetMergeRec.set_txt;
          v_sStmt3 := null;
          v_sStmt4 := null;

          insert into obfus_ctrl_stmts(obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt,stmt_overflow,stmt_overflow2,stmt_overflow3)
          values (p_obfus_run_id,cGetMergeRec.stage_step_code,cGetMergeRec.dependent_ss_code,'S' ,to_char(v_nOrder),replace(cGetMergeRec.owner,p_src_prefix||'_',null),cGetMergeRec.table_name,v_sStmt1,v_sStmt2,v_sStmt3,v_sStmt4);


        elsif cGetMergeRec.col_group = 0 then
          v_sStmt1 := cGetMergeRec.merge_txt||'( select '||cGetMergeRec.pk_cols||','||cGetMergeRec.trans_functions;
          v_sStmt3 := ' '||cGetMergeRec.on_txt||' '||cGetMergeRec.set_txt;
          null;
        elsif cGetMergeRec.col_group = 1 then
          v_sStmt2 := case when cGetMergeRec.trans_functions is not null then ','|| cGetMergeRec.trans_functions ||' from '||cGetMergeRec.select_table||') src' else null end;
          v_sStmt4 := null;

          insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt,stmt_overflow,stmt_overflow2,stmt_overflow3)
          values (p_obfus_run_id,cGetMergeRec.stage_step_code,cGetMergeRec.dependent_ss_code,'S' ,to_char(v_nOrder),replace(cGetMergeRec.owner,p_src_prefix||'_',null),cGetMergeRec.table_name,
          case when cGetMergeRec.manual_only = 'N' then v_sStmt1 else 'select 1 from dual' end,
          case when cGetMergeRec.manual_only = 'N' then v_sStmt2 else null end,
          case when cGetMergeRec.manual_only = 'N' then v_sStmt3 else null end,
          case when cGetMergeRec.manual_only = 'N' then v_sStmt4 else null end);

        end if;

    end loop;

    -- -- ====================
    ---- STEP  - Reports 1 - If there are any 'MAN' entries defer
    ---- ===================

  v_nStep := 4;
  v_nOrder := 1;

   insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
      select p_obfus_run_id as obfus_run_id,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
      'P' step_type,
       to_char(v_nOrder) as aorder ,
       owner,table_name,
       case when reportYN = 'Y' then
        p_run_env||'.'||'anonymisation_process.generate_table_stats('''||owner||''','''||table_name||''','''|| p_src_prefix ||''','||'SYSDATE'||','''|| p_anon_version ||''','||'1'||')'
       else
        'null'
       end stmt
       from
       (
          select distinct owner,table_name,'Y' reportYN
          from
          (
            select  pct.owner ,pct.table_name
            from pc_transform_2 pct
            minus
            select owner,table_name
            from obfus_ctrl_manual_config
            where enabled_YN = 'Y'
          )
          union
          select distinct owner,table_name,'N' reportYN
          from
          (
            select  pct.owner ,pct.table_name
            from pc_transform_2 pct
            intersect
            select owner,table_name
            from obfus_ctrl_manual_config
            where enabled_YN = 'Y'
          )


       );


  -- ====================
  -- STEP  - Fix anomalies
  -- ===================

  v_nStep := 5;
  v_nOrder := 1;


     insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
      select p_obfus_run_id as obfus_run_id,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
      'P' step_type,
       to_char(v_nOrder) as aorder ,
       owner,table_name,
       case when reportYN = 'Y' then
        p_run_env||'.'||'anonymisation_process.table_merge_fix_anomalies('''||owner||''','''||table_name||''','''|| p_src_prefix ||''','||'SYSDATE'||','''|| p_anon_version ||''')'
        else
        'null'
       end stmt

       from
       (
          select distinct owner,table_name,'Y' reportYN
          from
          (
            select  pct.owner ,pct.table_name
            from pc_transform_2 pct
            minus
            select owner,table_name
            from obfus_ctrl_manual_config
            where enabled_YN = 'Y'
          )
          union
          select distinct owner,table_name,'N' reportYN
          from
          (
            select  pct.owner ,pct.table_name
            from pc_transform_2 pct
            intersect
            select owner,table_name
            from obfus_ctrl_manual_config
            where enabled_YN = 'Y'
          )
       );
       

    ---- ====================
    ---- STEP  - Reports 2
    ---- ===================

  v_nStep := 6;
  v_nOrder := 1;


   insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
      select p_obfus_run_id as obfus_run_id,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
      'P' step_type,
       to_char(v_nOrder) as aorder ,
       owner,table_name,
       case when reportYN = 'Y' then
        p_run_env||'.'||'anonymisation_process.generate_table_stats('''||owner||''','''||table_name||''','''|| p_src_prefix ||''','||'SYSDATE'||','''|| p_anon_version ||''','||'2'||')'
       else
        'null'
       end stmt
       from
       (
          select distinct owner,table_name,'Y' reportYN
          from
          (
            select  pct.owner ,pct.table_name
            from pc_transform_2 pct
            minus
            select owner,table_name
            from obfus_ctrl_manual_config
            where enabled_YN = 'Y'
          )
          union
          select distinct owner,table_name,'N' reportYN
          from
          (
            select  pct.owner ,pct.table_name
            from pc_transform_2 pct
            intersect
            select owner,table_name
            from obfus_ctrl_manual_config
            where enabled_YN = 'Y'
          )
       );


    ---- ====================
    ---- STEP  - Exceptions Report (qa reports)
    ---- ===================

  v_nStep := 7;
  v_nOrder := 1;

     insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
      select p_obfus_run_id as obfus_run_id,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'A'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
      'P' step_type,
       to_char(v_nOrder) as aorder ,
       owner,table_name,
       case when reportYN = 'Y' then
         p_run_env||'.'||'anonymisation_process.generate_table_qa_reports('''||owner||''','''||table_name||''','''|| p_src_prefix ||''','||'SYSDATE'||','''|| p_anon_version ||
         ''','''|| const.k_REP_SRC_SYN_PREFIX
         ||''','''|| const.k_REP_TGT_SYN_PREFIX ||''')'
        else
          'null'
        end stmt
       from
       (
          select distinct owner,table_name,'Y' reportYN
          from
          (
            select  pct.owner ,pct.table_name
            from pc_transform_2 pct
            minus
            select owner,table_name
            from obfus_ctrl_manual_config
            where enabled_YN = 'Y'
          )
          union
          select distinct owner,table_name,'N' reportYN
          from
          (
            select  pct.owner ,pct.table_name
            from pc_transform_2 pct
            intersect
            select owner,table_name
            from obfus_ctrl_manual_config
            where enabled_YN = 'Y'
          )
       );



    commit;

    update_obfus_control(p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_auto_stages_loaded => 'Y');

end load_auto_stages;


procedure load_manual_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2)
AS

v_nStepOffset number;

begin

  delete obfus_control_exec_result where obfus_run_id = p_obfus_run_id and stage_step_code like 'M%';
  delete obfus_ctrl_stmts where obfus_run_id = p_obfus_run_id and stage_step_code like 'M%';

--===================
--LOAD MANUAl STAGES
--===================


  v_nStepOffset := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
      select p_obfus_run_id,
        'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
        null as   dependent_ss_code,
        'P' step_type,
        to_char(1) as stmt_seq ,
        owner,table_name,
         anon_proc
        from obfus_ctrl_manual_config
        where enabled_YN = 'Y';

    v_nStepOffset := 2;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
      select p_obfus_run_id,
        'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
        'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
        'P' step_type,
        to_char(1) as stmt_seq ,
        owner,table_name,
        merge_proc
        from obfus_ctrl_manual_config
        where enabled_YN = 'Y';


-- -- ====================
-- Reports 1
---- ===================

  v_nStepOffset := 3;

   insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
              select p_obfus_run_id,
              'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset),const.k_STEP_CODE_SIZE,'0') as   stage_step_code   ,
              'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset-1),const.k_STEP_CODE_SIZE,'0') as dependent_ss_code,
              'P' step_type,
              to_char(1) as stmt_seq ,
              owner,table_name,
              p_run_env||'.'||'anonymisation_process.generate_table_stats('''||owner||''','''||table_name||''','''|| p_src_prefix ||''','||'SYSDATE'||','''|| p_anon_version ||''','||'1'||')'  stmt
              from obfus_ctrl_manual_config ocmc
              where ocmc.enabled_YN = 'Y'
              order by ocmc.entity_group_id,ocmc.entity_order;

 -- ====================
-- Fix anomalies
-- ===================

  v_nStepOffset := 4;

   insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
              select p_obfus_run_id,
              'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset),const.k_STEP_CODE_SIZE,'0') as   stage_step_code   ,
              'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
              'P' step_type,
              to_char(1) as stmt_seq ,
              owner,table_name,
              p_run_env||'.'||'anonymisation_process.table_merge_fix_anomalies('''||owner||''','''||table_name||''','''|| p_src_prefix ||''','||'SYSDATE'||','''|| p_anon_version ||''')'  stmt
              from obfus_ctrl_manual_config ocmc
              where ocmc.enabled_YN = 'Y'
              order by ocmc.entity_group_id,ocmc.entity_order;

--
--
-- -- ====================
-- Reports 2
---- ===================
--
  v_nStepOffset := 5;

   insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
              select p_obfus_run_id,
              'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD( to_char(v_nStepOffset),const.k_STEP_CODE_SIZE,'0') as   stage_step_code   ,
              'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD( to_char(v_nStepOffset-1),const.k_STEP_CODE_SIZE,'0') as  dependent_ss_code,
              'P' step_type,
              to_char(1) as stmt_seq ,
              owner,table_name,
              p_run_env||'.'||'anonymisation_process.generate_table_stats('''||owner||''','''||table_name||''','''|| p_src_prefix ||''','||'SYSDATE'||','''|| p_anon_version ||''','||'2'||')'  stmt
              from obfus_ctrl_manual_config ocmc
              where ocmc.enabled_YN = 'Y'
              order by ocmc.entity_group_id,ocmc.entity_order;

--
--
-- -- ====================
--Exceptions Report (qa reports)
---- ===================
--
 v_nStepOffset := 6;

   insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
              select p_obfus_run_id,
              'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset),const.k_STEP_CODE_SIZE,'0') as   stage_step_code  ,
              'M'||LPAD(to_char(entity_order),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStepOffset-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
              'P' step_type,
              to_char(1) as stmt_seq ,
              owner,table_name,
              p_run_env||'.'||'anonymisation_process.generate_table_qa_reports('''||owner||''','''||table_name||''','''||
              p_src_prefix ||''','||'SYSDATE'||','''|| p_anon_version ||''','''|| const.k_REP_SRC_SYN_PREFIX
              ||''','''|| const.k_REP_TGT_SYN_PREFIX ||''')'  stmt
              from obfus_ctrl_manual_config ocmc
              where ocmc.enabled_YN = 'Y'
              order by ocmc.entity_group_id,ocmc.entity_order;


   commit;

   update_obfus_control(p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_manual_stages_loaded => 'Y');

end load_manual_stages;


procedure load_per_trans_cols(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2) is

  const_module      CONSTANT  varchar2(62) := 'obfuscation_control.load_Per_trans_cols';
  v_code       number;
  v_errm       varchar2(4000); 
  
  cursor c_Per_trans_col_con    
  is
      select ptc.owner, ptc.table_name, ptc.column_name, acc.owner constraint_owner, acc.constraint_name, 
             ac.constraint_type, ac.r_owner, ac.r_constraint_name,
             REPLACE(REPLACE(dbms_lob.substr(dbms_metadata.get_ddl('CONSTRAINT',acc.constraint_name,acc.owner),4000,1),'ENABLE','ENABLE NOVALIDATE'),'NOVALIDATE NOVALIDATE','NOVALIDATE') constraint_ddl
        from Per_trans_cols   ptc  
        left outer join all_cons_columns acc on p_src_prefix||'_'||ptc.owner = acc.owner and ptc.table_name = acc.table_name and ptc.column_name = acc.column_name
        left outer join all_constraints ac on ac.constraint_name = acc.constraint_name
       where ptc.transform <> 'NONE' and acc.column_name is not null;

  cursor c_Per_trans_col_ind
  is
      select ptc.owner, ptc.table_name, ptc.column_name, aic.index_owner, aic.index_name,
             dbms_lob.substr(dbms_metadata.get_ddl('INDEX',aic.index_name,aic.index_owner),4000,1) index_ddl
        from Per_trans_cols   ptc  
        left outer join all_ind_columns  aic on p_src_prefix||'_'||ptc.owner = aic.index_owner and ptc.table_name = aic.table_name and ptc.column_name = aic.column_name
       where ptc.transform <> 'NONE'  and aic.column_name is not null;

 begin
 

    delete Per_trans_col_ind;  
    delete Per_trans_col_con;
    delete Per_trans_cols;
  
      -- Load Audit table transformations

       insert into  Per_trans_cols (obfus_run_id ,owner , table_name, column_name, trans_func,transform,column_id,
                    data_type,data_length  ,data_precision  ,data_scale,nullable ,data_default,char_length)

         select  p_obfus_run_id obfus_run_id ,regexp_replace(tgt_owner,p_src_prefix||'_','',1,1) tgt_owner, tgt_table_name,  tgt_column_name,
          case when transform = 'Y' then
            case when atc.data_type in('VARCHAR2','CHAR') then 'ut.fn_char_mask(' else null end ||
            case when atc.data_type in('NUMBER') then 'ut.fn_number_mask(' else null end  ||
            case when atc.data_type in('DATE') then 'ut.fn_date_mask(' else null end   ||
            tgt_column_name ||')'
          else tgt_column_name end trans_func,
            case when transform = 'Y' and  atc.data_type in('VARCHAR2','CHAR') then '*'
                 when transform = 'Y' and  atc.data_type in('NUMBER') then '9'
                 when transform = 'Y' and  atc.data_type in('DATE') then '01/01/2099'
                 else 'NONE' end transform,
          atc.column_id,
          atc.data_type,atc.data_length  ,atc.data_precision  ,atc.data_scale,atc.nullable ,to_lob(atc.data_default) data_default,atc.char_length

      from
       (
         --  Masked Columns

         select
         res1.tgt_owner tgt_owner,res1.table_name tgt_table_name, res1.column_name tgt_column_name,
         'Y' transform
         from
         (
           (select p_src_prefix||'_AUDIT' tgt_owner,'A_'||table_name table_name,'O_'||column_name column_name
           from pc_transform_2 pc2
           where trans_function not like '%EXCLUDE%'
           union
           select p_src_prefix||'_AUDIT' tgt_owner,'A_'||table_name table_name,'N_'||column_name column_name
           from pc_transform_2 pc2
           where trans_function not like '%EXCLUDE%'
           union
           select p_src_prefix||'_AUDIT' owner,pcmo.table_name table_name,pcmo.column_name column_name
           from per_col_mask_overide pcmo
           join peripheral_tables pt on pt.owner = pcmo.owner and pt.table_name = pcmo.table_name
           where pt.table_type = const.k_PER_TABLE_AUD_TYPE)
           intersect
           select p_src_prefix||'_AUDIT' tgt_owner,atc.table_name,atc.column_name
           from all_tab_columns atc
           join peripheral_tables pt on p_src_prefix||'_'|| pt.owner = atc.owner and pt.table_name = atc.table_name
           where pt.table_type = const.k_PER_TABLE_AUD_TYPE
         ) res1
         union   all

         --  Non Masked Columns
         select res2.owner tgt_owner, res2.table_name tgt_table_name,res2.column_name tgt_column_name,'N' transform
         from
         (
             select atc.owner,atc.table_name,column_name
             from (select distinct p_src_prefix||'_AUDIT' tgt_owner,'A_'||table_name table_name from pc_transform_2
                   union
                   select distinct p_src_prefix||'_AUDIT' owner,pcmo.table_name table_name from per_col_mask_overide pcmo  
                   where pcmo.table_type = const.k_PER_TABLE_AUD_TYPE
                ) pc2
             join all_tab_columns atc on atc.owner = pc2.tgt_owner and  atc.table_name = pc2.table_name
             minus
            (select p_src_prefix||'_AUDIT' tgt_owner,'A_'||table_name table_name,'O_'||column_name column_name
             from pc_transform_2 pc2
             where trans_function not like '%EXCLUDE%'
             union
             select p_src_prefix||'_AUDIT' tgt_owner,'A_'||table_name table_name,'N_'||column_name column_name
             from pc_transform_2 pc2
             where trans_function not like '%EXCLUDE%'
             union
             select p_src_prefix||'_AUDIT' owner,pcmo.table_name table_name,pcmo.column_name column_name
             from per_col_mask_overide pcmo
             join peripheral_tables pt on pt.owner = pcmo.owner and pt.table_name = pcmo.table_name
             where pt.table_type = const.k_PER_TABLE_AUD_TYPE
             )
             intersect
             select p_src_prefix||'_AUDIT' tgt_owner,atc.table_name,atc.column_name
             from all_tab_columns atc
             join peripheral_tables pt on p_src_prefix||'_'|| pt.owner = atc.owner and pt.table_name = atc.table_name
             where pt.table_type = const.k_PER_TABLE_AUD_TYPE
        )res2

      ) res
      join all_tab_columns  atc on res.tgt_owner =  atc.owner and  res.tgt_table_name = atc.table_name and res.tgt_column_name = atc.column_name;

   -- Load History  table transformations

      insert into  Per_trans_cols (obfus_run_id ,owner , table_name, column_name, trans_func,transform,column_id,
                                   data_type,data_length  ,data_precision  ,data_scale,nullable ,data_default,char_length)


      select   p_obfus_run_id obfus_run_id , regexp_replace(tgt_owner,p_src_prefix||'_','',1,1) tgt_owner, tgt_table_name,  tgt_column_name,
          case when transform = 'Y' then
            case when atc.data_type in('VARCHAR2','CHAR') then 'ut.fn_char_mask(' else null end ||
            case when atc.data_type in('NUMBER') then 'ut.fn_number_mask(' else null end  ||
            case when atc.data_type in('DATE') then 'ut.fn_date_mask(' else null end   ||
            tgt_column_name ||')'
          else tgt_column_name end trans_func,
           case when transform = 'Y' and  atc.data_type in('VARCHAR2','CHAR') then '*'
                when transform = 'Y' and  atc.data_type in('NUMBER') then '9'
                when transform = 'Y' and  atc.data_type in('DATE') then '01/01/2099'
                else 'NONE' end transform,
          atc.column_id,atc.data_type,atc.data_length  ,atc.data_precision  ,atc.data_scale,atc.nullable ,to_lob(atc.data_default) data_default,atc.char_length

      from
(


         select
         res1.tgt_owner tgt_owner,res1.table_name tgt_table_name, res1.column_name tgt_column_name,
         'Y' transform
         from
         (
            (select p_src_prefix||'_'||pt.owner tgt_owner,pt.table_name table_name,pc2.column_name
            from pc_transform_2 pc2
            join peripheral_tables pt on pt.related_owner =  pc2.owner  and pt.related_table_name = pc2.table_name
            where pt.table_type = const.k_PER_TABLE_HIST_TYPE  and pc2.trans_function not like '%EXCLUDE%'
            union
            select  p_src_prefix||'_'||pt.owner owner,pt.table_name table_name,pcmo.column_name column_name
            from per_col_mask_overide pcmo
            join peripheral_tables pt on pt.related_owner = pt.owner and pt.related_table_name = pt.table_name
            where pt.table_type = const.k_PER_TABLE_HIST_TYPE )
            intersect
            select atc.owner tgt_owner,atc.table_name,atc.column_name
            from all_tab_columns atc
            join peripheral_tables pt on p_src_prefix||'_'|| pt.owner = atc.owner and pt.table_name = atc.table_name
            where pt.table_type = const.k_PER_TABLE_HIST_TYPE
         ) res1
         union   all

         -- Non Masked Columns
         select tgt_owner tgt_owner, res2.table_name tgt_table_name,res2.column_name tgt_column_name,'N' transform
         from
         (
            select atc.owner tgt_owner,atc.table_name table_name,atc.column_name
            from (select distinct owner,table_name from pc_transform_2) pc2
            join peripheral_tables pt on pt.related_owner = pc2.owner and pt.related_table_name = pc2.table_name
            join all_tab_columns atc on atc.owner =p_src_prefix||'_'||pt.owner  and atc.table_name = pt.table_name
            where pt.table_type = const.k_PER_TABLE_HIST_TYPE
             minus
             (
                 (select p_src_prefix||'_'||pt.owner tgt_owner,pt.table_name table_name,pc2.column_name
                  from pc_transform_2 pc2
                  join peripheral_tables pt on pt.related_owner =  pc2.owner  and pt.related_table_name = pc2.table_name
                  where pt.table_type = const.k_PER_TABLE_HIST_TYPE  and pc2.trans_function not like '%EXCLUDE%'
                  union
                  select  p_src_prefix||'_'||pt.owner owner,pt.table_name table_name,pcmo.column_name column_name
                  from per_col_mask_overide pcmo
                  join peripheral_tables pt on pt.related_owner = pt.owner and pt.related_table_name = pt.table_name
                  where pt.table_type = const.k_PER_TABLE_HIST_TYPE )
                  intersect
                  select atc.owner tgt_owner,atc.table_name,atc.column_name
                  from all_tab_columns atc
                  join peripheral_tables pt on p_src_prefix||'_'|| pt.owner = atc.owner and pt.table_name = atc.table_name
                  where pt.table_type = const.k_PER_TABLE_HIST_TYPE
            )

        )res2

      ) res
      join all_tab_columns  atc on res.tgt_owner =  atc.owner and  res.tgt_table_name = atc.table_name and res.tgt_column_name = atc.column_name;



    -- Other Per tables (using per_col_mask_overide only)

    insert into  Per_trans_cols (obfus_run_id ,owner , table_name, column_name, trans_func,transform,column_id,
    data_type,data_length  ,data_precision  ,data_scale,nullable ,data_default,char_length)

    select   p_obfus_run_id obfus_run_id ,regexp_replace(owner,p_src_prefix||'_','',1,1) owner,table_name,column_name,
          case when transform = 'Y' then
            case when atc.data_type in('VARCHAR2','CHAR') then 'ut.fn_char_mask(' else null end ||
            case when atc.data_type in('NUMBER') then 'ut.fn_number_mask(' else null end  ||
            case when atc.data_type in('DATE') then 'ut.fn_date_mask(' else null end   ||
            column_name ||')'
          else column_name end trans_func,
          case when transform = 'Y' and atc.data_type in('VARCHAR2','CHAR') then '*'
                when transform = 'Y' and  atc.data_type in('NUMBER') then '9'
                when transform = 'Y' and  atc.data_type in('DATE') then '01/01/2099'
                else 'NONE' end transform,
          atc.column_id,atc.data_type,atc.data_length  ,atc.data_precision  ,atc.data_scale,atc.nullable ,to_lob(atc.data_default) data_default,atc.char_length
    from
    (
      -- Masked Columns
      select p_src_prefix||'_'||pt.owner tgt_owner,pt.table_name tgt_table_name,pcmo.column_name tgt_column_name,'Y' transform
      from per_col_mask_overide pcmo
      join peripheral_tables pt on pt.owner = pcmo.owner and pt.table_name = pcmo.table_name
      where pt.table_type = const.k_PER_TABLE_OTHER_TYPE

      --Non Masked Columns
      union
      (
          select tgt_owner tgt_owner, tgt_table_name tgt_table_name,column_name tgt_column_name,'N' transform
          from
          (
            select atc.owner tgt_owner,atc.table_name tgt_table_name ,atc.column_name
            from (select distinct owner,table_name from per_col_mask_overide) pcmo
            join peripheral_tables pt on pcmo.owner = pt.owner and pcmo.table_name = pt.table_name
            join all_tab_columns atc on atc.owner =p_src_prefix||'_'||pt.owner  and atc.table_name = pt.table_name
            where pt.table_type = const.k_PER_TABLE_OTHER_TYPE

          minus
          (select atc.owner tgt_owner,atc.table_name tgt_table_name,atc.column_name column_name
          from per_col_mask_overide pcmo
          join peripheral_tables pt on pt.owner = pcmo.owner and pt.table_name = pcmo.table_name
          join all_tab_columns  atc on p_src_prefix||'_'||pcmo.owner =  atc.owner and  pcmo.table_name = atc.table_name and pcmo.column_name = atc.column_name
          where pt.table_type = const.k_PER_TABLE_OTHER_TYPE)
        )
      )
    ) res
    join all_tab_columns  atc on res.tgt_owner =  atc.owner and  res.tgt_table_name = atc.table_name and res.tgt_column_name = atc.column_name;
    

  begin
    for r in c_Per_trans_col_ind
    loop
      begin
        insert into Per_trans_col_ind (obfus_run_id,owner, table_name, column_name, index_owner, index_name, index_ddl)
        values (p_obfus_run_id,r.owner, r.table_name, r.column_name, r.index_owner, r.index_name,r.index_ddl);
      
      exception
        when dup_val_on_index then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          obfuscation_control.obfus_log('Index ' ||r.index_name|| ' already exists.',p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,const_module);
      end;
    end loop;  
  end;  
  
  begin
    for r in c_Per_trans_col_con
    loop
      begin
        insert into Per_trans_col_con ( obfus_run_id,owner, table_name, column_name, constraint_owner, constraint_name, 
                                        constraint_type, r_owner, r_constraint_name, constraint_ddl )  
        values (p_obfus_run_id,r.owner, r.table_name, r.column_name, r.constraint_owner, r.constraint_name, 
                r.constraint_type, r.r_owner, r.r_constraint_name,r.constraint_ddl);
      exception
        when dup_val_on_index then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          obfuscation_control.obfus_log('Constraint ' ||r.constraint_name|| ' already exists.',p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,const_module);
      end;                
    end loop;    
  end;
  merge into peripheral_tables pt
  using ( select owner,table_name, count(*) num_masked_cols
          from per_trans_cols
          where transform <> 'NONE'
          group by owner,table_name) res
  on (pt.owner = res.owner and pt.table_name = res.table_name)
  when matched then
    update set pt.num_masked_cols = res.num_masked_cols;

  update peripheral_tables pt set use_fast_mask = use_fast_mask(pt.owner,pt.table_name,p_src_prefix,p_anon_version,p_tgt_prefix);													   

  commit;
  
  exception when others then
    v_code := SQLCODE;
    v_errm := SUBSTR(SQLERRM,1,4000);
    obfuscation_control.obfus_log('Error: ' ||'Unexpected',p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,const_module);
    raise;
end load_per_trans_cols;


function use_fast_mask (p_owner varchar2, p_table_name varchar2,p_src_prefix varchar2,p_anon_version varchar2,p_tgt_prefix varchar2) return varchar2 is
  v_use_fast_mask varchar2(1);
  const_module      CONSTANT  varchar2(62) := 'obfuscation_control.use_fast_mask';
  v_code       number;
  v_errm       varchar2(4000); 
  
begin

  select case when num_rows > gp.col_mask_row_threshold then 'Y' else 'N' end
  into v_use_fast_mask
  from dd_tab_statistics
  where owner = p_owner and table_name = p_table_name;

  return v_use_fast_mask;
  
  exception when others then
    v_code := SQLCODE;
    v_errm := SUBSTR(SQLERRM,1,4000);
    obfuscation_control.obfus_log('Error: ' ||'Unexpected',p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,const_module);
    raise;
end use_fast_mask;


procedure load_per_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2)

AS
  v_nStep number;
  v_nOrder number;
  v_nStageOffset number;
  
  



      cursor genPerObfusCtrlStmts (p_obfus_run_id number,p_Step number) is select res.owner,res.table_name,col_group,
      listagg(res.column_name, ', ') within group (order by res.column_id) as acutal_cols ,
      listagg(res.trans_func, ', ') within group (order by res.column_id) as trans_cols ,
      'P'||LPAD(to_char(dense_rank() over (partition by 1 order by res.owner,res.table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(p_Step),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
      'P'||LPAD(to_char(dense_rank() over (partition by 1 order by res.owner,res.table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(p_Step-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code
      from
      (
        select ptc.owner,ptc.table_name,ptc.column_name,ptc.trans_func,ptc.column_id,
        trunc((ptc.column_id - 1)/ (max(ptc.column_id) over (partition by ptc.owner,ptc.table_name) /3),0)  col_group
        from Per_trans_cols ptc
        join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
        where ptc.obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'N'
      ) res group by res.owner,res.table_name,col_group order by res.owner,res.table_name,col_group;


      cursor genPerObfusCtrlStmts2 (p_obfus_run_id number,p_Step number) is select res.owner,res.table_name,col_group,
      listagg(res.column_name ||' '||res.data_type, ', ') within group (order by res.column_id) as create_cols ,
      'P'||LPAD(to_char(dense_rank() over (partition by 1 order by res.owner,res.table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(p_Step),const.k_STEP_CODE_SIZE,'0') as   stage_step_code,
      'P'||LPAD(to_char(dense_rank() over (partition by 1 order by res.owner,res.table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(p_Step-1),const.k_STEP_CODE_SIZE,'0') as dependent_ss_code
      from
      (
        select ptc.owner,ptc.table_name,ptc.column_name,
        case when atc.data_type in('VARCHAR2','CHAR') then 'VARCHAR2'||'('|| atc.char_length ||' CHAR)' else null end ||
        case when atc.data_type in('NUMBER','DATE','TIMESTAMP(6)','TIMESTAMP(6) WITH LOCAL TIME ZONE','XMLTYPE','CLOB') then atc.data_type else null end  data_type,
        ptc.column_id,trunc((ptc.column_id - 1)/ (max(ptc.column_id) over (partition by ptc.owner,ptc.table_name) /6),0) col_group
        from Per_trans_cols ptc
        join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
        join all_tab_columns atc on atc.owner = p_src_prefix||'_'||ptc.owner and atc.table_name = ptc.table_name  and atc.column_name = ptc.column_name
        where ptc.obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'N'
      ) res group by res.owner,res.table_name,col_group order by res.owner,res.table_name,col_group;

  v_sStmt1 varchar2(4000);
  v_sStmt2 varchar2(4000);
  v_sStmt3 varchar2(4000);
  v_sStmt4 varchar2(4000);
  v_sStmt5 varchar2(4000);
  v_sStmt6 varchar2(4000);
  v_sStmt7 varchar2(4000);
  v_sStmt8 varchar2(4000);

  v_current_stage_step_code    obfus_ctrl_stmts.stage_step_code%TYPE;
  v_current_dependent_ss_code  obfus_ctrl_stmts.dependent_ss_code%TYPE;
  v_current_owner              obfus_ctrl_stmts.owner%TYPE;
  v_current_table_name         obfus_ctrl_stmts.table_name%TYPE; 
  bln_stmt_inserted            BOOLEAN := FALSE; 
  v_counter                    number  := 0;

  const_module  CONSTANT  varchar2(62) := 'obfuscation_control.load_per_stages';

begin

  delete obfus_control_exec_result where obfus_run_id = p_obfus_run_id and stage_step_code like 'P%';
  delete from obfus_ctrl_stmts where obfus_run_id = p_obfus_run_id and stage_step_code like 'P%';

  --PERIFERAL TABLE PROCESSING
  
  
--    -- FAST MASK ENTITITIES
    v_nStageOffset := 0;
    v_nStep := 1;
    v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select p_obfus_run_id as obfus_run_id,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      null as  dependent_ss_code,
      'P' step_type,
      to_char(v_nOrder) as aorder ,
      owner,table_name,
      stmt
    from
    (
      select  owner,table_name,'anonymisation_process.apply_fast_mask('''||owner||''','''||table_name||''')'  stmt
      from
      (
        select  distinct ptc.owner,ptc.table_name 
        from Per_trans_cols  ptc
        join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
        where ptc.obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'Y'
      ) res
    );
    
    v_nStep := 2;
    v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select p_obfus_run_id as obfus_run_id,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name) + v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name) + v_nStageOffset ),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
      'P' step_type,
      to_char(v_nOrder) as aorder ,
      owner,table_name,
      stmt
    from
    (
      select   owner,table_name,p_run_env||'.'||'anonymisation_process.per_col_masking_exceptions('''||owner||''','''||table_name||''','''|| 'TGT' ||''')'  stmt
      from
      (
        select  distinct ptc.owner,ptc.table_name 
        from Per_trans_cols  ptc
        join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
        where ptc.obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'Y'
      ) res
    );

    commit;
    
    -- NON FAST MASK ENTITITIES
    v_nStep := 1;
    v_nOrder := 1;
    
    
    select max(to_number(substr(stage_step_code,2,const.k_STAGE_CODE_SIZE))) 
    into   v_nStageOffset
    from obfus_ctrl_stmts 
    where obfus_run_id =  p_obfus_run_id
    and substr(stage_step_code,1,1) = 'P' ;
    
    obfuscation_control.obfus_log('Processing NON FAST MASK ENTITITIES using v_nStageOffset: '||to_char(v_nStageOffset),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
																															

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select p_obfus_run_id as obfus_run_id,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      null   dependent_ss_code,
      'D' step_type,
      to_char(v_nOrder) as aorder ,
      owner,table_name,
      stmt
    from
    (
      select   owner,table_name,'DROP TABLE ' ||p_run_env||'.'|| 'S_STG_'||res.table_name  stmt
      from
      (
        select  distinct ptc.owner,ptc.table_name from Per_trans_cols  ptc
        join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
        where ptc.obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'N'
      ) res
    );

    obfuscation_control.obfus_log('Inserted: '||to_char(sql%rowcount) || ' drop S_STG table statments',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);


   v_nStep := 2;
   v_nOrder := 1;
   v_counter := 0;

    obfuscation_control.obfus_log('Opening genPerObfusCtrlStmts2 cursor for CREATE S_STG_ table statements, with obfus_run_id: '||to_char(p_obfus_run_id) || ' for v_nStep ' || to_char(v_nStep),gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
    for genPerObfusCtrlStmts2Rec in genPerObfusCtrlStmts2(p_obfus_run_id,v_nStep) loop
         
      if v_counter = 0
      then
         obfuscation_control.obfus_log('Initialising v_current_stage_step_code to : '||genPerObfusCtrlStmts2Rec.stage_step_code,gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
         v_current_stage_step_code := genPerObfusCtrlStmts2Rec.stage_step_code;
         v_counter := v_counter + 1;
      end if;
      
      if genPerObfusCtrlStmts2Rec.stage_step_code <> v_current_stage_step_code
      then 
      
        -- exception case where fewer than 5 col groups ('SIP_CASH_RECON_TEMP')
         if not bln_stmt_inserted 
         then
            obfuscation_control.obfus_log('Inserting obfus_ctrl_stmts for: ' || v_current_table_name ||
                                          ' for stage_step_code: ' || v_current_stage_step_code,
                                           gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
         
            insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt,stmt_overflow,stmt_overflow2,stmt_overflow3,stmt_overflow4,stmt_overflow5,stmt_overflow6,stmt_overflow7)
            values (p_obfus_run_id, v_current_stage_step_code, v_current_dependent_ss_code,'S' ,to_char(v_nOrder),
                    v_current_owner,v_current_table_name,v_sStmt1,v_sStmt2,v_sStmt3,v_sStmt4,v_sStmt5,v_sStmt6,null,null);
         end if;
         
         -- reset for next stage_step_code
         bln_stmt_inserted := FALSE;         
      end if;

      if genPerObfusCtrlStmts2Rec.col_group = 0 then
        v_sStmt1 := 'CREATE TABLE ' ||p_run_env||'.'|| 'S_STG_'||genPerObfusCtrlStmts2Rec.table_name ||'( '||genPerObfusCtrlStmts2Rec.create_cols;
      end if;

      if genPerObfusCtrlStmts2Rec.col_group = 1 then
        v_sStmt2 := case when genPerObfusCtrlStmts2Rec.create_cols is not null then ','||genPerObfusCtrlStmts2Rec.create_cols else null end;
      end if;

      if genPerObfusCtrlStmts2Rec.col_group = 2 then
        v_sStmt3 :=  case when genPerObfusCtrlStmts2Rec.create_cols is not null then ','||genPerObfusCtrlStmts2Rec.create_cols else null end;
      end if;

      if genPerObfusCtrlStmts2Rec.col_group = 3 then
        v_sStmt4 :=  case when genPerObfusCtrlStmts2Rec.create_cols is not null then ','||genPerObfusCtrlStmts2Rec.create_cols else null end;
      end if;

      if genPerObfusCtrlStmts2Rec.col_group = 4 then
        v_sStmt5 :=  case when genPerObfusCtrlStmts2Rec.create_cols is not null then ','||genPerObfusCtrlStmts2Rec.create_cols else null end;
      end if;

      if genPerObfusCtrlStmts2Rec.col_group = 5 then

        v_sStmt6 := case when genPerObfusCtrlStmts2Rec.create_cols is not null then ','||genPerObfusCtrlStmts2Rec.create_cols else null end ||') ';

        insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt,stmt_overflow,stmt_overflow2,stmt_overflow3,stmt_overflow4,stmt_overflow5,stmt_overflow6,stmt_overflow7)
        values (p_obfus_run_id,genPerObfusCtrlStmts2Rec.stage_step_code,genPerObfusCtrlStmts2Rec.dependent_ss_code,'S' ,to_char(v_nOrder),genPerObfusCtrlStmts2Rec.owner,genPerObfusCtrlStmts2Rec.table_name,v_sStmt1,v_sStmt2,v_sStmt3,v_sStmt4,v_sStmt5,v_sStmt6,null,null);
        bln_stmt_inserted := TRUE;
      end if;

      v_current_stage_step_code   := genPerObfusCtrlStmts2Rec.stage_step_code;
      v_current_dependent_ss_code := genPerObfusCtrlStmts2Rec.dependent_ss_code;
      v_current_owner             := genPerObfusCtrlStmts2Rec.owner;
      v_current_table_name        := genPerObfusCtrlStmts2Rec.table_name;

    end loop;

    v_nStep := 3;
    v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select p_obfus_run_id as obfus_run_id,
          'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
          'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as  dependent_ss_code,
          'D' step_type,
          to_char(v_nOrder) as aorder ,
          owner,table_name,
          stmt
    from
    (
      select owner,table_name ,'CREATE OR REPLACE SYNONYM '||p_run_env||'.'||const.k_REP_TGT_SYN_PREFIX||'_'||res1.table_name || ' FOR '||p_tgt_prefix||'_'||res1.owner||'.'||res1.table_name stmt
      from
      (
          select  distinct ptc.owner,ptc.table_name 
          from Per_trans_cols ptc
          join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
          where obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'N'
      ) res1
    );

    v_nStep := 4;
    v_nOrder := 1;

    for genPerObfusCtrlStmtsRec in genPerObfusCtrlStmts(p_obfus_run_id,v_nStep) loop

      if genPerObfusCtrlStmtsRec.col_group = 0 then
        v_sStmt1 := ' INSERT  INTO '||p_run_env||'.'|| 'S_STG_'||genPerObfusCtrlStmtsRec.table_name||'('||genPerObfusCtrlStmtsRec.acutal_cols;
        v_sStmt4 :=   'SELECT '||genPerObfusCtrlStmtsRec.trans_cols  ;

      end if;

      if genPerObfusCtrlStmtsRec.col_group = 1 then
        v_sStmt2 := case when genPerObfusCtrlStmtsRec.acutal_cols is not null then ',' end||genPerObfusCtrlStmtsRec.acutal_cols;
        v_sStmt5 := case when genPerObfusCtrlStmtsRec.trans_cols is not null then ',' end ||genPerObfusCtrlStmtsRec.trans_cols;
      end if;


      if genPerObfusCtrlStmtsRec.col_group = 2 then
        v_sStmt3 := case when genPerObfusCtrlStmtsRec.acutal_cols is not null then ',' end ||genPerObfusCtrlStmtsRec.acutal_cols ||') ';
        v_sStmt6 := case when genPerObfusCtrlStmtsRec.trans_cols is not null then ',' end ||genPerObfusCtrlStmtsRec.trans_cols || ' FROM '||p_src_prefix||'_'||genPerObfusCtrlStmtsRec.owner||'.'||genPerObfusCtrlStmtsRec.table_name || ' NOOGGING ';

       insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt,stmt_overflow,stmt_overflow2,stmt_overflow3,stmt_overflow4,stmt_overflow5)
       values (p_obfus_run_id,genPerObfusCtrlStmtsRec.stage_step_code,genPerObfusCtrlStmtsRec.dependent_ss_code,'S' ,to_char(v_nOrder),genPerObfusCtrlStmtsRec.owner,genPerObfusCtrlStmtsRec.table_name,v_sStmt1,v_sStmt2,v_sStmt3,v_sStmt4,v_sStmt5,v_sStmt6);

      end if;

    end loop;

    v_nStep := 5;
    v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select p_obfus_run_id as obfus_run_id,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as  dependent_ss_code,
      'P' step_type,
      to_char(v_nOrder) as aorder ,
      owner,table_name,
      stmt
    from
    (
      select  owner,table_name,'ut.truncate_table_new('''||p_tgt_prefix||'_'||owner||''','''||table_name||''','''||temporary||''')'  stmt
      from
      (
        select distinct ptc.owner,ptc.table_name,at.temporary
          from Per_trans_cols  ptc
          join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
          join all_tables at on p_tgt_prefix||'_'||ptc.owner = at.owner and ptc.table_name = at.table_name
          where ptc.obfus_run_id = p_obfus_run_id  and pt.use_fast_mask = 'N'
      ) res
    );

    v_nStep := 6;
    v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select p_obfus_run_id as obfus_run_id,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as  dependent_ss_code,
      'P' step_type,
      to_char(v_nOrder) as aorder ,
      owner,table_name,
      stmt
    from
    (
      select  owner,table_name,'ut.rebuild_indexes('''||p_tgt_prefix||'_'||owner||''','''||table_name||''')'  stmt
      from
      (
        select  distinct ptc.owner,ptc.table_name 
        from Per_trans_cols  ptc
        join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
        where ptc.obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'N'
      ) res
    );

   v_nStep := 7;
   v_nOrder := 1;

    for genPerObfusCtrlStmtsRec in genPerObfusCtrlStmts(p_obfus_run_id,v_nStep) loop

      if genPerObfusCtrlStmtsRec.col_group = 0 then
        v_sStmt1 := ' INSERT  INTO '||p_tgt_prefix||'_'||genPerObfusCtrlStmtsRec.owner ||'.'||genPerObfusCtrlStmtsRec.table_name||'('|| genPerObfusCtrlStmtsRec.acutal_cols;
        v_sStmt4 := ' SELECT '||genPerObfusCtrlStmtsRec.trans_cols  ;
      end if;

      if genPerObfusCtrlStmtsRec.col_group = 1 then
        v_sStmt2 :=  case when genPerObfusCtrlStmtsRec.acutal_cols is not null then ',' end||genPerObfusCtrlStmtsRec.acutal_cols;
        v_sStmt5 :=  case when genPerObfusCtrlStmtsRec.trans_cols is not null then ',' end ||genPerObfusCtrlStmtsRec.trans_cols  ;
      end if;

      if genPerObfusCtrlStmtsRec.col_group = 2 then
        v_sStmt3 := case when genPerObfusCtrlStmtsRec.acutal_cols is not null then ',' end ||genPerObfusCtrlStmtsRec.acutal_cols ||') ';
        v_sStmt6 :=  case when genPerObfusCtrlStmtsRec.trans_cols is not null then ',' end || genPerObfusCtrlStmtsRec.trans_cols ||' FROM '||p_run_env||'.'|| 'S_STG_'||genPerObfusCtrlStmtsRec.table_name || ' NOOGGING ';

        insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt,stmt_overflow,stmt_overflow2,stmt_overflow3,stmt_overflow4,stmt_overflow5)
        values (p_obfus_run_id,genPerObfusCtrlStmtsRec.stage_step_code,genPerObfusCtrlStmtsRec.dependent_ss_code,'S' ,to_char(v_nOrder),genPerObfusCtrlStmtsRec.owner,genPerObfusCtrlStmtsRec.table_name,v_sStmt1,v_sStmt2,v_sStmt3,v_sStmt4,v_sStmt5,v_sStmt6);

      end if;

    end loop;


    v_nStep := 8;
    v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select p_obfus_run_id as obfus_run_id,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
      'D' step_type,
      to_char(v_nOrder) as aorder ,
      owner,table_name,
      stmt
    from
    (
      select   owner,table_name,'DROP TABLE ' ||p_run_env||'.'|| 'S_STG_'||res.table_name  stmt
      from
      (
        select  distinct ptc.owner,ptc.table_name 
        from Per_trans_cols  ptc
        join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
        where ptc.obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'N'
      ) res
    );

    v_nStep := 9;
    v_nOrder := 1;

    insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,table_name,stmt)
    select p_obfus_run_id as obfus_run_id,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep),const.k_STEP_CODE_SIZE,'0') as   stage_step_code ,
      'P'||LPAD(to_char(row_number() over (partition by 1 order by owner,table_name)+ v_nStageOffset),const.k_STAGE_CODE_SIZE,'0')||'S'||LPAD(to_char(v_nStep-1),const.k_STEP_CODE_SIZE,'0') as   dependent_ss_code,
      'P' step_type,
      to_char(v_nOrder) as aorder ,
      owner,table_name,
      stmt
    from
    (
      select   owner,table_name,p_run_env||'.'||'anonymisation_process.per_col_masking_exceptions('''||owner||''','''||table_name||''','''|| 'TGT'||''')'  stmt
      from
      (
        select  distinct ptc.owner,ptc.table_name 
        from Per_trans_cols  ptc
        join peripheral_tables pt on pt.owner = ptc.owner and pt.table_name = ptc.table_name
        where ptc.obfus_run_id = p_obfus_run_id and pt.use_fast_mask = 'N'
      ) res
    );

    commit;
    update_obfus_control(p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_per_stages_loaded => 'Y');

end load_per_stages;


 procedure load_stats_stmts(p_obfus_run_id number,p_src_prefix varchar2,p_src_rep_syn_prefix varchar2,p_tgt_rep_syn_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2) is
    const_module      CONSTANT  varchar2(62) := 'obfuscation_control.load_stats_stmts';
    v_ddl varchar2(4000);
 begin
 
    obfuscation_control.obfus_log('truncating tables tmp_tab_pk, tmp_pc_tab_pk, stats_stmts'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),p_src_prefix,p_anon_version,p_tgt_prefix,null,null,const_module);
    execute immediate 'truncate table stats_stmts';
    execute immediate 'truncate table tmp_tab_pk';
    execute immediate 'truncate table tmp_pc_tab_pk';
 
    insert into tmp_tab_pk ( owner, table_name, pkcols, pkjoin )
      select ac.owner,ac.table_name,
             listagg(acc.column_name,',') within group (ORDER BY column_name) as pkcols,
             listagg('x.'||acc.column_name ||'=' ||'y.'||acc.column_name, ' and ') within group (ORDER BY column_name)  as pkjoin
        from all_constraints  ac
        join all_cons_columns acc on ac.constraint_name = acc.constraint_name and ac.owner = acc.owner
       where ac.constraint_type = 'P' and  ac.owner like p_src_prefix||'\_%' escape '\'
      group by ac.owner,ac.table_name;

    obfuscation_control.obfus_log(sql%rowcount || ' rows inserted into tmp_tab_pk',p_src_prefix,p_anon_version,p_tgt_prefix,null,null,const_module);
    
    update tmp_tab_pk
       set leading_pkcol = nvl(substr(pkcols,1,instr(pkcols,',')-1),pkcols);
 
    obfuscation_control.obfus_log(sql%rowcount || ' tmp_tab_pk rows updated',p_src_prefix,p_anon_version,p_tgt_prefix,null,null,const_module); 
   
    insert into tmp_pc_tab_pk     
      select
         distinct pt.owner, pt.table_name, pt.column_name, pt.technique,
                  pt.trans_function, pt.stereo_type, pk.pkcols, pk.leading_pkcol, pk.pkjoin
             from pc_transform pt 
             join TMP_TAB_PK pk on pk.table_name = pt.table_name
              and pk.owner = p_src_prefix ||'_'|| pt.owner;   

    obfuscation_control.obfus_log(sql%rowcount || ' rows inserted into tmp_pc_tab_pk',p_src_prefix,p_anon_version,p_tgt_prefix,null,null,const_module);
    
    insert into stats_stmts (owner,table_name,column_name,technique,trans_function,stereo_type,stmt)
       select t.owner, t.table_name, t.column_name, t.technique, t.trans_function, t.stereo_type,     
     ' select sum(ne)                 not_equals, 
              sum(eq)                 equals, 
              sum(value_to_null)      value_to_null, 
              sum(value_from_null)    value_from_null, 
              round(avg(dist_sim),2)  avg_sim_dist, 
              sum(src_rec)            total_recs_src, 
              sum(tgt_rec)            total_recs_tgt,
              sum(null_src)           total_nulls_src,
              sum(null_tgt)           total_nulls_tgt
         from (' ||
             'select case when x.' ||t.column_name || ' <> y.' ||t.column_name || '
                          then 1 else 0
                     end  AS ne,
                     case when x.' ||t.column_name || ' = y.' ||t.column_name || '
                          then 1 else 0
                     end  AS eq,
                     case when x.' ||t.column_name || ' is not null and y.' ||t.column_name || ' is null 
                          then 1 else 0
                     end  AS value_to_null, 
                     case when x.' ||t.column_name || ' is null and y.' ||t.column_name || ' is not null  
                          then 1 else 0
                     end  AS value_from_null, 
                     case when x.' ||t.column_name || ' is not null and y.' ||t.column_name || ' is not null                    
                          then 
                            utl_match.EDIT_DISTANCE_SIMILARITY(substr(x.' ||t.column_name || ',1,20) , substr(y.' ||t.column_name||',1,20)) 
                          else
                            null
                     end AS dist_sim,
                     1 AS src_rec,
                     1 AS tgt_rec,
                     case when x.' ||t.column_name || ' is null
                          then 1 else 0
                     end  AS null_src,      
                     case when y.' ||t.column_name || ' is null
                          then 1 else 0
                     end  AS null_tgt                  
                    from ' || p_src_rep_syn_prefix ||'_'|| t.table_name || ' x 
                    full outer join ' || p_tgt_rep_syn_prefix || '_' || t.table_name || ' y on ' || t.pkjoin || ')'
     from tmp_pc_tab_pk t;

     obfuscation_control.obfus_log(sql%rowcount || ' rows inserted into stats_stmts',p_src_prefix,p_anon_version,p_tgt_prefix,null,null,const_module);
 
     update_obfus_control(p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_stats_stmts_loaded => 'Y');
     commit;

 end load_stats_stmts;


 procedure load_dd_stats as

  start_time date;
  end_time date;

  const_module      CONSTANT  varchar2(62) := 'obfuscation_control.load_dd_stats';
  v_code           number;
  v_errm           varchar2(4000);

  v_obfus_run_id    obfus_control.obfus_run_id%TYPE := null;

  begin
  
    obfuscation_control.obfus_log('truncating tables dd_tab_columns, dd_tab_statistics and dd_tab_col_statistics',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);   
    execute immediate 'truncate table dd_tab_columns';  
    execute immediate 'truncate table dd_tab_statistics';
    execute immediate 'truncate table dd_tab_col_statistics';

    insert into dd_tab_columns( actual_owner,owner, table_name,column_name,data_type,data_type_mod,
                data_type_owner,data_length,data_precision,data_scale,nullable,column_id,default_length,num_distinct,       
                density,num_nulls,num_buckets,last_analyzed,sample_size,character_set_name,  
                char_col_decl_length,global_stats,user_stats,avg_col_len,char_length)
    select owner actual_owner,replace(owner,gp.src_prefix||'_',null) owner, table_name,column_name,data_type,data_type_mod,
           data_type_owner,data_length,data_precision,data_scale,nullable,column_id,default_length,num_distinct,       
           density,num_nulls,num_buckets,last_analyzed,sample_size,character_set_name,  
           char_col_decl_length,global_stats,user_stats,avg_col_len,char_length 
      from all_tab_columns
     where owner like gp.src_prefix||'\_%' escape '\';

    obfuscation_control.obfus_log(to_char(sql%rowcount) || ' rows inserted into dd_tab_columns',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);


    insert into dd_tab_statistics(owner,actual_owner,table_name,partition_name,partition_position,subpartition_name,
                subpartition_position,object_type,num_rows,blocks,empty_blocks,avg_space,chain_cnt,avg_row_len,
                avg_space_freelist_blocks,num_freelist_blocks,avg_cached_blocks,avg_cache_hit_ratio,im_imcu_count,
                im_block_count,im_stat_update_time,scan_rate,sample_size,last_analyzed,global_stats,user_stats,
                stattype_locked,stale_stats,scope)
     select  replace(owner,gp.src_prefix||'_',null) owner,owner actual_owner,	table_name,partition_name,
                partition_position,subpartition_name,subpartition_position,object_type,num_rows,blocks,
                empty_blocks,avg_space,chain_cnt,avg_row_len,avg_space_freelist_blocks,num_freelist_blocks,
                avg_cached_blocks,avg_cache_hit_ratio,im_imcu_count,im_block_count,
                im_stat_update_time,scan_rate,sample_size,last_analyzed,global_stats,
                user_stats,stattype_locked,stale_stats,scope  
      from all_tab_statistics
     where owner like gp.src_prefix||'\_%' escape '\'
       and object_type = 'TABLE';

    obfuscation_control.obfus_log(to_char(sql%rowcount) || ' rows inserted into dd_tab_statistics',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);     
                
    insert into  dd_tab_col_statistics( owner,  actual_owner,table_name,column_name,num_distinct,low_value,high_value,density , 	
                 num_nulls,num_buckets,last_analyzed,sample_size,global_stats,user_stats,notes,avg_col_len,histogram ,scope )
    select replace(owner,gp.src_prefix||'_',null)  owner,owner actual_owner,table_name,column_name,num_distinct,low_value,high_value,density,num_nulls,num_buckets,
           last_analyzed,sample_size,global_stats,user_stats,notes,avg_col_len,histogram,scope 
      from all_tab_col_statistics
     where owner like gp.src_prefix||'\_%' escape '\';

    obfuscation_control.obfus_log(to_char(sql%rowcount) || ' rows inserted into dd_tab_col_statistics',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
     
    commit;

 exception
    when others then
       v_code := SQLCODE;
       v_errm := SUBSTR(SQLERRM,1,4000);
       obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
       obfuscation_control.update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_dd_loaded => 'FAILED');
    
 end load_dd_stats;

 procedure load_dd as

  start_time date;
  end_time date;

  const_module      CONSTANT  varchar2(62) := 'obfuscation_control.load_dd';
  v_code           number;
  v_errm           varchar2(4000);

  v_obfus_run_id    obfus_control.obfus_run_id%TYPE := null;

  begin
  
    obfuscation_control.obfus_log('truncating  dd_* tables',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);     
    execute immediate 'truncate table dd_tables';
    execute immediate 'truncate table dd_tab_columns';
    execute immediate 'truncate table dd_tab_statistics';
    execute immediate 'truncate table dd_tab_col_statistics';
    execute immediate 'truncate table dd_constraints';
    execute immediate 'truncate table dd_cons_columns';
    execute immediate 'truncate table dd_ind_columns';
    --execute immediate 'truncate table dd_synonyms';
    --execute immediate 'truncate table dd_triggers';
    
    v_obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,null);
    begin
    
    insert into dd_tables (actual_owner, owner, table_name,tablespace_name,temporary,num_rows,iot_type) 
      select owner actual_owner, replace(owner,gp.src_prefix||'_',null) owner, table_name,tablespace_name,temporary,num_rows,iot_type
        from all_tables 
       where owner like gp.src_prefix||'\_%' escape '\';

    obfuscation_control.obfus_log(to_char(sql%rowcount) || ' rows inserted into dd_tables',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);
        
    load_dd_stats;
    
    insert into dd_constraints (
       actual_owner,owner,constraint_name,constraint_type,table_name,
       actual_r_owner, r_owner,r_constraint_name,
       actual_index_owner,index_owner,index_name    
    ) 
    select owner actual_owner, replace(owner,gp.src_prefix||'_',null) owner, constraint_name,constraint_type,table_name,
           r_owner actual_r_owner, replace(r_owner,gp.src_prefix||'_',null) r_owner, r_constraint_name, 
           index_owner actual_index_owner, replace(index_owner,gp.src_prefix||'_',null) index_owner,index_name 
      from all_constraints 
     where owner like gp.src_prefix||'\_%' escape '\';

    obfuscation_control.obfus_log(to_char(sql%rowcount) || ' rows inserted into dd_constraints',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);

    insert into dd_cons_columns (
       actual_owner,owner,constraint_name,table_name,column_name,position
    )
    select owner actual_owner, replace(owner,gp.src_prefix||'_',null) owner, constraint_name, table_name, column_name, position
      from all_cons_columns 
     where owner like gp.src_prefix||'\_%' escape '\';

    obfuscation_control.obfus_log(to_char(sql%rowcount) || ' rows inserted into dd_cons_columns',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);

    insert into dd_ind_columns (
       actual_index_owner,index_owner,index_name,
       actual_table_owner,table_owner,table_name,
       column_name,column_position,column_length,char_length
    ) 
    select index_owner actual_index_owner, replace(index_owner,gp.src_prefix||'_',null) index_owner, index_name,  
           table_owner actual_table_owner, replace(table_owner,gp.src_prefix||'_',null) table_owner, table_name, 
           column_name, column_position, column_length, char_length
      from all_ind_columns 
     where index_owner like gp.src_prefix||'\_%' escape '\'; 

    obfuscation_control.obfus_log(to_char(sql%rowcount) || ' rows inserted into dd_ind_columns',gp.src_prefix,gp.anon_version,gp.tgt_prefix,null,null,const_module);

--    insert into dd_synonyms (
--       actual_owner,owner,synonym_name,
--       actual_table_owner,table_owner,table_name    
--    )
--    select owner actual_owner, replace(owner,gp.src_prefix||'_',null) owner, synonym_name, 
--           table_owner actual_table_owner, replace(table_owner,gp.src_prefix||'_',null) table_owner, table_name
--      from all_synonyms 
--     where owner like gp.src_prefix||'\_%' escape '\'; 

--    insert into dd_triggers (
--       actual_owner,owner,trigger_name,trigger_type,triggering_event,
--       actual_table_owner,table_owner,table_name,column_name,status
--    )
--    select owner actual_owner, replace(owner,gp.src_prefix||'_',null) owner, trigger_name, trigger_type, triggering_event, 
--           table_owner actual_table_owner, replace(table_owner,gp.src_prefix||'_',null) table_owner, table_name, column_name, status
--      from all_triggers 
--     where owner like gp.src_prefix||'\_%' escape '\';

    update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_dd_loaded => 'Y');
    
    commit;


    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
          obfuscation_control.update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_dd_loaded => 'FAILED');
    end;
end load_dd;

 procedure load_pc_transform as

  start_time date;
  end_time date;

  v_prefix VARCHAR2(20);

  cursor getDistFuncs(p_funcs varchar2) is
  select distinct regexp_substr(p_funcs,'[^,]+', 1, level) func
  from dual
  connect by regexp_substr(p_funcs, '[^,]+', 1, level) is not null;

  cursor getItems(p_items varchar2) is
  select  regexp_substr(p_items,'[^,]+', 1, level) item
  from dual
  connect by regexp_substr(p_items, '[^,]+', 1, level) is not null;

  l_trans_function  varchar2(4000);
  l_needs_substr varchar2(10);
  l_length varchar2(10);

  const_module      CONSTANT  varchar2(62) := 'obfuscation_control.load_pc_transform';
  v_code           number;
  v_errm           varchar2(4000);

  v_obfus_run_id    obfus_control.obfus_run_id%TYPE := null;

  begin

    v_obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,null);
    begin
 										  
      execute immediate ('truncate table pc_transform_2');

      for pc_rec in (               
                      select owner,table_name,column_name,trans_function,execution_prior,technique,char_length,data_type,
                      no_trans_funcs,no_distinct_trans_funcs,stereo_type ,
                      case when (sum (case when trans_function= 'ut.MAN' or trans_function= 'ut.EXCLUDE' then 0 else 1 end) over  (partition by  owner,table_name)) = 0 then 'Y' else 'N' end manual_only
                      from pc_transform)
      loop
        l_trans_function := '';
        l_needs_substr := 'false';

        for getDistFuncsRec in getDistFuncs(pc_rec.trans_function) loop
          if getDistFuncsRec.func in ('ut.RN') then
            l_needs_substr := 'true';
          end if;
          l_trans_function := l_trans_function||getDistFuncsRec.func||'(';
        end loop;

        l_trans_function := RPAD(l_trans_function||pc_rec.column_name,length(l_trans_function||pc_rec.column_name)+(pc_rec.no_distinct_trans_funcs),')');

        if l_needs_substr = 'true' then
          for getItemsRec in getItems(pc_rec.char_length) loop
            l_length:= getItemsRec.item;
          end loop;
          l_trans_function := 'substr('||l_trans_function||',1,'||l_length||')';
        end if;
        insert into pc_transform_2(owner,table_name ,column_name,trans_function,execution_prior,technique,char_length,
                                   data_type,no_trans_funcs,no_distinct_trans_funcs,stereo_type,manual_only)
        values (pc_rec.owner,pc_rec.table_name ,pc_rec.column_name,l_trans_function,pc_rec.execution_prior,pc_rec.technique,pc_rec.char_length,
                pc_rec.data_type,pc_rec.no_trans_funcs,pc_rec.no_distinct_trans_funcs,pc_rec.stereo_type,pc_rec.manual_only);
      end loop;


      update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_pc_transform_loaded => 'Y');
      commit;


    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),gp.src_prefix,gp.anon_version,gp.tgt_prefix,v_code,v_errm,const_module);
          obfuscation_control.update_obfus_control(v_obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_obfus_status => 'FAILED');
    end;
end load_pc_transform;

function check_stages(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2,p_stage_tab out string_list_257,p_msg_tab out string_list_4000) return varchar2 is

  cursor getViolations is 
  select distinct owner,table_name,stage
  from
  (
     select owner,table_name,substr(stage_step_code,1,5) stage, 
            count(distinct table_name) over (partition by substr(stage_step_code,1,5)) no_tables
       from obfus_ctrl_stmts
      where obfus_run_id = gp.obfus_run_id
      
  ) where no_tables > 1;
  
  i number;
  
  const_module      CONSTANT  varchar2(62) := 'obfuscation_control.check_stages';
  v_code       number;
  v_errm       varchar2(4000); 
  
  begin
    i:= 1;
    for getViolationsRec in getViolations loop
      p_stage_tab(i) := getViolationsRec.owner||'.'||getViolationsRec.table_name;
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
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,const_module);
          obfuscation_control.update_obfus_control(p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_obfus_status => 'FAILED');
          return  const.k_Fail; 
  end check_stages;					  

procedure execution_report(p_obfus_run_id number, p_src_prefix varchar2, p_tgt_prefix varchar2, p_run_env varchar2, p_anon_version varchar2)  is
  
  const_module      CONSTANT  varchar2(62) := 'obfuscation_control.execution_report';
  v_code       number;
  v_errm       varchar2(4000); 
  
  begin
  
    delete from execution_report where obfus_run_id = p_obfus_run_id;
  
    insert into execution_report(obfus_run_id,stage_step_code,owner,table_name,stmt_seq,execution_id,start_timestamp,end_timestamp,duration_seconds,status,log_msg) 
    
    select ocer.obfus_run_id,ocer.stage_step_code,ocs.owner,ocs.table_name,ocer.stmt_seq,ocer.execution_id ,         
    ocer.start_timestamp,ocer.end_timestamp,ocer.duration_seconds,ocer.status,ol.log_msg          
    from  obfus_control_exec_result ocer
    join obfus_ctrl_stmts ocs on ocs.stage_step_code = ocer.stage_step_code and ocs.obfus_run_id = ocer.obfus_run_id and ocs.stmt_seq = ocer.stmt_seq
    left outer join  obfuscation_log ol on ol.log_id = ocer.obfus_log_id
    where (ocer.obfus_run_id,ocer.stage_step_code, ocer.execution_id,ocer.stmt_seq) in
    (
      select obfus_run_id,stage_step_code,max(execution_id) over (partition by stage_step_code) execution_id,stmt_seq 
      from obfus_control_exec_result
      where obfus_run_id = p_obfus_run_id
    ) 
    order by translate(ocer.stage_step_code,const.k_Stmt_Order_Chr,const.k_Stmt_Order_Num),ocer.stmt_seq;
  
    commit;
  
    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,const_module);
          obfuscation_control.update_obfus_control(p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_obfus_status => 'FAILED');
          rollback;
  end execution_report;
end obfuscation_control;
/
