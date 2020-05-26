create or replace PACKAGE BODY obfuscation_control
AS

  procedure obfus_log(p_log_msg VARCHAR2,p_src_prefix VARCHAR2,p_anon_version VARCHAR2,p_tgt_prefix VARCHAR2,p_code NUMBER,p_errm varchar2,p_module varchar2) is
  
     pragma autonomous_transaction;
    
  begin 
     insert into obfuscation_log (log_id,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module, mod_timestamp)
     values (obfuscation_log_seq.nextval ,p_log_msg ,p_src_prefix,p_anon_version,p_tgt_prefix,p_code,p_errm, p_module, systimestamp);
     commit;
     
     g_errm := null;
     g_code := null;
  end obfus_log;  
  
  function create_obfus_control(p_src_prefix VARCHAR2, p_tgt_prefix VARCHAR2, p_run_env VARCHAR2, p_anon_version VARCHAR2) 
    return number
  is
     pragma autonomous_transaction;
     v_run_id_seq NUMBER := NULL;
     x_obfus_control_already_exists EXCEPTION;
     PRAGMA exception_init (x_obfus_control_already_exists, -20002);       
  begin 
     g_module := 'obfuscation_control.create_obfus_control'; 
     
     begin
        insert into obfus_control(obfus_run_id,src_prefix,tgt_prefix,run_env,anon_version,obfus_status,
                                  setup_triggers,setup_synonyms,setup_indexes,setup_cheque_ranges,setup_stats,
                                  checked,run_start_time,run_completed_date,created_time,updated_time)   
                          values (OBFUS_RUN_ID_SEQ.nextval,p_src_prefix,p_tgt_prefix,p_run_env,p_anon_version,'PENDING',
                                  'N','N','N','N','N',
                                  'N',null,null,sysdate,sysdate);
        commit;
         
        v_run_id_seq := OBFUS_RUN_ID_SEQ.currval;
     exception
        when dup_val_on_index then
           select obfus_run_id
             into v_run_id_seq
             from obfus_control
            where src_prefix = p_src_prefix
              and tgt_prefix = p_tgt_prefix
              and run_env = p_run_env;
           
           obfus_log('Obfuscation control record already exists for source: '|| p_src_prefix || ' , target: ' || p_tgt_prefix || ' ,run_env: ' || p_run_env,
                      p_src_prefix,p_anon_version,p_tgt_prefix,null,null,g_module);         
     end;
     
     return v_run_id_seq;
  end create_obfus_control;
  
  procedure update_obfus_control(p_obfus_run_id NUMBER, p_src_prefix VARCHAR2,  p_tgt_prefix VARCHAR2, p_run_env VARCHAR2, p_anon_version VARCHAR2, 
                                 p_obfus_status VARCHAR2 DEFAULT NULL, p_setup_triggers VARCHAR2 DEFAULT NULL, p_setup_synonyms VARCHAR2 DEFAULT NULL, 
                                 p_setup_indexes VARCHAR2 DEFAULT NULL, p_setup_cheque_ranges VARCHAR2 DEFAULT NULL, p_setup_stats VARCHAR2 DEFAULT NULL,                               
                                 p_checked VARCHAR2 DEFAULT NULL ) is
    
     pragma autonomous_transaction;     
  begin 
     g_module := 'obfuscation_control.update_obfus_control';  
  
     update obfus_control
        set obfus_status        = nvl(p_obfus_status,obfus_status),
            setup_triggers      = nvl(p_setup_triggers,setup_triggers),
            setup_synonyms      = nvl(p_setup_synonyms,setup_synonyms),
            setup_indexes       = nvl(p_setup_indexes,setup_indexes),
            setup_cheque_ranges = nvl(p_setup_cheque_ranges,setup_cheque_ranges),
            setup_stats         = nvl(p_setup_stats,setup_stats),            
            checked             = nvl(p_checked,checked),
            run_start_time      = DECODE(p_obfus_status,'RUNNING',sysdate,run_start_time),
            run_completed_date  = DECODE(p_obfus_status,'COMPLETED',sysdate,run_completed_date),
            updated_time        = sysdate
      where obfus_run_id = p_obfus_run_id
        and src_prefix   = p_src_prefix
        and p_run_env    = p_run_env
        and anon_version = p_anon_version
        and tgt_prefix   = p_tgt_prefix;
            
     if sql%rowcount = 0 then       
        obfus_log('No matching obfus_control record found to update',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,g_module);   
     end if;
     
     commit;
  end update_obfus_control;  
    
  procedure insert_tables_to_truncate(p_tgt_prefix VARCHAR2)
  is
  begin
     g_module := 'obfuscation_control.insert_tables_to_truncate';    
  
     obfuscation_control.obfus_log('Truncating table tables_to_truncate',null,null,p_tgt_prefix,null,null,g_module);
     execute immediate 'truncate table tables_to_truncate';
  
     obfuscation_control.obfus_log('Inserting table tables_to_truncate',null,null,p_tgt_prefix,null,null,g_module);  
     insert into tables_to_truncate ( owner, table_name, single_column, num_rows )
        with  sc as (select atc.owner,atc.table_name,column_name  as single_column
        from all_tab_columns atc where atc.column_id = 1 and  atc.owner like p_tgt_prefix||'\_%' escape g_escape) 
        
        select at.owner, at.table_name table_name,sc.single_column,at.num_rows
        from sys.all_tables at join sc on at.table_name = sc.table_name
        where at.owner like p_tgt_prefix||'\_%' escape g_escape and at.table_name in  ('F0817438_TEMP_TABLE','REVERSAL_TEMP','TEMP_HOLDER','COMPANIES_TO_DROP','FR2007_AEOI_HOLDERS_TMP','RR654_DISC_EXCER_INSTRUCT_TEMP','RR380_SHARE_ALLOC_REVERSAL'
        ,'RR433_MATRIX_STG_DATA','RR423_GENRATE_AWARD_DT_STATUS','RR478_APPA_CAP_SUMM_TEMP','RR478_APPA_CASH_PAY_TEMP','RR478_APPB_FUND_SCHD_TEMP'
        ,'RR580_EVT_PAYMENT_STG','RR596_AUDIT_DIFF_DATA_STG','RR658_STAGING_SUMM_RPT','RS2860_00000001_TMPREP_TMP','SIP_CASH_RECON_TEMP'
        ,'TEST_719','TMP_MANDATE_DATA')

        union
        
        select at.owner, at.table_name table_name,sc.single_column,at.num_rows
        from sys.all_tables at join sc on at.table_name = sc.table_name
        where at.owner like p_tgt_prefix || '%\_AUDIT' escape '\' and (at.table_name LIKE 'A\_%' escape g_escape or at.table_name LIKE 'T\_%' escape g_escape )
        
        union
        
        select at.owner, at.table_name table_name,sc.single_column,at.num_rows
        from all_tables at join sc on  at.table_name = sc.table_name
        where at.owner like p_tgt_prefix || '\_INTEGRATION' escape '\'
                and at.iot_name is null
                and at.table_name not in ('DATA_LOCATION_TYPES','DISPATCH_BILLING_STATS_TYPES','DISPATCH_FTP_DEST_TYPES','DISPATCH_GROUPS',
                                          'DISPATCH_GROUP_STATUS_TYPES','DISPATCH_GRPS_LIST_TYPES','DISPATCH_ITEM_SOURCE_TYPES','DISPATCH_ITEM_STATUS_TYPES',
                                          'DISPATCH_TYPES','INTERFACE_SETTINGS','INT_PAYMENT_STATUS_TYPES','JOB_SCHEDULE_INTERVAL_TYPES','JOB_SCHEDULE_STAT_TYPES',
                                          'JOB_STATUS_TYPES','JOB_TYPES','OM_PRISM_COMP_CTRL','SOURCE_EMAIL_ADDRESS_TYPES','SYSTEMS','WUP_HEADER_STATUS_TYPES',
                                          'MIFID_UNAVISTA_REPORT_DATA','WUP_HEADER_SUB_UPDATE_TYPES','WUP_HEADER_UPDATE_TYPES','WUP_MARKER_ACTION_TYPES',
                                          'TASK_PARAM_COLLECTIONS','TASK_PARAM_TYPES')
        
        union
        
        select at.owner, at.table_name table_name,sc.single_column,at.num_rows
        from all_tables at join sc on  at.table_name = sc.table_name
        where at.owner in (p_tgt_prefix||'_' ||'CASH_MANAGEMENT',p_tgt_prefix||'_' || 'CREST',p_tgt_prefix||'_' || 'PRISM_CAG_PARAMS',p_tgt_prefix||'_' || 'PRISM_CORE')   
        and at.table_name LIKE '%\_HIST' escape g_escape
        
        union
 
        select at.owner, at.table_name, sc.single_column,at.num_rows
          from all_tables at join sc on  at.table_name = sc.table_name 
         where at.owner like p_tgt_prefix||'\_%' escape g_escape
           and REGEXP_LIKE(at.table_name, '^(RR){1}[0-9]{2,3}')
           and at.table_name not like'%TYPE%'
           and at.temporary <> 'Y'
        
        union
        
        select at.owner, at.table_name, sc.single_column,at.num_rows
          from all_tables at join sc on  at.table_name = sc.table_name 
         where at.owner like p_tgt_prefix||'\_%' escape g_escape
           and REGEXP_LIKE(at.table_name, '^(FR){1}[0-9]{2,3}')
           and at.table_name not like'%TYPE%'
           and at.temporary <> 'Y'
           and at.table_name like '%\_TEMP%' escape g_escape;    
  
     commit;
  
  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);     
        obfuscation_control.obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,p_tgt_prefix,g_code,g_errm,g_module);
  
  end insert_tables_to_truncate;
    
  procedure obfus_precheck ( p_src_prefix VARCHAR2, p_tgt_prefix VARCHAR2, p_run_env VARCHAR2, p_anon_version VARCHAR2 ) AS 
     v_ddl VARCHAR2(4000);  
     v_sql VARCHAR2(4000);    
     v_count integer;
     v_obfus_run_id obfus_control.obfus_run_id%TYPE;
    
  begin
  
     g_module := 'obfuscation_control.obfus_precheck';   
     v_obfus_run_id := obfuscation_control.create_obfus_control(p_src_prefix,p_tgt_prefix,p_run_env,p_anon_version);  
     obfuscation_control.obfus_log('Running ' || g_module || ' with obfus_control.obfus_run_id = ' || v_obfus_run_id,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,g_module);
     
     obfus_log('Calling insert_tables_to_truncate',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,g_module);     
     insert_tables_to_truncate(p_tgt_prefix);
     
     obfus_log('Re-creating table_populated_bu',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,g_module); 
     begin
        execute immediate 'drop table table_populated_bu purge';  
     exception
        when others then 
           null;
     end;
   
     execute immediate  'create table table_populated_bu as select * from table_populated';       
    
     obfus_log('Updating table_populated',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,g_module);            
     update table_populated 
        set prev_populated = case when prev_populated ='Y' then 'Y'
                                  else populated end,
            prev_mig_version = mig_version, populated = null, mig_version = null;  
            
     v_sql := 'update table_populated tp
                  set exist = ''N''
               where not exists ( select 1 
                                    from ALL_TAB_COLUMNS atc
                                    join ALL_TABLES at on at.OWNER = atc.OWNER and at.TABLE_NAME = atc.TABLE_NAME
                                   where at.temporary =  ''N'' and atc.owner like '''||p_tgt_prefix||'\_%'' escape ''\''
                                     and atc.table_name = tp.table_name          
                                     and atc.column_name = tp.column_name )';
     --dbms_output.put_line( v_sql );
     execute immediate v_sql; 

     obfus_log('Merging into table_populated',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,g_module);       
     v_sql := 'merge into table_populated tgt
                  using (select replace(atc.owner,'''||p_tgt_prefix||'_'','''') owner,atc.table_name,atc.column_name,atc.data_type,
                                case when atc.num_distinct = 0 then ''N'' else ''Y'' end populated
                           from all_tab_columns atc
                           join all_tables at on at.owner = atc.owner and at.table_name = atc.table_name
                          where at.temporary =  ''N'' and atc.owner like '''||p_tgt_prefix||'\_%'' escape ''\'') res
                     on (tgt.owner = res.owner and  tgt.table_name = res.table_name and tgt.column_name = res.column_name)
                   when matched then update set tgt.populated = res.populated,
                                            new_col = ''N'',tgt.data_type = res.data_type,mig_version = '||''''||p_tgt_prefix||''''||
                  ' when not matched then insert (owner,table_name, column_name,data_type,populated,new_col,mig_version)
                                          values (res.owner,res.table_name, res.column_name,res.data_type,res.populated,''Y'','''||p_tgt_prefix||''')' ;
            
     --dbms_output.put_line( v_sql );
     execute immediate v_sql; 
            
     commit;
         
     obfus_log('Re-creating table keys',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,g_module);                     
     begin
        execute immediate 'drop table keys purge';      
     exception
        when others then 
           null;
     end;
            
     v_ddl := 'create table keys as 
                  select acc.owner, acc.table_name, acc.column_name 
                    from all_cons_columns acc
                    join all_constraints ac on ac.owner = acc.owner
                                            and ac.table_name = acc.table_name
                                            and ac.constraint_name = acc.constraint_name
                   where ac.owner like ''' || p_tgt_prefix ||'%'' and ac.constraint_type IN (''P'',''R'')';
            
     --dbms_output.put_line(v_ddl);
     execute immediate v_ddl;              
                                   
     execute immediate 'create index keys_idx1 on keys(table_name,column_name)';         
            
     obfus_log('Re-populating table obfus_pre_check',p_src_prefix,p_anon_version,p_tgt_prefix,SQLCODE,SQLERRM,g_module);  
     execute immediate 'truncate table obfus_pre_check';          
            
     insert into obfus_pre_check(owner,table_name,column_name,populated,prev_populated,new_col,in_pc,mig_version,prev_mig_version,data_type,exist,notes)     
           select tp.owner,tp.table_name,tp.column_name,tp.populated,tp.prev_populated,tp.new_col,
                   case when pc.column_name is null then 'N' else 'Y'||'('||pc.technique||')'  end  in_pc,tp.mig_version,tp.prev_mig_version,tp.data_type,tp.exist,
                   case when tt.table_name is not null then 'ok - truncated'
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
            left join tables_to_truncate tt on tt.table_name = tp.table_name and replace(tt.owner,p_tgt_prefix||'_','') = tp.owner  
             where tp.table_name not like 'A\_%' escape g_escape;  
  
     commit;
         
     update_obfus_control(v_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_checked => 'Y');
     
     --INSERT INTO OBFUS_PRE_CHECK_history ( OWNER, TABLE_NAME, COLUMN_NAME, POPULATED, PREV_POPULATED, NEW_COL, IN_PC, MIG_VERSION, PREV_MIG_VERSION, DATA_TYPE , EXIST, NOTES )
     --SELECT OWNER, TABLE_NAME, COLUMN_NAME, POPULATED, PREV_POPULATED, NEW_COL, IN_PC, MIG_VERSION, PREV_MIG_VERSION, DATA_TYPE , EXIST, NOTES FROM OBFUS_PRE_CHECK;
     --commit;
     
  end obfus_precheck;    
    
  procedure check_obfus_ready(p_obfus_run_id IN OUT NUMBER, p_src_prefix IN OUT VARCHAR2, p_tgt_prefix IN OUT VARCHAR2, p_run_env IN OUT VARCHAR2, p_anon_version IN OUT VARCHAR2 ) is 
  begin 
     g_module := 'obfuscation_control.check_obfus_ready';  
     
     select obfus_run_id, src_prefix, tgt_prefix, run_env, anon_version 
       into p_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version
       from (
              select obfus_run_id, src_prefix, tgt_prefix, run_env, anon_version 
                from obfus_control
               where obfus_status IN ( 'PENDING', 'FAILED' ) -- allow recovery runs
                 and setup_triggers = 'Y'
                 and setup_synonyms = 'Y'
                 and setup_indexes = 'Y'
                 and setup_cheque_ranges = 'Y'
                 and setup_stats = 'Y'                 
                 and checked = 'Y'
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
    begin
    g_module := 'obfuscation_control.update_cheque_ranges';  
    
    obfus_log('UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES set end_no',p_src_prefix,null,null,null,null,g_module);
    execute immediate 'UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR
                          set cr.end_No= nvl((select cr1.start_no -1 from '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR1 
                                             where cr1.bank_account_id = cr.bank_account_id and CR1.cheque_range_type_code = ''REISSUE''),999999)
                        where CR.cheque_range_type_code = ''AUTO''';

    obfus_log('UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES set start_no',p_src_prefix,null,null,null,null,g_module);
    execute immediate 'UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR
                          set cr.start_no = nvl((select cr1.end_no+1 from '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR1 
                                                  where cr1.bank_account_id = cr.bank_account_id and CR1.cheque_range_type_code = ''MANUAL''),1)
                        where CR.cheque_range_type_code = ''AUTO''';
    
    commit;
   end update_cheque_ranges;
 
end obfuscation_control; 