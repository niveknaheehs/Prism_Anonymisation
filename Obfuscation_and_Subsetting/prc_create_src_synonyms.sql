  create or replace procedure prc_create_src_synonyms (p_prism_prefix in varchar2, p_run_env in varchar2)
  is
     const_module  CONSTANT   varchar2(62) := 'prc_create_src_synonyms';
     
     v_code              number;
     v_errm              varchar2(4000); 
    
     cursor c_src_synonyms
     is    
        select 'create or replace synonym SRC_'||table_name||' for '||p_prism_prefix||'_PRISM_UTILITIES.'||table_name||'@SRC_LINK' stmt
          from dd_tables@SRC_LINK dd
         where ( table_name like 'DD\_%' escape '\'
              or table_name like 'MD\_%' escape '\'
              or table_name like 'SS\_%' escape '\' )
        union
        select 'create or replace synonym SRC_'||view_name||' for '||p_prism_prefix||'_PRISM_UTILITIES.'||view_name||'@SRC_LINK' stmt
          from dd_views@SRC_LINK dd
         where view_name like 'VW\_%' escape '\';    
    
    
  begin

     insert into ss_log (log_id,stage_step_code,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module, mod_timestamp, ss_run_id, execution_id, stage_type)
     values (ss_log_id_seq.nextval, null, 'Calling metadata_utilities.load_dd@SRC_LINK('||p_prism_prefix||')', p_prism_prefix, null, null, null, null, const_module, systimestamp, null, null, null);
     commit;
     
     metadata_utilities.load_dd@SRC_LINK(p_prism_prefix);  
  
     for r in c_src_synonyms
     loop  
            
        insert into ss_log (log_id,stage_step_code,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module, mod_timestamp, ss_run_id, execution_id, stage_type)
        values (ss_log_id_seq.nextval, null, 'Creating SYNONYM '||r.stmt, p_prism_prefix, null, null, null, null, const_module, systimestamp, null, null, null);
        
        commit;
        
        execute immediate r.stmt;
        
     end loop;
 
     insert into ss_log (log_id,stage_step_code,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module, mod_timestamp, ss_run_id, execution_id, stage_type)
     values (ss_log_id_seq.nextval, null, 'Recompiling schema '||p_run_env, p_prism_prefix, null, null, null, null, const_module, systimestamp, null, null, null);     
     commit;

     DBMS_UTILITY.COMPILE_SCHEMA(upper(p_run_env));
     
  end prc_create_src_synonyms;
  /