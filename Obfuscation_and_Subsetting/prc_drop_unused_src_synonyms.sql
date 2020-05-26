  create or replace procedure prc_drop_unused_src_synonyms (p_prism_prefix in varchar2)
  is
     const_module  CONSTANT   varchar2(62) := 'prc_drop_unused_src_synonyms';
     
     v_code              number;
     v_errm              varchar2(4000);
     
     cursor c_synonyms_to_drop
     is
        select * from user_synonyms where table_owner not like p_prism_prefix||'%' and db_link = 'SRC_LINK';
  
     v_stmt varchar2(1000);  
  
   begin
   
      for r in c_synonyms_to_drop
      loop
      
         v_stmt := 'DROP SYNONYM '||r.synonym_name;
         
         insert into ss_log (log_id,stage_step_code,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module, mod_timestamp, ss_run_id, execution_id, stage_type)
         values (ss_log_id_seq.nextval, null, 'Executing '||v_stmt, p_prism_prefix, null, null, null, null, const_module, systimestamp, null, null, null);
        
         commit;
        
         execute immediate v_stmt;
         
      end loop;
      
   end prc_drop_unused_src_synonyms;
   /