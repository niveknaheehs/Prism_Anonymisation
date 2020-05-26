declare
  v_tgt_prefix     obfus_control.tgt_prefix%TYPE   := 'ANONPOST8201B';
  v_src_prefix     obfus_control.src_prefix%TYPE   := 'ANONPRE8201';
  v_run_env        obfus_control.run_env%TYPE      := 'ANONDEV_ANONYMISE';
  v_anon_version   obfus_control.anon_version%TYPE := 'OB8';
  v_module         varchar2(50) := 'setup_obfus_env_synonyms_and_grants';
  v_obfus_run_id   obfus_control.obfus_run_id%TYPE := null;
  v_synonym_count  integer := 0;  
  v_escape         varchar2(1)  := '\'; 
    
  cursor get_tabs(p_prefix VARCHAR2,p_escape VARCHAR2) is select dt.owner,dt.table_name  
                                                            from dba_tables dt
                                                           where dt.owner like p_prefix || '\_%' escape p_escape
                                                             and ( iot_type IS NULL or iot_type != 'IOT_OVERFLOW' )
                                                             --ORA-25191: cannot reference overflow table of an index-organized table ANONPRE8201_INTEGRATION.SYS_IOT_OVER_2055452
                                                             and (    dt.table_name not like'RS2365_00000001_A_TMP_NT%'
                                                                  and dt.table_name not like'RS2365_00000001_B_TMP_NT%'
                                                                  and dt.table_name not like'RS2856_00000001_EXTRACT_TMPNT%' );
                                                             --22812. 00000 -  "cannot reference nested table column's storage table"
                                                                
  cursor get_synonyms (p_env VARCHAR2,p_escape VARCHAR2) is 
    select owner,synonym_name from dba_synonyms where owner = p_env;

  x_synonym_not_exist EXCEPTION;
  PRAGMA exception_init (x_synonym_not_exist, -01434);       
  
begin

  v_obfus_run_id := obfuscation_control.create_obfus_control(v_src_prefix,v_tgt_prefix,v_run_env,v_anon_version);  
  obfuscation_control.obfus_log('Running ' || v_module || ' with obfus_control.obfus_run_id = ' || v_obfus_run_id,v_src_prefix,v_anon_version,v_tgt_prefix,null,null,v_module);
  
   --
   --  -- Clear Out all Synonyms in the run environment
   --       
   obfuscation_control.obfus_log('Dropping synonyms in run env: ' || v_run_env,v_src_prefix,v_anon_version,v_tgt_prefix,null,null,v_module);   
     for get_synonyms_rec in get_synonyms(v_run_env,v_escape) loop
       begin       
         execute immediate ('drop synonym  '||get_synonyms_rec.owner||'.'||get_synonyms_rec.synonym_name);
         
         --dbms_output.put_line('drop synonym  '||get_synonyms_rec.owner||'.'||get_synonyms_rec.synonym_name);
       exception 
          when x_synonym_not_exist then
             null;
       end;
     end loop;
   
     -- Grant 'Select' on all tables in the source database
     -- Create synonyms for all tables in the source database
     obfuscation_control.obfus_log('Creating synonyms and grants for source env: ' || v_src_prefix,v_src_prefix,v_anon_version,v_tgt_prefix,null,null,v_module);    
     for get_tabs_rec in get_tabs(v_src_prefix,v_escape) loop
       begin

         --dbms_output.put_line('create or replace synonym '||v_run_env ||'.'||get_tabs_rec.table_name||' for '|| get_tabs_rec.owner||'.'||get_tabs_rec.table_name);
         execute immediate ('create or replace synonym '||v_run_env ||'.'||get_tabs_rec.table_name||' for '|| get_tabs_rec.owner||'.'||get_tabs_rec.table_name);
 
         v_synonym_count := v_synonym_count + 1;
 
         --dbms_output.put_line('grant select on '||get_tabs_rec.owner||'.'||get_tabs_rec.table_name||' to '|| v_run_env);  
         execute immediate ('grant select on '||get_tabs_rec.owner||'.'||get_tabs_rec.table_name||' to '|| v_run_env);

       exception 
          when others then
             raise;
       end;
     end loop;
     
     -- Grant 'all' on all tables in the target database
     obfuscation_control.obfus_log('Granting all on tables in target env: ' || v_tgt_prefix,v_src_prefix,v_anon_version,v_tgt_prefix,null,null,v_module);        
     for get_tabs_rec in get_tabs(v_tgt_prefix,v_escape) loop
        begin
          execute immediate ('grant all on '||get_tabs_rec.owner||'.'||get_tabs_rec.table_name||' to '|| v_run_env);
          begin
            execute immediate ('grant all on '||get_tabs_rec.owner||'.'||get_tabs_rec.table_name||' to cpenny');
            execute immediate ('grant all on '||get_tabs_rec.owner||'.'||get_tabs_rec.table_name||' to ksheehan'); 
          exception
             when others then null; -- will either fail for cpenny or ksheehan - cannot grant to self
          end; 
       exception 
          when others then
             raise;      
       end;
     end loop;
      
     -- Create synonyms for all tables in the target database
     obfuscation_control.obfus_log('Creating synonyms for target env: ' || v_src_prefix,v_src_prefix,v_anon_version,v_tgt_prefix,null,null,v_module);     
     for get_tabs_rec in get_tabs(v_tgt_prefix,v_escape) loop
       begin
         execute immediate ('create or replace synonym '||v_run_env ||'.TGT_'||substr(get_tabs_rec.table_name,1,26)||' for '|| get_tabs_rec.owner||'.'||get_tabs_rec.table_name);
         v_synonym_count := v_synonym_count + 1;
       exception when others then
           raise;
       end;
     end loop;         
     
     obfuscation_control.obfus_log('Granting select, update on '||v_src_prefix||'_CASH_MANAGEMENT.cheque_ranges to '||v_run_env,v_src_prefix,v_anon_version,v_tgt_prefix,null,null,v_module);     
     --dbms_output.put_line('grant select, update on '||v_src_prefix||'_CASH_MANAGEMENT.cheque_ranges to '||v_run_env);
     execute immediate 'grant select, update on '||v_src_prefix||'_CASH_MANAGEMENT.cheque_ranges to '||v_run_env;         

     if v_synonym_count > 1 then        
       obfuscation_control.update_obfus_control(v_obfus_run_id, v_src_prefix, v_tgt_prefix, v_run_env, v_anon_version, p_setup_synonyms => 'Y');
       obfuscation_control.obfus_log('setup completed successfully for ' || v_synonym_count || ' synonyms with obfus_run_id = ' || v_obfus_run_id,v_src_prefix,v_anon_version,v_tgt_prefix,null,null,v_module);  
     end if;
     
exception
  when others then
     raise;
end;

