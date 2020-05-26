declare
g_src_prefix VARCHAR2(50) := 'ANONPRE8201\_%'; -- Prism Source Prefix ANONPOST8201A
g_escape VARCHAR2(50) := '\'; -- Prism Source Prefix
g_tgt_prefix VARCHAR2(50) := 'ANONPOST8201A\_%';  -- Prism Target Prefix
g_run_env VARCHAR2(20) := 'ANONDEV_ANONYMISE'; -- Schema name for the run evironment
g_src_prefix1 VARCHAR2(50) := 'ANONPRE8201'; -- Prism Source Prefix
g_tgt_prefix1 VARCHAR2(50) := 'ANONPOST8201A';  -- Prism Target Prefix

cursor get_tabs(p_src_prefix VARCHAR2,p_escape VARCHAR2) is select dtc.owner,dtc.table_name from dba_tab_columns dtc
                                                            join dba_tables dt on dt.owner = dtc.owner and dt.table_name = dtc.table_name
                                                            where dtc.owner like p_src_prefix escape p_escape;

cursor get_indexes (p_env VARCHAR2) is select atc.owner,atc.table_name,atc.column_name,
                                        row_number() over (partition by atc.table_name order by atc.table_name,atc.column_name) as iseq 
                                        from all_tab_columns  atc
                                        join all_tables at on at.owner = atc.owner and at.table_name = atc.table_name                                                  
                                        where atc.owner = p_env and (atc.column_name like '%HASH%' or atc.column_name like '%KEY_NS%');

cursor get_triggers (p_env VARCHAR2,p_escape VARCHAR2) is select owner,trigger_name from dba_triggers 
where table_owner like  p_env escape p_escape;

cursor get_synonyms (p_env VARCHAR2,p_escape VARCHAR2) is select owner,synonym_name from all_synonyms 
where owner =   p_env ;

begin


  dbms_output.put_line('Start');

  -- Disable all Triggers in the Target Environment
  for get_triggers_rec in get_triggers(g_tgt_prefix,g_escape) loop
    begin
      execute immediate ('alter trigger  '||get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name||' disable');
         --dbms_output.put_line('alter trigger  '||get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name||' enable');
      exception when others then
                null;
                 --dbms_output.put_line('alter trigger  '||get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name||' enable'); --null;
    end;
  end loop;
--
--  -- Clear Out all Synonyms in the run environmet
--  
   for get_synonyms_rec in get_synonyms(g_run_env,g_escape) loop
     begin
       execute immediate ('drop synonym  '||get_synonyms_rec.owner||'.'||get_synonyms_rec.synonym_name);
       
       --dbms_output.put_line('drop synonym  '||get_synonyms_rec.owner||'.'||get_synonyms_rec.synonym_name);
       exception when others then
       null;
     end;
   end loop;
--   
   -- Grant 'Select' on all tables in the source database
   -- Create synonyms for all tables in the source database
  
   for get_tabs_rec in get_tabs(g_src_prefix,g_escape) loop
     begin

       execute immediate ('grant select on '||get_tabs_rec.owner||'.'||get_tabs_rec.table_name||' to '|| g_run_env);
       execute immediate ('create or replace synonym '||g_run_env ||'.'||get_tabs_rec.table_name||' for '|| get_tabs_rec.owner||'.'||get_tabs_rec.table_name);
       exception when others then
            dbms_output.put_line ('grant select on '||get_tabs_rec.owner||'.'||get_tabs_rec.table_name||' to '|| g_run_env);
            raise;

       null;
     end;
   end loop;
 
    
-- Analyse Stats in the target  
    
-- for get_tabs_rec in get_tabs(g_tgt_prefix,g_escape) loop
--   begin
--   
----     dbms_output.put_line ('dbms_stats.gather_table_stats('||get_tabs_rec.owner||','||get_tabs_rec.table_name||',10);');
--     dbms_stats.gather_table_stats(get_tabs_rec.owner, get_tabs_rec.table_name);
--     exception when others then
--     --raise;
--     null;
--   end;
-- end loop;

--
   -- Grant 'all' on all tables in the target database
   
  for get_tabs_rec in get_tabs(g_tgt_prefix,g_escape) loop
    begin
      execute immediate ('grant all on '||get_tabs_rec.owner||'.'||get_tabs_rec.table_name||' to '|| g_run_env);
      exception when others then
     --raise;
     null;
    end;
  end loop;
----  
  -- Create synonyms for all tables in the target database
     
 for get_tabs_rec in get_tabs(g_tgt_prefix,g_escape) loop
   begin
     execute immediate ('create or replace synonym '||g_run_env ||'.TGT_'||substr(get_tabs_rec.table_name,1,26)||' for '|| get_tabs_rec.owner||'.'||get_tabs_rec.table_name);
     exception when others then
     --raise;
     null;
   end;
 end loop;
-- 
--  -- Create indexes for all  HASH and KEY_NS columns in the run environment (ensure the table creation script is run first)
--   
 for get_indexes_rec in get_indexes(g_run_env) loop
   begin
     --dbms_output.put_line ('create or replace index '||substr(get_indexes_rec.table_name,1,10)||substr(get_indexes_rec.column_name,11,20)||get_indexes_rec.iseq ||' on '||get_indexes_rec.owner||'.'||get_indexes_rec.table_name||'('|| get_indexes_rec.column_name||')');
     execute immediate 'create  index '||get_indexes_rec.owner||'.'||substr(get_indexes_rec.table_name,1,10)||substr(get_indexes_rec.column_name,11,20)||get_indexes_rec.iseq ||' on '||get_indexes_rec.owner||'.'||get_indexes_rec.table_name||'('|| get_indexes_rec.column_name||')';
     null;
     exception when others then
     null;
   end;
 end loop;

--   dbms_output.put_line ('create create or replace index test_f on '||g_src_prefix1||'_cash_management.cash_accounts (nvl (IVC_code, - 9999))'); 
--  

--  begin
--    execute immediate 'create index test_f on '||g_src_prefix1||'_cash_management.cash_accounts (nvl (IVC_code, - 9999))'; 
--    exception when others then null;
--  end;
------     
--  begin                           
--    execute immediate 'create index holder_names_idx on '||g_src_prefix1||'_prism_core.holder_names (comp_code)'; 
--    exception when others then null;
--  end;
----  

--  
--  execute immediate 'alter trigger '||g_src_prefix1||'_PRISM_CORE.HOLDERS_BRIUD enable';
--  
--
--  execute immediate 'create or replace synonym '||g_run_env||'.holder_crud for anonpost_prism_core.holder_crud';
--
--  execute immediate 'grant execute on anonpost_prism_core.holder_crud to '||g_run_env;
--
--  execute immediate 'alter trigger '||g_src_prefix1||'_PRISM_CORE.HOLDER_ADDRESSES_BRIUD disable';
--

execute immediate 'grant drop any table  to '||g_run_env;

-- Cheque Range Adjustment Patch

execute immediate 'grant all on '||g_src_prefix1||'_CASH_MANAGEMENT.cheque_ranges to '||g_run_env;
    
execute immediate 'alter trigger '||g_src_prefix1||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD disable';
--
execute immediate 'UPDATE '||g_src_prefix1||'_CASH_MANAGEMENT.CHEQUE_RANGES CR
set cr.end_No= nvl((select cr1.start_no -1 from '||g_src_prefix1||'_CASH_MANAGEMENT.CHEQUE_RANGES CR1 
                   where cr1.bank_account_id = cr.bank_account_id and CR1.cheque_range_type_code = ''REISSUE''),999999)
where CR.cheque_range_type_code = ''AUTO''';
-- 
-- 
execute immediate 'UPDATE '||g_src_prefix1||'_CASH_MANAGEMENT.CHEQUE_RANGES CR
set cr.start_no = nvl((select cr1.end_no+1 from '||g_src_prefix1||'_CASH_MANAGEMENT.CHEQUE_RANGES CR1 
                   where cr1.bank_account_id = cr.bank_account_id and CR1.cheque_range_type_code = ''MANUAL''),1)
where CR.cheque_range_type_code = ''AUTO''';

execute immediate 'alter trigger '||g_src_prefix1||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD enable';


end;