create or replace procedure setup_obfus_env (p_src_prefix                VARCHAR2,
                                             p_tgt_prefix                VARCHAR2,
                                             p_run_env                   VARCHAR2,
                                             p_anon_version              VARCHAR2,
                                             p_triggers                  BOOLEAN  DEFAULT TRUE,
                                             p_indexes                   BOOLEAN  DEFAULT TRUE,
                                             p_cheque_ranges             BOOLEAN  DEFAULT TRUE, 
                                             p_stats                     BOOLEAN  DEFAULT TRUE,                                     
                                             p_stats_est_percent         INTEGER  DEFAULT 10,
                                             p_days_since_last_analyzed  INTEGER  DEFAULT 1)
as       
/*************************************************************************************************************************************************************************************************
synonyms:
CREATE OR REPLACE SYNONYM cpenny.anonymisation_process FOR ANONDEV_ANONYMISE.anonymisation_process;
CREATE OR REPLACE SYNONYM cpenny.obfuscation_control FOR ANONDEV_ANONYMISE.obfuscation_control; 
CREATE OR REPLACE SYNONYM cpenny.obfus_control FOR ANONDEV_ANONYMISE.obfus_control; 
CREATE OR REPLACE SYNONYM cpenny.obfus_run_id_seq FOR ANONDEV_ANONYMISE.obfus_run_id_seq; 
CREATE OR REPLACE SYNONYM cpenny.obfuscation_log FOR ANONDEV_ANONYMISE.obfuscation_log;
CREATE OR REPLACE SYNONYM cpenny.obfuscation_log_seq FOR ANONDEV_ANONYMISE.obfuscation_log_seq;
CREATE OR REPLACE SYNONYM cpenny.calc_stats FOR ANONDEV_ANONYMISE.calc_stats;
CREATE OR REPLACE SYNONYM ksheehan.anonymisation_process FOR ANONDEV_ANONYMISE.anonymisation_process; 
CREATE OR REPLACE SYNONYM ksheehan.obfus_control FOR ANONDEV_ANONYMISE.obfus_control; 
CREATE OR REPLACE SYNONYM ksheehan.obfus_run_id_seq FOR ANONDEV_ANONYMISE.obfus_run_id_seq; 
CREATE OR REPLACE SYNONYM ksheehan.obfuscation_log FOR ANONDEV_ANONYMISE.obfuscation_log;
CREATE OR REPLACE SYNONYM ksheehan.obfuscation_log_seq FOR ANONDEV_ANONYMISE.obfuscation_log_seq;
CREATE OR REPLACE SYNONYM ksheehan.calc_stats FOR ANONDEV_ANONYMISE.calc_stats;
grants:
GRANT SELECT, INSERT, UPDATE ON ANONDEV_ANONYMISE.obfus_control TO cpenny;
GRANT EXECUTE ON ANONDEV_ANONYMISE.anonymisation_process TO cpenny;
GRANT SELECT ON ANONDEV_ANONYMISE.obfus_run_id_seq TO cpenny;
GRANT INSERT, SELECT ON ANONDEV_ANONYMISE.obfuscation_log TO cpenny;
GRANT SELECT ON ANONDEV_ANONYMISE.obfuscation_log_seq TO cpenny;
GRANT EXECUTE ON ANONDEV_ANONYMISE.calc_stats TO cpenny;
GRANT EXECUTE ON ANONDEV_ANONYMISE.obfuscation_control TO cpenny;

GRANT SELECT, INSERT, UPDATE ON ANONDEV_ANONYMISE.obfus_control TO ksheehan;
GRANT EXECUTE ON ANONDEV_ANONYMISE.anonymisation_process TO ksheehan;
GRANT SELECT ON ANONDEV_ANONYMISE.obfus_run_id_seq TO ksheehan;
GRANT INSERT, SELECT ON ANONDEV_ANONYMISE.obfuscation_log TO ksheehan;
GRANT SELECT ON ANONDEV_ANONYMISE.obfuscation_log_seq TO ksheehan;
GRANT EXECUTE ON ANONDEV_ANONYMISE.calc_stats TO ksheehan;
GRANT EXECUTE ON ANONDEV_ANONYMISE.obfuscation_control TO ksheehan;

-- GRANTS ON DBA views from JIRA ITSD-12260 
grant select on dba_tab_columns to cpenny;
grant select on dba_tab_columns to ksheehan;
grant select on dba_tables to cpenny;
grant select on dba_tables to ksheehan;
grant select on dba_tab_statistics to cpenny;
grant select on dba_tab_statistics to ksheehan;
grant select on dba_triggers to cpenny;
grant select on dba_triggers to ksheehan;
grant select on dba_synonyms to cpenny;
grant select on dba_synonyms to ksheehan;

GRANT ALTER ANY TRIGGER TO cpenny;
GRANT ALTER ANY TRIGGER TO ksheehan;
GRANT CREATE ANY SYNONYM TO ksheehan;
GRANT CREATE ANY SYNONYM TO cpenny;
GRANT DROP ANY SYNONYM TO ksheehan;
GRANT DROP ANY SYNONYM TO cpenny;
GRANT SELECT ANY TABLE TO ksheehan;
GRANT SELECT ANY TABLE TO cpenny;
GRANT CREATE ANY INDEX TO ksheehan;
GRANT CREATE ANY INDEX TO cpenny;
GRANT ANALYZE ANY TO ksheehan;
GRANT ANALYZE ANY TO cpenny;

Tests:
pre-setup: run setup_obfus_env_synonyms_and_grants.sql script
exec setup_obfus_env('ANONPRE8901','ANONPOST8901A','ANONDEV_ANONYMISE','OB8_R89_01', p_triggers => TRUE,  p_indexes => FALSE, p_cheque_ranges => FALSE, p_stats => FALSE);
exec setup_obfus_env('ANONPRE8901','ANONPOST8901A','ANONDEV_ANONYMISE','OB8_R89_01', p_triggers => FALSE, p_indexes => TRUE,  p_cheque_ranges => FALSE, p_stats => FALSE);
exec setup_obfus_env('ANONPRE8901','ANONPOST8901A','ANONDEV_ANONYMISE','OB8_R89_01', p_triggers => FALSE, p_indexes => FALSE, p_cheque_ranges => TRUE,  p_stats => FALSE);
exec setup_obfus_env('ANONPRE8901','ANONPOST8901A','ANONDEV_ANONYMISE','OB8_R89_01', p_triggers => FALSE, p_indexes => FALSE, p_cheque_ranges => FALSE, p_stats => TRUE);
post-setup: 
*************************************************************************************************************************************************************************************************/

  v_src_prefix       varchar2(50);  -- Prism Source Prefix 
  v_tgt_prefix       varchar2(50);  -- Prism Target Prefix 
  v_obfus_run_id     obfus_control.obfus_run_id%TYPE := null;
  
  v_escape           varchar2(1)  := '\'; 
  v_code             number;
  v_errm             varchar2(4000); 
  v_module           varchar2(32) := 'setup_obfus_env'; 
  
  v_trigger_count    integer := 0;
  v_synonym_count    integer := 0;
  v_index_count      integer := 0;
  v_indexes_dropped  integer := 0;  
  v_stats_count      integer := 0;
  
  v_ddl              varchar2(4000);
  v_index_name       varchar2(128);
  
  cursor get_tabs(p_prefix VARCHAR2,p_escape VARCHAR2) is select dt.owner,dt.table_name  
                                                            from dba_tables dt
                                                           where dt.owner like p_prefix escape p_escape;
 
  cursor c_tgt_table_stats (p_prefix VARCHAR2,p_escape VARCHAR2, p_days_since_last_analyzed INTEGER) is 
                                                                    select distinct dt.owner,dt.table_name  
                                                                      from dba_tables dt
                                                                      join dba_tab_statistics ats on dt.owner = ats.owner and dt.table_name = ats.table_name 
                                                                     where dt.owner like p_prefix escape p_escape 
                                                                       and TRUNC(NVL(ats.last_analyzed,SYSDATE-(p_days_since_last_analyzed+1))) < TRUNC(SYSDATE-p_days_since_last_analyzed);
  
  cursor get_indexes (p_env VARCHAR2) is select atc.owner,atc.table_name,atc.column_name,
                                                row_number() over (partition by atc.table_name order by atc.table_name,atc.column_name) as iseq 
                                           from dba_tab_columns  atc
                                           join dba_tables at on at.owner = atc.owner and at.table_name = atc.table_name                                                  
                                          where atc.owner = p_env 
                                            and (atc.column_name like '%HASH%' or atc.column_name like '%KEY_NS%');
  
  cursor get_triggers (p_env VARCHAR2,p_escape VARCHAR2) is 
    select owner,trigger_name from dba_triggers where table_owner like p_env escape p_escape;

  x_synonym_not_exist EXCEPTION;
  PRAGMA exception_init (x_synonym_not_exist, -01434);     

  x_table_not_exist EXCEPTION;
  PRAGMA exception_init (x_table_not_exist, -00942);
 
  x_index_not_exist EXCEPTION;
  PRAGMA exception_init (x_index_not_exist, -01418);  
  
  x_columns_already_indexed EXCEPTION;
  PRAGMA exception_init (x_columns_already_indexed, -01408);   
  
  x_idx_name_already_used EXCEPTION;
  PRAGMA exception_init (x_idx_name_already_used, -00955);  

begin

  v_tgt_prefix := p_tgt_prefix || '\_%'; 
  v_src_prefix := p_src_prefix || '\_%';
  
  dbms_output.put_line('Start');
  --dbms_output.put_line('v_tgt_prefix: ' || v_tgt_prefix || ' v_src_prefix: ' ||  v_src_prefix);
  v_obfus_run_id := obfuscation_control.create_obfus_control(p_src_prefix,p_tgt_prefix,p_run_env,p_anon_version);  
  obfuscation_control.obfus_log('Running ' || v_module || ' with obfus_control.obfus_run_id = ' || v_obfus_run_id,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);

  if p_triggers then
    -- Disable all Triggers in the Target Environment
    obfuscation_control.obfus_log('opening get_triggers cursor with ' || ' v_tgt_prefix: ' || v_tgt_prefix || ' v_escape: ' || v_escape,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);
    for get_triggers_rec in get_triggers(v_tgt_prefix,v_escape) loop
      begin
         --obfuscation_control.obfus_log(v_module || ': Disabling trigger ' || get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);
         execute immediate ('alter trigger  '||get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name||' disable');        
         v_trigger_count := v_trigger_count + 1;   
      exception when others then
         v_code := SQLCODE;
         v_errm := SUBSTR(SQLERRM,1,4000);        
         obfuscation_control.obfus_log('Error disabling trigger ' || get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name,p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,v_module);
         raise;
      end;
    end loop;
    
    if v_trigger_count > 1 then
      obfuscation_control.update_obfus_control(v_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_setup_triggers => 'Y');
      obfuscation_control.obfus_log(v_module || ' completed successfully for disabling ' || v_trigger_count || ' triggers with obfus_run_id = ' || v_obfus_run_id,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);   
    end if;
  end if; -- p_triggers
 
 
  if p_stats then    
  -- Analyse Stats in the target  
      
     obfuscation_control.obfus_log('opening c_tgt_table_stats cursor with ' || ' v_tgt_prefix: ' || v_tgt_prefix || ' v_escape: ' || v_escape || ' p_days_since_last_analyzed: ' || p_days_since_last_analyzed,
                                    p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);            
     for rec in c_tgt_table_stats(v_tgt_prefix,v_escape, p_days_since_last_analyzed) loop
        begin
       
          --obfuscation_control.obfus_log(v_module || ': gather_table_stats for '||rec.owner||'.'||rec.table_name,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);      
          dbms_stats.gather_table_stats(ownname => rec.owner,
                                        tabname => rec.table_name,
                                        estimate_percent => p_stats_est_percent );
                                        
          --calc_stats (rec.owner, rec.table_name, p_stats_est_percent);                                        

          v_stats_count := v_stats_count + 1;
       exception when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);        
          obfuscation_control.obfus_log('Error gathering stats for '||rec.owner||'.'||rec.table_name,p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,v_module);
       end;
     end loop;

     if v_stats_count > 1
     then
       obfuscation_control.update_obfus_control(v_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_setup_stats => 'Y');
       obfuscation_control.obfus_log(v_module || ' completed successfully gathering stats for ' || v_stats_count || ' target tables with obfus_run_id = ' || v_obfus_run_id,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);  
     end if;
  end if; -- p_stats

  if p_indexes then
     -- 
     --  -- Create indexes for all  HASH and KEY_NS columns in the run environment (ensure the table creation script is run first)
     --   
     obfuscation_control.obfus_log('opening get_indexes cursor with ' || ' p_run_env: ' || p_run_env,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);       
     for get_indexes_rec in get_indexes(p_run_env) loop
        begin
        
           v_index_name := get_indexes_rec.owner||'.'||get_indexes_rec.table_name||'_'||RTRIM(LTRIM(NVL(substr(get_indexes_rec.column_name,11,20),substr(get_indexes_rec.column_name,1,10)),'_'),'_')||'_'||get_indexes_rec.iseq;

           begin
              v_ddl := 'drop index '||v_index_name;    
              --dbms_output.put_line(v_ddl);
              execute immediate v_ddl;
              v_indexes_dropped := v_indexes_dropped + 1;
           exception
             when x_index_not_exist then
               null;
           end;
          
           v_ddl := 'create index '||v_index_name||' on '||get_indexes_rec.owner||'.'||get_indexes_rec.table_name||'('|| get_indexes_rec.column_name||')';
           --dbms_output.put_line(v_ddl);
           execute immediate v_ddl;
           v_index_count := v_index_count + 1;
        exception 
           when x_columns_already_indexed then
              v_index_count := v_index_count + 1;
              obfuscation_control.obfus_log('Error columns_already_indexed - index DDL: ' || v_ddl,p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,v_module);                        
           when x_idx_name_already_used then
              v_code := SQLCODE;
              v_errm := SUBSTR(SQLERRM,1,4000);        
              obfuscation_control.obfus_log('Error idx_name_already_used - index DDL: '  || v_ddl,p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,v_module);           
           when others then
              v_code := SQLCODE;
              v_errm := SUBSTR(SQLERRM,1,4000);        
              obfuscation_control.obfus_log('Error - index DDL: '  || v_ddl,p_src_prefix,p_anon_version,p_tgt_prefix,v_code,v_errm,v_module);
        end;
     end loop;
    
    --   dbms_output.put_line ('create create or replace index test_f on '||v_src_prefix1||'_cash_management.cash_accounts (nvl (IVC_code, - 9999))'); 
    --  
    
    --  begin
    --    execute immediate 'create index test_f on '||p_src_prefix||'_cash_management.cash_accounts (nvl (IVC_code, - 9999))'; 
    --    exception when others then null;
    --  end;
    ------     
    --  begin                           
    --    execute immediate 'create index holder_names_idx on '||p_src_prefix||'_prism_core.holder_names (comp_code)'; 
    --    exception when others then null;
    --  end;
    ----  
  
     if (v_indexes_dropped > 0) then
        obfuscation_control.obfus_log('Dropped ' || v_indexes_dropped || ' indexes with obfus_run_id = ' || v_obfus_run_id,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);     
     end if;    
     
     if (v_index_count > 1) then
        obfuscation_control.update_obfus_control(v_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_setup_indexes => 'Y');
        obfuscation_control.obfus_log(v_module || ' completed successfully for creating ' || v_index_count || ' indexes with obfus_run_id = ' || v_obfus_run_id,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);     
     end if;
  end if; -- p_indexes

--  
--  execute immediate 'alter trigger '||p_src_prefix||'_PRISM_CORE.HOLDERS_BRIUD enable';
--  
--
--  execute immediate 'create or replace synonym '||p_run_env||'.holder_crud for anonpost_prism_core.holder_crud';
--
--  execute immediate 'grant execute on anonpost_prism_core.holder_crud to '||p_run_env;
--
--  execute immediate 'alter trigger '||p_src_prefix||'_PRISM_CORE.HOLDER_ADDRESSES_BRIUD disable';
--

--  anonymisation_process.obfus_log('setup: grant drop any table  to '||p_run_env,null,null,v_module);
--  execute immediate 'grant drop any table to '||p_run_env;

  if p_cheque_ranges then  
    -- Cheque Range Adjustment Patch
     
    -- below moved to setup_obfus_env_synonyms_and_grants.sql 
    --anonymisation_process.obfus_log('setup: grant all on '||p_src_prefix||'_CASH_MANAGEMENT.cheque_ranges to '||p_run_env,null,null,v_module);
    --execute immediate 'grant select, update on '||p_src_prefix||'_CASH_MANAGEMENT.cheque_ranges to '||p_run_env;
 
    obfuscation_control.obfus_log(v_module || ': alter trigger '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD disable',p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);        
    execute immediate 'alter trigger '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD disable';
  
    obfuscation_control.update_cheque_ranges(p_src_prefix);
  
--    anonymisation_process.obfus_log(v_module || ': UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES set end_no',null,null,v_module);
--    execute immediate 'UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR
--                          set cr.end_No= nvl((select cr1.start_no -1 from '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR1 
--                                             where cr1.bank_account_id = cr.bank_account_id and CR1.cheque_range_type_code = ''REISSUE''),999999)
--                        where CR.cheque_range_type_code = ''AUTO''';
--
--    anonymisation_process.obfus_log(v_module || ': UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES set start_no',null,null,v_module);
--    execute immediate 'UPDATE '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR
--                          set cr.start_no = nvl((select cr1.end_no+1 from '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES CR1 
--                                                  where cr1.bank_account_id = cr.bank_account_id and CR1.cheque_range_type_code = ''MANUAL''),1)
--                        where CR.cheque_range_type_code = ''AUTO''';
    
    obfuscation_control.obfus_log(v_module || ': alter trigger '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD enable',p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);    
    execute immediate 'alter trigger '||p_src_prefix||'_CASH_MANAGEMENT.CHEQUE_RANGES_BRIUD enable';
    
    obfuscation_control.update_obfus_control(v_obfus_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, p_setup_cheque_ranges => 'Y');
    obfuscation_control.obfus_log(v_module || ' completed successfully for cheque_ranges with obfus_run_id = ' || v_obfus_run_id,p_src_prefix,p_anon_version,p_tgt_prefix,null,null,v_module);    
    
  end if; --p_cheque_ranges
  
end setup_obfus_env;
