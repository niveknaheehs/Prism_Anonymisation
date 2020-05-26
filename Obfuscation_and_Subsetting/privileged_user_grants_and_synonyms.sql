grant select any table to cpenny;

synonyms:
CREATE OR REPLACE SYNONYM cpenny.anonymisation_process FOR &&ANON_OWNER..anonymisation_process;
CREATE OR REPLACE SYNONYM cpenny.obfuscation_control FOR &&ANON_OWNER..obfuscation_control; 
CREATE OR REPLACE SYNONYM cpenny.obfus_control FOR &&ANON_OWNER..obfus_control; 
CREATE OR REPLACE SYNONYM cpenny.obfus_run_id_seq FOR &&ANON_OWNER..obfus_run_id_seq; 
CREATE OR REPLACE SYNONYM cpenny.obfuscation_log FOR &&ANON_OWNER..obfuscation_log;
CREATE OR REPLACE SYNONYM cpenny.obfuscation_log_seq FOR &&ANON_OWNER..obfuscation_log_seq;
CREATE OR REPLACE SYNONYM cpenny.peripheral_tables FOR &&ANON_OWNER..peripheral_tables;
CREATE OR REPLACE SYNONYM cpenny.table_populated FOR &&ANON_OWNER..table_populated;
CREATE OR REPLACE SYNONYM cpenny.obfus_pre_check FOR &&ANON_OWNER..obfus_pre_check;
CREATE OR REPLACE SYNONYM cpenny.obfus_onoff_switch FOR &&ANON_OWNER..obfus_onoff_switch;
CREATE OR REPLACE SYNONYM cpenny.obfus_ctrl_stmts FOR &&ANON_OWNER..obfus_ctrl_stmts;
CREATE OR REPLACE SYNONYM cpenny.obfus_ctrl_manual_config FOR &&ANON_OWNER..obfus_ctrl_manual_config;
CREATE OR REPLACE SYNONYM cpenny.obfus_control_exec_result FOR &&ANON_OWNER..obfus_control_exec_result;
CREATE OR REPLACE SYNONYM cpenny.pc_transform FOR &&ANON_OWNER..pc_transform;
CREATE OR REPLACE SYNONYM cpenny.pc_transform_2 FOR &&ANON_OWNER..pc_transform_2;

CREATE OR REPLACE SYNONYM ksheehan.anonymisation_process FOR &&ANON_OWNER..anonymisation_process; 
CREATE OR REPLACE SYNONYM ksheehan.obfus_control FOR &&ANON_OWNER..obfus_control; 
CREATE OR REPLACE SYNONYM ksheehan.obfus_run_id_seq FOR &&ANON_OWNER..obfus_run_id_seq; 
CREATE OR REPLACE SYNONYM ksheehan.obfuscation_log FOR &&ANON_OWNER..obfuscation_log;
CREATE OR REPLACE SYNONYM ksheehan.obfuscation_log_seq FOR &&ANON_OWNER..obfuscation_log_seq;
CREATE OR REPLACE SYNONYM ksheehan.peripheral_tables FOR &&ANON_OWNER..peripheral_tables;
CREATE OR REPLACE SYNONYM ksheehan.table_populated FOR &&ANON_OWNER..table_populated;
CREATE OR REPLACE SYNONYM ksheehan.obfus_pre_check FOR &&ANON_OWNER..obfus_pre_check;
CREATE OR REPLACE SYNONYM ksheehan.obfus_onoff_switch FOR &&ANON_OWNER..obfus_onoff_switch;
CREATE OR REPLACE SYNONYM ksheehan.obfus_ctrl_stmts FOR &&ANON_OWNER..obfus_ctrl_stmts;
CREATE OR REPLACE SYNONYM ksheehan.obfus_ctrl_manual_config FOR &&ANON_OWNER..obfus_ctrl_manual_config;
CREATE OR REPLACE SYNONYM ksheehan.obfus_control_exec_result FOR &&ANON_OWNER..obfus_control_exec_result;
CREATE OR REPLACE SYNONYM ksheehan.pc_transform FOR &&ANON_OWNER..pc_transform;
CREATE OR REPLACE SYNONYM ksheehan.pc_transform_2 FOR &&ANON_OWNER..pc_transform_2;

grants:
GRANT SELECT, INSERT, UPDATE ON &&ANON_OWNER..obfus_control TO cpenny;
GRANT EXECUTE ON &&ANON_OWNER..anonymisation_process TO cpenny;
GRANT SELECT ON &&ANON_OWNER..obfus_run_id_seq TO cpenny;
GRANT INSERT, SELECT ON &&ANON_OWNER..obfuscation_log TO cpenny;
GRANT SELECT ON &&ANON_OWNER..obfuscation_log_seq TO cpenny;
GRANT EXECUTE ON &&ANON_OWNER..obfuscation_control TO cpenny;
GRANT ALL ON &&ANON_OWNER..peripheral_tables TO cpenny;
GRANT ALL ON &&ANON_OWNER..table_populated TO cpenny;
GRANT ALL ON &&ANON_OWNER..obfus_pre_check TO cpenny;
GRANT ALL ON &&ANON_OWNER..obfus_onoff_switch TO cpenny;
GRANT SELECT, INSERT, UPDATE ON &&ANON_OWNER..pc_transform TO cpenny;
GRANT SELECT, INSERT, UPDATE ON &&ANON_OWNER..pc_transform_2 TO cpenny;
GRANT SELECT, INSERT, UPDATE ON &&ANON_OWNER..obfus_ctrl_stmts TO cpenny;
GRANT SELECT, INSERT, UPDATE ON &&ANON_OWNER..obfus_ctrl_manual_config TO cpenny;
GRANT SELECT, INSERT, UPDATE, DELETE ON &&ANON_OWNER..obfus_control_exec_result TO cpenny;

GRANT ALL ON &&ANON_OWNER..peripheral_tables TO ksheehan;
GRANT ALL ON &&ANON_OWNER..table_populated TO ksheehan;
GRANT ALL ON &&ANON_OWNER..obfus_pre_check TO ksheehan;
GRANT ALL ON &&ANON_OWNER..obfus_onoff_switch TO ksheehan;
GRANT SELECT, INSERT, UPDATE ON &&ANON_OWNER..obfus_control TO ksheehan;
GRANT EXECUTE ON &&ANON_OWNER..anonymisation_process TO ksheehan;
GRANT SELECT ON &&ANON_OWNER..obfus_run_id_seq TO ksheehan;
GRANT INSERT, SELECT ON &&ANON_OWNER..obfuscation_log TO ksheehan;
GRANT SELECT ON &&ANON_OWNER..obfuscation_log_seq TO ksheehan;
--GRANT EXECUTE ON &&ANON_OWNER..calc_stats TO ksheehan;
GRANT EXECUTE ON &&ANON_OWNER..obfuscation_control TO ksheehan;
GRANT SELECT, INSERT, UPDATE, DELETE ON &&ANON_OWNER..pc_transform TO ksheehan;
GRANT SELECT, INSERT, UPDATE, DELETE ON &&ANON_OWNER..pc_transform_2 TO ksheehan;
GRANT SELECT, INSERT, UPDATE, DELETE ON &&ANON_OWNER..obfus_ctrl_stmts TO ksheehan;
GRANT SELECT, INSERT, UPDATE, DELETE ON &&ANON_OWNER..obfus_ctrl_manual_config TO ksheehan;
GRANT SELECT, INSERT, UPDATE, DELETE ON &&ANON_OWNER..obfus_control_exec_result TO ksheehan;

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
GRANT DROP ANY TABLE TO ksheehan;
GRANT DROP ANY TABLE TO cpenny;
GRANT SELECT ANY TABLE TO ksheehan;
GRANT SELECT ANY TABLE TO cpenny;
GRANT CREATE ANY INDEX TO ksheehan;
GRANT CREATE ANY INDEX TO cpenny;
GRANT ANALYZE ANY TO ksheehan;
GRANT ANALYZE ANY TO cpenny;

alter user cpenny quota 2000M on USERS;
alter user ksheehan quota 2000M on USERS;
  

 