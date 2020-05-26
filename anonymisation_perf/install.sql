WHENEVER SQLERROR EXIT SQL.SQLCODE
WHENEVER  OSERROR EXIT SQL.SQLCODE

set verify on;
set define on;

spool install_&&ANON_OWNER..log
PROMPT DROPPING USER &&ANON_OWNER IF EXISTS

DECLARE
  X_USER_NOT_EXISTS EXCEPTION;
  PRAGMA EXCEPTION_INIT (X_USER_NOT_EXISTS, -01918);
  
  X_USER_CONNECTED EXCEPTION;
  PRAGMA EXCEPTION_INIT (X_USER_CONNECTED, -01940);   
BEGIN
  EXECUTE IMMEDIATE 'drop user &&ANON_OWNER CASCADE';
EXCEPTION
  WHEN X_USER_NOT_EXISTS THEN
     NULL;
  WHEN X_USER_CONNECTED THEN
     RAISE;
END;
/

PROMPT CREATING USER &&ANON_OWNER 
create user &&ANON_OWNER identified by &&PASSWORD default tablespace PRISM_DATA;

PROMPT GRANTING PRIVILEGES TO &&ANON_OWNER;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE, CREATE ANY TABLE, DROP ANY TABLE, PSM_DEVELOPERS_DBA TO &&ANON_OWNER;
GRANT EXECUTE ON DBMS_LOCK TO &&ANON_OWNER;

PROMPT CONNECTING &&ANON_OWNER
CONN &&ANON_OWNER/&&PASSWORD@&&INSTANCE

PROMPT ***************************
PROMPT Running db_tables1.sql
PROMPT ***************************
@db_tables1

PROMPT ***************************
PROMPT Running db_tables2.sql
PROMPT ***************************
@db_tables2

PROMPT ***************************
PROMPT Running sequences.sql
PROMPT ***************************
@sequences

PROMPT ***********************************
PROMPT Compiling Constants package spec
PROMPT ***********************************
@const.pks

PROMPT ***********************************
PROMPT Compiling Exceptions package spec
PROMPT ***********************************
@excep.pks

PROMPT ***********************************
PROMPT Compiling Utility Package ut
PROMPT ***********************************
@ut.pks
@ut.pkb

PROMPT ***********************************
PROMPT Compiling Globals Package
PROMPT ***********************************
@gp.pks
@gp.pkb

PROMPT ***********************************
PROMPT Compiling anonymisation_process
PROMPT ***********************************
@anonymisation_process.pks
@anonymisation_process.pkb

PROMPT ***********************************
PROMPT Compiling obfuscation_control
PROMPT ***********************************
@obfuscation_control.pks
@obfuscation_control.pkb

set verify off;
set define on;
PROMPT Insert privacy_catalog
@insert_privacy_catalog.sql;
commit;

set verify off;
set define on;
PROMPT Insert pc_transform
@insert_pc_transform.sql;
commit;

set verify off;
set define on;
PROMPT Insert pc_obfuscatn_rules
@insert_pc_obfuscatn_rules.sql;
commit;



set verify off;
set define on;
PROMPT Insert table_populated
@insert_table_populated.sql;
commit;

set verify off;
set define on;
PROMPT Insert obfus_ctrl_manual_config
@insert_obfus_ctrl_manual_config.sql;
commit;

set verify off;
set define on;
PROMPT Insert peripheral_tables_load
@insert_per_tables_load.sql;
commit;

set verify off;
set define on;
PROMPT Insert per_col_mask_overide_load
@insert_per_col_mask_overide_load.sql;
commit;



set verify off;
set define on;
PROMPT Insert PSEUDO_CONS_COLUMNS
@insert_PSEUDO_CONS_COLUMNS.sql;
commit;

set verify off;
set define on;
PROMPT delete_non_existent_tab_cols
@delete_non_existent_tab_cols.sql;
commit;

set verify on;
set define on;
PROMPT *******************************************************************************
PROMPT Add dbms_scheduler.add_event_queue_subscriber('&&ANON_OWNER._obfus_jobs_agent')
PROMPT *******************************************************************************

BEGIN
  dbms_scheduler.remove_event_queue_subscriber('&&ANON_OWNER._obfus_jobs_agent');
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  dbms_scheduler.add_event_queue_subscriber('&&ANON_OWNER._obfus_jobs_agent');
END;
/

set verify on;
set define on;
PROMPT ***********************************
PROMPT Re-Compiling &ANON_OWNER schema
PROMPT ***********************************

BEGIN
  DBMS_UTILITY.COMPILE_SCHEMA('&&ANON_OWNER');
END;
/

set verify off;
set define on;
set timing on;
PROMPT Insert places/word_list/forename_seed should be run in another session or using datapump as it takes a long time 
PROMPT (insert_places.sql/insert_word_list.sql/;
PROMPT This way setup of ANON environment and stats collection can run in parallel

--@insert_places.sql;
--INSERT INTO &&ANON_OWNER..places SELECT * FROM ANONDEV_ANONYMISE.places;
--commit;
--PROMPT Insert word_list
--@insert_word_list.sql;
--commit;
--PROMPT Insert forename_seed
--@forename_seed.sql;
commit;


spool off
