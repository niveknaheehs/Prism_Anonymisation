WHENEVER SQLERROR EXIT SQL.SQLCODE
WHENEVER  OSERROR EXIT SQL.SQLCODE

set verify on;
set define on;

spool install_&&OS_OWNER._&&INSTANCE..log
PROMPT DROPPING USER &&OS_OWNER IF EXISTS

DECLARE
  X_USER_NOT_EXISTS EXCEPTION;
  PRAGMA EXCEPTION_INIT (X_USER_NOT_EXISTS, -01918);
  
  X_USER_CONNECTED EXCEPTION;
  PRAGMA EXCEPTION_INIT (X_USER_CONNECTED, -01940);   
BEGIN
  EXECUTE IMMEDIATE 'drop user &&OS_OWNER CASCADE';
EXCEPTION
  WHEN X_USER_NOT_EXISTS THEN
     NULL;
  WHEN X_USER_CONNECTED THEN
     RAISE;
END;
/

PROMPT CREATING USER &&OS_OWNER 
--ACCEPT os_owner_pwd CHAR PROMPT 'Type OS_OWNER Password:  ' HIDE
create user &&OS_OWNER identified by &&PASSWORD default tablespace PRISM_DATA;

PROMPT GRANTING PRIVILEGES TO &&OS_OWNER;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE, CREATE ANY TABLE, DROP ANY TABLE, CREATE SYNONYM, SELECT ANY DICTIONARY TO &&OS_OWNER;
GRANT DATAPUMP_IMP_FULL_DATABASE,DATAPUMP_EXP_FULL_DATABASE,IMP_FULL_DATABASE to &&OS_OWNER;
GRANT EXECUTE ON DBMS_LOCK TO &&OS_OWNER;
-- As DBA: GRANT EXECUTE ON SYS.UTL_RECOMP TO CPENNY with grant option;
--GRANT EXECUTE ON SYS.UTL_RECOMP TO KSHEEHAN with grant option; 
GRANT EXECUTE ON SYS.UTL_RECOMP TO &&OS_OWNER;
--, PSM_DEVELOPERS_DBA
GRANT MANAGE SCHEDULER TO &&OS_OWNER;
GRANT CREATE JOB TO &&OS_OWNER;

set verify on;
set define on;
PROMPT *******************************************************************************
PROMPT Add dbms_scheduler.add_event_queue_subscriber('&&OS_OWNER._obfus_jobs_agent')
PROMPT *******************************************************************************

BEGIN
  dbms_scheduler.remove_event_queue_subscriber('&&OS_OWNER._obfus_jobs_agent');
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  dbms_scheduler.add_event_queue_subscriber('&&OS_OWNER._obfus_jobs_agent');
END;
/

--ACCEPT psm_utilities_pwd CHAR PROMPT 'Type &&PRISM_PREFIX._PRISM_UTILITIES Password:  ' HIDE
--Remove the link creation , this should now be created manually before install is run

--BEGIN
--  execute immediate 'DROP PUBLIC DATABASE LINK SRC_LINK';
--EXCEPTION
--  WHEN OTHERS THEN NULL;
--END;
--/

--CREATE PUBLIC DATABASE LINK SRC_LINK
--   USING '&&PRISM_HOSTNAME:1521/&&PRISM_SERVICENAME';
--PSMPPSORA01:1521/OBFA'

BEGIN
  execute immediate 'DROP PUBLIC DATABASE LINK FROM_READ';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE PUBLIC DATABASE LINK FROM_READ USING '127.0.0.1:1521/&&INSTANCE';

BEGIN
  execute immediate 'DROP PUBLIC DATABASE LINK DBLINK_&&REF_SCHEMA';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE PUBLIC DATABASE LINK DBLINK_&&REF_SCHEMA
   CONNECT TO &&REF_SCHEMA IDENTIFIED BY &&REF_SCHEMA_PASSWORD
   USING '&&PRISM_HOSTNAME:1521/&&PRISM_SERVICENAME';


PROMPT CONNECTING &&OS_OWNER

CONN &&OS_OWNER/&&PASSWORD@&&OS_HOST:1521/&&INSTANCE

PROMPT ***************************
PROMPT Running sequences.sql
PROMPT ***************************
@sequences

PROMPT ***************************
PROMPT Running db_tables1.sql
PROMPT ***************************
@db_tables1

PROMPT ***************************
PROMPT Running db_tables2.sql
PROMPT ***************************
@db_tables2

set verify off;
set define on;

declare

  cursor c_tabs
  is
    select table_name
      from dba_tables
     where owner = upper('&&OS_OWNER')
       and (   table_name like'SS%'
            or table_name like'DD%'
            or table_name like'MD%' )
    order by table_name;			

  v_ddl    varchar2(4000);
  
begin

  for r in c_tabs
  loop
     begin
		  v_ddl := 'create or replace TRIGGER &&OS_OWNER..'||r.table_name||'_biu'||
					  ' BEFORE INSERT OR UPDATE ON &&OS_OWNER..'||r.table_name||
					  '   FOR EACH ROW
							 BEGIN
								IF INSERTING THEN
								   :NEW.created_ts := systimestamp;            
								   :NEW.modified_ts := systimestamp;
								ELSE
								   :NEW.modified_ts := systimestamp;
								END IF;
							 END;';
		  
		  dbms_output.put_line('Creating trigger '||r.table_name||'_biu');                  
		  execute immediate v_ddl;
	 exception
	    when others
	    then
		   dbms_output.put_line('v_ddl: '||v_ddl);	  
     end;
  end loop;
end;
/

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

PROMPT ***************************
PROMPT Running ss_schema_load_excl_config.sql
PROMPT ***************************
@ss_schema_load_excl_config.sql

PROMPT ***************************
PROMPT Running ss_load_excl_config.sql
PROMPT ***************************
@ss_load_excl_config.sql


PROMPT ***************************
PROMPT Running ss_load_incl_config.sql
PROMPT ***************************
@ss_load_incl_config.sql

PROMPT ***************************
PROMPT Running create_src_synonyms.sql
PROMPT ***************************
@create_src_synonyms.sql

PROMPT ***************************************************************************************************************************
PROMPT Creating public synonyms used for alter session set current_schema to target prism users to compile objects (such as triggers) 
PROMPT ***************************************************************************************************************************
create or replace public synonym ss_log_id_seq for &&OS_OWNER..ss_log_id_seq;
create or replace public synonym ss_log for &&OS_OWNER..ss_log;
create or replace public synonym ss_ctrl_exec_result for &&OS_OWNER..ss_ctrl_exec_result;

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

PROMPT ***********************************
PROMPT Compiling subsetting_control
PROMPT ***********************************
@subsetting_control.pks
@subsetting_control.pkb

PROMPT ***********************************
PROMPT Compiling ss_reporting
PROMPT ***********************************
@ss_reporting.pks
@ss_reporting.pkb

PROMPT *****************************************************
PROMPT Compiling procedure prc_create_src_synonyms
PROMPT *****************************************************
@prc_create_src_synonyms.sql

PROMPT *****************************************************
PROMPT Compiling procedure prc_drop_unused_src_synonyms
PROMPT *****************************************************
@prc_drop_unused_src_synonyms.sql

set verify off;
set define on;
--@insert_places.sql;
INSERT INTO &&OS_OWNER..places SELECT * FROM places@DBLINK_&&REF_SCHEMA;
commit;

set verify off;
set define on;
PROMPT Insert privacy_catalog
--@insert_privacy_catalog.sql;
INSERT INTO &&OS_OWNER..privacy_catalog SELECT * FROM privacy_catalog@DBLINK_&&REF_SCHEMA;
commit;

set verify off;
set define on;
PROMPT Insert pc_transform
--@insert_pc_transform.sql;
INSERT INTO &&OS_OWNER..pc_transform SELECT * FROM pc_transform@DBLINK_&&REF_SCHEMA;
commit;

set verify off;
set define on;
PROMPT Insert pc_obfuscatn_rules
--@insert_pc_obfuscatn_rules.sql;
INSERT INTO &&OS_OWNER..pc_obfuscatn_rules SELECT * FROM pc_obfuscatn_rules@DBLINK_&&REF_SCHEMA;
commit;

PROMPT Insert forename_seed
--@forename_seed.sql;
INSERT INTO &&OS_OWNER..forename_seed SELECT * FROM forename_seed@DBLINK_&&REF_SCHEMA;
commit;

set verify off;
set define on;
PROMPT Insert table_populated
--@insert_table_populated.sql;
INSERT INTO &&OS_OWNER..table_populated SELECT * FROM table_populated@DBLINK_&&REF_SCHEMA;
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
PROMPT Insert word_list
--@insert_word_list.sql;
INSERT INTO &&OS_OWNER..word_list SELECT * FROM word_list@DBLINK_&&REF_SCHEMA;
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
PROMPT Re-Compiling &OS_OWNER schema
PROMPT ***********************************

BEGIN
  DBMS_UTILITY.COMPILE_SCHEMA(upper('&&OS_OWNER'));
END;
/

set verify off;
set define on;
set timing on;

commit;
PROMPT Run patch_test_tables_populated.bat which calls .sql file:
PROMPT sqlplus cpenny/PT4_reader@PSMSHRDORA03:1521/PT4 @patch_test_tables_populated.sql
spool off