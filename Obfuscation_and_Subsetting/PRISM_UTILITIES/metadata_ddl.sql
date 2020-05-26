WHENEVER SQLERROR EXIT SQL.SQLCODE
WHENEVER  OSERROR EXIT SQL.SQLCODE


spool install_&&OWNER..log


set serveroutput on;
set verify on;
set define on;
grant select on &&PREFIX._PRISM_CORE.companies to &&OWNER;
grant select any table to &&OWNER;
GRANT CREATE ANY JOB TO &&OWNER;
grant analyze any to &&OWNER;
grant execute on DBMS_LOCK to &&OWNER;
--GRANT EXECUTE ON DBMS_SCHEDULER TO &&OWNER;
GRANT MANAGE SCHEDULER TO &&OWNER;
GRANT DATAPUMP_IMP_FULL_DATABASE,DATAPUMP_EXP_FULL_DATABASE,IMP_FULL_DATABASE to &&OWNER;
GRANT UNLIMITED TABLESPACE TO &&OWNER;
grant select any dictionary to &&OWNER;
grant execute on DBMS_UTILITY to &&OWNER;

ALTER SESSION SET CURRENT_SCHEMA = &&OWNER;


create or replace synonym &&OWNER..companies for &&PREFIX._PRISM_CORE.companies;
create or replace synonym &&OWNER..prism_data_base_versions for &&PREFIX._PRISM_CORE.prism_data_base_versions;

create or replace procedure prc_drop_table (p_table_name varchar2)
is
  x_table_not_exist EXCEPTION;
  PRAGMA exception_init (x_table_not_exist, -00942);
BEGIN
  execute immediate 'DROP TABLE &&OWNER..'||p_table_name;
EXCEPTION
  WHEN x_table_not_exist THEN NULL;
END;
/ 
 
create or replace procedure prc_drop_sequence (p_sequence_name varchar2)
is
  x_sequence_not_exist EXCEPTION;
  PRAGMA exception_init (x_sequence_not_exist, -02289); 
begin 
  execute immediate 'DROP SEQUENCE  &&OWNER..'||p_sequence_name;
exception
  when x_sequence_not_exist then
    null;
end;   
/ 
 
exec prc_drop_table('md_ddl_parts');
exec prc_drop_table('md_ddl'); 
exec prc_drop_table('md_object_types'); 

--------------------------------------------------------
PROMPT Creating table md_object_types
--------------------------------------------------------
CREATE TABLE &&OWNER..md_object_types 
( object_type_id   NUMBER,   
  object_type      VARCHAR2(128),
  created_ts       timestamp,
  modified_ts      timestamp, 
 PRIMARY KEY (object_type_id));

 CREATE UNIQUE INDEX &&OWNER..md_object_types_uk1 ON &&OWNER..md_object_types (object_type);
 
--grant select on ANONPRE9801_PRISM_UTILITIES.object_types to CPENNY;
--grant select on ANONPRE9801_PRISM_UTILITIES.md_ddl to CPENNY;
--select distinct object_type from md_ddl;

insert all
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 1,'USER')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 2,'DEFAULT_ROLE')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 3,'ROLE_GRANT')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 4,'SYSTEM_GRANT')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 5,'OBJECT_GRANT')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 6,'TABLE')  
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 7,'VIEW')    
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 8,'SYNONYM')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 9,'SEQUENCE')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 10,'FUNCTION')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values( 11,'PROCEDURE')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values(12,'PACKAGE')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values(13,'PARTITION')
  into &&OWNER..md_object_types(object_type_id,object_type)
  values(14,'INDEX')   
  into &&OWNER..md_object_types(object_type_id,object_type)
  values(15,'REF_CONSTRAINT')  
  into &&OWNER..md_object_types(object_type_id,object_type)
  values(16,'TRIGGER')   
  select * from dual;
commit;
--select * from md_object_types;   

--------------------------------------------------------
PROMPT Creating table md_ddl
--------------------------------------------------------
CREATE TABLE &&OWNER..md_ddl 
( md_ddl_id          NUMBER,  
  actual_owner             varchar2(128),     
  owner                    varchar2(128),    
  object_type              varchar2(128),
  object_name              varchar2(128),
  base_object_name         varchar2(128),
  object_ddl               CLOB,
  object_ddl_length        NUMBER,
  object_xml               XMLTYPE,
  object_cre_seq           number, 
  relational_level         number, 
  PARTITIONING_TYPE        varchar2(10),
  SUBPARTITIONING_TYPE     varchar2(10),
  ref_ptn_constraint_name  varchar2(128),
  view_name                varchar2(128),
  has_large_object         varchar2(1) default 'N',
  dp_YN                    varchar2(1) default 'N',
  created_ts               timestamp,
  modified_ts              timestamp
 --,CONSTRAINT fk_ot FOREIGN KEY (object_type) REFERENCES &&OWNER..md_object_types (object_type)
);  

ALTER TABLE &&OWNER..md_ddl ADD CONSTRAINT pk_md_ddl PRIMARY KEY (md_ddl_id); 
ALTER TABLE &&OWNER..md_ddl ADD CONSTRAINT chk_has_large_object CHECK (has_large_object IN ( 'Y', 'N')); 

   
--------------------------------------------------------
PROMPT Creating Sequence md_seq
--------------------------------------------------------   
exec prc_drop_sequence('md_seq');
 
CREATE SEQUENCE  &&OWNER..md_seq  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 32001 CACHE 1000;   
                                   
                           
CREATE TABLE &&OWNER..md_ddl_parts 
( part_seq_id        number,
  md_ddl_id          number,
  object_name        varchar2(128),
  object_ddl         varchar2(4000), 
  created_ts         timestamp,
  modified_ts        timestamp,  
  PRIMARY KEY (md_ddl_id,part_seq_id),
  CONSTRAINT fk_mdp  FOREIGN KEY (md_ddl_id) REFERENCES &&OWNER..md_ddl (md_ddl_id)
);
      
--------------------------------------------------------
PROMPT Creating Sequence md_part_seq
-------------------------------------------------------- 
exec prc_drop_sequence('md_part_seq');

CREATE SEQUENCE  &&OWNER..md_part_seq  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 1000;
   
--------------------------------------------------------
PROMPT Creating table MD_REF_PART_ORDER 
--------------------------------------------------------   
exec prc_drop_table('MD_REF_PART_ORDER'); 

   CREATE TABLE &&OWNER..MD_REF_PART_ORDER
   ( IN_LINK     VARCHAR2(128), 
     OUT_LINK    VARCHAR2(128), 
     TABLE_NAME  VARCHAR2(128), 
     ALEVEL      NUMBER, 
     PATH        VARCHAR2(4000), 
     CREATED_TS  TIMESTAMP (6), 
     MODIFIED_TS TIMESTAMP (6)
   );
   
   --CONSTRAINT PK_REF_PART_ORDER PRIMARY KEY (IN_LINK,OUT_LINK));
       
	   
--------------------------------------------------------
PROMPT Creating table md_obj_ddl_exclusions 
--------------------------------------------------------   
exec prc_drop_table('md_obj_ddl_exclusions'); 

DECLARE
  x_table_not_exist EXCEPTION;
  PRAGMA exception_init (x_table_not_exist, -00942);
BEGIN
  execute immediate 'DROP TABLE &&OWNER..md_obj_ddl_exclusions';
EXCEPTION
  WHEN x_table_not_exist THEN NULL;
END;
/  
   CREATE TABLE &&OWNER..md_obj_ddl_exclusions		
 (	OWNER VARCHAR2(128 BYTE), 
	ACTUAL_OWNER VARCHAR2(128 BYTE), 
	OBJECT_TYPE VARCHAR2(128 BYTE), 
	OBJECT_NAME VARCHAR2(128 BYTE), 
	CREATED_TS TIMESTAMP (6), 
	MODIFIED_TS TIMESTAMP (6)
   );       		

--------------------------------------------------------
PROMPT Creating table MD_GEN_METADATA_SUMMARY 
--------------------------------------------------------   
exec prc_drop_table('MD_GEN_METADATA_SUMMARY'); 

   CREATE TABLE &&OWNER..MD_GEN_METADATA_SUMMARY
   ( PSM_PREFIX VARCHAR2(30 BYTE), 
	 DB_VERSION VARCHAR2(11 BYTE), 
	 START_TS TIMESTAMP (6), 
	 END_TS TIMESTAMP (6), 
	 DURATION_SECONDS NUMBER(38,3) GENERATED ALWAYS AS (EXTRACT(DAY FROM (END_TS-START_TS)DAY(9) TO SECOND(6))*24*60*60+EXTRACT(HOUR FROM (END_TS-START_TS)DAY(9) TO SECOND(6))*60*60+EXTRACT(MINUTE FROM (END_TS-START_TS)DAY(9) TO SECOND(6))*60+EXTRACT(SECOND FROM (END_TS-START_TS)DAY(9) TO SECOND(6))) VIRTUAL , 
	 CREATED_TS TIMESTAMP (6), 
	 MODIFIED_TS TIMESTAMP (6)
   );		
		
 --------------------------------------------------------
PROMPT Creating table ss_companies  
--------------------------------------------------------  
exec prc_drop_table('ss_companies'); 

CREATE TABLE &&OWNER..ss_companies (
   comp_code           VARCHAR2(10),
   CREATED_TS          TIMESTAMP (6), 
   MODIFIED_TS         TIMESTAMP (6),   
   CONSTRAINT PK_ss_companies PRIMARY KEY (comp_code)
);        
        
--------------------------------------------------------
PROMPT Creating table SS_GRAPH 
--------------------------------------------------------     
exec prc_drop_table('SS_GRAPH'); 
  
   CREATE TABLE &&OWNER..SS_GRAPH
   ( LINK_TYPE   VARCHAR2(15), 
     IN_LINK     VARCHAR2(128), 
     OUT_LINK    VARCHAR2(128), 
     CREATED_TS  TIMESTAMP(6), 
     MODIFIED_TS TIMESTAMP(6)
   );

--------------------------------------------------------
PROMPT Creating table SS_NODES 
--------------------------------------------------------

exec prc_drop_table('SS_NODES');

  CREATE TABLE &&OWNER..SS_NODES
  ( CONSTRAINT_NAME  VARCHAR2(128), 
     OBJECT_NAME      VARCHAR2(128), 
     COLS             VARCHAR2(4000), 
     COL_TYPES        VARCHAR2(4000), 
     NULLABLE         VARCHAR2(4000), 
     CREATED_TS       TIMESTAMP(6), 
     MODIFIED_TS      TIMESTAMP(6),
  CONSTRAINT PK_NODES PRIMARY KEY (CONSTRAINT_NAME)
  );

--------------------------------------------------------
PROMPT Creating table SS_SCHEMA_LIST 
--------------------------------------------------------
   
exec prc_drop_table('SS_SCHEMA_LIST');
   
CREATE TABLE &&OWNER..SS_SCHEMA_LIST
   (  SRC_SCHEMA   VARCHAR2(128),
      CREATED_TS   TIMESTAMP(6),
      MODIFIED_TS  TIMESTAMP(6),
  CONSTRAINT PK_SCHEMA_LIST PRIMARY KEY (SRC_SCHEMA)
  );
   
--------------------------------------------------------
PROMPT Creating table SS_TABLE_SPLITS 
--------------------------------------------------------  
   
exec prc_drop_table('SS_TABLE_SPLITS');
   
  CREATE TABLE &&OWNER..SS_TABLE_SPLITS
  (	SCHEMA_NAME VARCHAR2(128 BYTE), 
	  TABLE_LIST CLOB, 
	  ASEQ NUMBER, 
	  CREATED_TS TIMESTAMP (6), 
	  MODIFIED_TS TIMESTAMP (6), 
	  NUM_TABLES NUMBER
   );   
   
--------------------------------------------------------
PROMPT Creating Sequence TAB_SPLIT_SEQ
--------------------------------------------------------
    
exec prc_drop_sequence('TAB_SPLIT_SEQ');	  
CREATE SEQUENCE  &&OWNER..TAB_SPLIT_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1;   
  
--------------------------------------------------------
PROMPT Creating table SS_GRAPH_SORT 
--------------------------------------------------------   
exec prc_drop_table('SS_GRAPH_SORT');

  CREATE TABLE &&OWNER..SS_GRAPH_SORT 
   (	PARENT_TABLE_OWNER VARCHAR2(128 BYTE), 
	CHILD_TABLE_OWNER VARCHAR2(128 BYTE), 
	PAR_TABLE_ACTUAL_OWNER VARCHAR2(128 BYTE), 
	CHILD_TABLE_ACTUAL_OWNER VARCHAR2(128 BYTE), 
	PARENT_TABLE VARCHAR2(128 BYTE), 
	CHILD_TABLE VARCHAR2(128 BYTE), 
	PARENT_TABLE_ORDER NUMBER, 
	CHILD_TABLE_ORDER NUMBER, 
	CREATED_TS TIMESTAMP (6), 
	MODIFIED_TS TIMESTAMP (6)
   );      
 
--------------------------------------------------------
PROMPT Creating table SS_TABLE_LIST_SORT 
--------------------------------------------------------   
exec prc_drop_table('SS_TABLE_LIST_SORT');

  CREATE TABLE &&OWNER..SS_TABLE_LIST_SORT 
   (	OWNER VARCHAR2(128 BYTE), 
	ACTUAL_OWNER VARCHAR2(128 BYTE), 
	TABLE_NAME VARCHAR2(128 BYTE), 
	TABLE_ORDER NUMBER, 
	CREATED_TS TIMESTAMP (6), 
	MODIFIED_TS TIMESTAMP (6), 
	TABLE_ID NUMBER
   ); 
  
   
--------------------------------------------------------
PROMPT Creating Table UTIL_LOG
--------------------------------------------------------
exec prc_drop_table('UTIL_LOG');

CREATE TABLE &&OWNER..UTIL_LOG 
(	LOG_ID NUMBER, 
	LOG_MSG VARCHAR2(4000), 
	MOD_TIMESTAMP TIMESTAMP (6) WITH LOCAL TIME ZONE, 
	ERR_CODE NUMBER, 
	ERRM VARCHAR2(4000), 
	MODULE VARCHAR2(4000)
);
 
ALTER TABLE &&OWNER..UTIL_LOG add constraint pk_SUBSETTING_LOG primary key(LOG_ID);  
  
--------------------------------------------------------
PROMPT Creating Sequence util_log_id_seq
--------------------------------------------------------   
exec prc_drop_sequence('util_log_id_seq');
   CREATE SEQUENCE  &&OWNER..util_log_id_seq  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1; 
   
--------------------------------------------------------
PROMPT Creating table DD_USERS 
--------------------------------------------------------
exec prc_drop_table('dd_users');

CREATE TABLE &&OWNER..dd_users (
  USERNAME VARCHAR2(128 BYTE) NOT NULL ENABLE,
  created_ts             timestamp,
  modified_ts            timestamp  
);

--------------------------------------------------------
PROMPT Creating table DD_TABLES 
--------------------------------------------------------
exec prc_drop_table('DD_TABLES');

CREATE TABLE &&OWNER..DD_TABLES 
(  ACTUAL_OWNER         VARCHAR2(128) NOT NULL ENABLE, 
   OWNER                VARCHAR2(128) NOT NULL ENABLE, 
   TABLE_NAME           VARCHAR2(128) NOT NULL ENABLE, 
   TABLESPACE_NAME      VARCHAR2(30), 
   NUM_ROWS             NUMBER,				
   TEMPORARY            VARCHAR2(1),
   IOT_NAME             VARCHAR2(128),
   IOT_TYPE             VARCHAR2(12),
   CRE_ORDER            NUMBER,
   created_ts           timestamp,
   modified_ts          timestamp, 
 CONSTRAINT PK_DD_TABLES PRIMARY KEY (OWNER, TABLE_NAME) 
);

--------------------------------------------------------
PROMPT Creating table DD_VIEWS 
--------------------------------------------------------
exec prc_drop_table('DD_VIEWS');

CREATE TABLE &&OWNER..DD_VIEWS 
 ( ACTUAL_OWNER         VARCHAR2(128) NOT NULL ENABLE, 
   OWNER                VARCHAR2(128) NOT NULL ENABLE, 
   VIEW_NAME            VARCHAR2(128) NOT NULL ENABLE,
   created_ts           timestamp,
   modified_ts          timestamp,  
 CONSTRAINT PK_DD_VIEWS PRIMARY KEY (OWNER, VIEW_NAME) 
); 
   
--------------------------------------------------------
PROMPT Creating table DD_PART_TABLES 
--------------------------------------------------------
exec prc_drop_table('dd_part_tables');

  CREATE TABLE &&OWNER..dd_part_tables (
    ACTUAL_OWNER                   VARCHAR2(128), 
    OWNER                          VARCHAR2(128),  
    TABLE_NAME                     VARCHAR2(128),  
    PARTITIONING_TYPE              VARCHAR2(9),    
    SUBPARTITIONING_TYPE           VARCHAR2(9),  
	created_ts             timestamp,
    modified_ts            timestamp,  
   CONSTRAINT PK_DD_PART_TABLES PRIMARY KEY (OWNER, TABLE_NAME)
  );
  
   
--------------------------------------------------------
PROMPT Creating table DD_CONSTRAINTS 
--------------------------------------------------------
--------------------------------------------------------
exec prc_drop_table('DD_CONSTRAINTS');

CREATE TABLE &&OWNER..DD_CONSTRAINTS 
(	ACTUAL_OWNER VARCHAR2(128), 
	OWNER VARCHAR2(128), 
	CONSTRAINT_NAME VARCHAR2(128), 
	CONSTRAINT_TYPE VARCHAR2(1), 
	TABLE_NAME VARCHAR2(128), 
	ACTUAL_R_OWNER VARCHAR2(128), 
	R_OWNER VARCHAR2(128), 
	R_CONSTRAINT_NAME VARCHAR2(128), 
	ACTUAL_INDEX_OWNER VARCHAR2(128), 
	INDEX_OWNER VARCHAR2(128), 
	INDEX_NAME VARCHAR2(128),
	created_ts             timestamp,
    modified_ts            timestamp, 	
	CONSTRAINT PK_DD_CONSTRAINTS PRIMARY KEY (OWNER, TABLE_NAME, CONSTRAINT_NAME)
);      
   
    
--------------------------------------------------------
PROMPT Creating table DD_CONS_COLUMNS 
--------------------------------------------------------
exec prc_drop_table('DD_CONS_COLUMNS');

  CREATE TABLE &&OWNER..DD_CONS_COLUMNS
   ( ACTUAL_OWNER    VARCHAR2(128) NOT NULL ENABLE, 
	 OWNER           VARCHAR2(128), 
	 CONSTRAINT_NAME VARCHAR2(128) NOT NULL ENABLE, 
	 TABLE_NAME      VARCHAR2(128) NOT NULL ENABLE, 
	 COLUMN_NAME     VARCHAR2(4000), 
	 POSITION        NUMBER,
	 created_ts             timestamp,
     modified_ts            timestamp,    
	CONSTRAINT PK_DD_CONS_COLUMNS PRIMARY KEY (OWNER, CONSTRAINT_NAME, COLUMN_NAME) 
   ); 
  
  
--------------------------------------------------------
PROMPT Creating table DD_TAB_COLUMNS 
--------------------------------------------------------   
 
exec prc_drop_table('DD_TAB_COLUMNS');
 
   CREATE TABLE &&OWNER..DD_TAB_COLUMNS
   ( ACTUAL_OWNER  VARCHAR2(128) NOT NULL ENABLE, 
     OWNER         VARCHAR2(128) NOT NULL ENABLE, 
     TABLE_NAME    VARCHAR2(128) NOT NULL ENABLE, 
     COLUMN_NAME   VARCHAR2(128) NOT NULL ENABLE, 
     DATA_TYPE     VARCHAR2(128),
     DATA_LENGTH     NUMBER             NULL, 
     DATA_PRECISION  NUMBER,
     DATA_SCALE      NUMBER,	 
     NULLABLE        VARCHAR2(1),
     column_id           number,
	 character_set_name  VARCHAR2(44),
	 created_ts             timestamp,
     modified_ts            timestamp, 
   CONSTRAINT PK_DD_TAB_COLUMNS PRIMARY KEY (OWNER, TABLE_NAME, COLUMN_NAME)
   ); 
   

--------------------------------------------------------
PROMPT Creating table dd_tab_partitions 
--------------------------------------------------------   
exec prc_drop_table('dd_tab_partitions');
 
   CREATE TABLE &&OWNER..dd_tab_partitions ( 
     ACTUAL_TABLE_OWNER       VARCHAR2(128) NOT NULL ENABLE,
     TABLE_OWNER              VARCHAR2(128) NOT NULL ENABLE, 
     TABLE_NAME               VARCHAR2(128) NOT NULL ENABLE, 
     PARTITION_NAME           VARCHAR2(128) NOT NULL ENABLE,
     NUM_ROWS                 NUMBER,
     LAST_ANALYZED            DATE,
	 created_ts             timestamp,
     modified_ts            timestamp, 	 
    CONSTRAINT PK_DD_TAB_PARTITIONS PRIMARY KEY (ACTUAL_TABLE_OWNER, TABLE_OWNER, TABLE_NAME, PARTITION_NAME)	 
   );   
     
   
--------------------------------------------------------
PROMPT Creating table DD_OBJECTS 
--------------------------------------------------------
exec prc_drop_table('dd_objects');

  CREATE TABLE &&OWNER..dd_objects (  
     OBJECT_ID        NUMBER         NOT NULL,
     ACTUAL_OWNER     VARCHAR2(128)  NOT NULL,
     OWNER            VARCHAR2(128),
     OBJECT_TYPE      VARCHAR2(23),
     OBJECT_NAME      VARCHAR2(128) NOT NULL,
     LAST_DDL_TIME    DATE,
     STATUS           VARCHAR2(7),
     TEMPORARY        VARCHAR2(1),	 	     
	 created_ts             timestamp,
     modified_ts            timestamp, 	 
     CONSTRAINT PK_DD_OBJECTS PRIMARY KEY (OBJECT_ID)
  );      
   
--------------------------------------------------------
PROMPT Creating table DD_TAB_PRIVS 
--------------------------------------------------------
exec prc_drop_table('DD_TAB_PRIVS');

  CREATE TABLE &&OWNER..DD_TAB_PRIVS 
   ( GRANTEE          VARCHAR2(128 BYTE), 
     ACTUAL_OWNER     VARCHAR2(128 BYTE),   
	   OWNER            VARCHAR2(128 BYTE), 
	   TABLE_NAME       VARCHAR2(128 BYTE), 
	   GRANTOR          VARCHAR2(128 BYTE), 
	   PRIVILEGE        VARCHAR2(40 BYTE), 
	   GRANTABLE        VARCHAR2(3 BYTE), 
	   HIERARCHY        VARCHAR2(3 BYTE), 
     COMMON           VARCHAR2(3 BYTE), 
  	 TYPE             VARCHAR2(24 BYTE), 
  	 INHERITED        VARCHAR2(3 BYTE),
	 created_ts             timestamp,
     modified_ts            timestamp	 
   );  
   
--------------------------------------------------------
PROMPT Creating table DD_IND_COLUMNS 
--------------------------------------------------------
exec prc_drop_table('DD_IND_COLUMNS');

  CREATE TABLE &&OWNER..DD_IND_COLUMNS 
(	ACTUAL_INDEX_OWNER VARCHAR2(128) NOT NULL ENABLE, 
	INDEX_OWNER VARCHAR2(128), 
	INDEX_NAME VARCHAR2(128) NOT NULL ENABLE, 
	ACTUAL_TABLE_OWNER VARCHAR2(128) NOT NULL ENABLE, 
	TABLE_OWNER VARCHAR2(128), 
	TABLE_NAME VARCHAR2(128) NOT NULL ENABLE, 
	COLUMN_NAME VARCHAR2(4000), 
	COLUMN_POSITION NUMBER NOT NULL ENABLE, 
	COLUMN_LENGTH NUMBER NOT NULL ENABLE, 
	CHAR_LENGTH NUMBER,
	 created_ts             timestamp,
     modified_ts            timestamp	
   );   

--------------------------------------------------------
PROMPT Creating table DD_INDEXES 
--------------------------------------------------------
exec prc_drop_table('DD_INDEXES');

  CREATE TABLE &&OWNER..DD_INDEXES (
	 ACTUAL_OWNER         VARCHAR2(128) NOT NULL ENABLE,  
	 OWNER                VARCHAR2(128),
	 INDEX_NAME           VARCHAR2(128) NOT NULL ENABLE,
	 INDEX_TYPE           VARCHAR2(27),
	 ACTUAL_TABLE_OWNER   VARCHAR2(128) NOT NULL ENABLE,
	 TABLE_OWNER          VARCHAR2(128),
	 TABLE_NAME           VARCHAR2(128) NOT NULL ENABLE,
	 TABLE_TYPE           VARCHAR2(11),   
	 created_ts           timestamp,
     modified_ts          timestamp	
   );    
   
--------------------------------------------------------
PROMPT Creating table DD_TAB_COL_STATS 
--------------------------------------------------------
exec prc_drop_table('DD_TAB_COL_STATS');

  CREATE TABLE &&OWNER..DD_TAB_COL_STATS 
   (	OWNER VARCHAR2(128), 
	ACTUAL_OWNER VARCHAR2(128), 
	TABLE_NAME VARCHAR2(128), 
	COLUMN_NAME VARCHAR2(128), 
	NUM_DISTINCT NUMBER, 
	LOW_VALUE RAW(2000), 
	HIGH_VALUE RAW(2000), 
	DENSITY NUMBER, 
	NUM_NULLS NUMBER, 
	NUM_BUCKETS NUMBER, 
	LAST_ANALYZED DATE, 
	SAMPLE_SIZE NUMBER, 
	GLOBAL_STATS VARCHAR2(3), 
	USER_STATS VARCHAR2(3), 
	NOTES VARCHAR2(80), 
	AVG_COL_LEN NUMBER, 
	HISTOGRAM VARCHAR2(15), 
	SCOPE VARCHAR2(7),
	 created_ts             timestamp,
     modified_ts            timestamp	
   );
   
--------------------------------------------------------
PROMPT Creating table DD_TAB_STATS 
--------------------------------------------------------
exec prc_drop_table('DD_TAB_STATS');

  CREATE TABLE &&OWNER..DD_TAB_STATS 
   (	OWNER VARCHAR2(128), 
	ACTUAL_OWNER VARCHAR2(128), 
	TABLE_NAME VARCHAR2(128), 
	PARTITION_NAME VARCHAR2(128), 
	PARTITION_POSITION NUMBER, 
	SUBPARTITION_NAME VARCHAR2(128), 
	SUBPARTITION_POSITION NUMBER, 
	OBJECT_TYPE VARCHAR2(12), 
	NUM_ROWS NUMBER, 
	BLOCKS NUMBER, 
	EMPTY_BLOCKS NUMBER, 
	AVG_SPACE NUMBER, 
	CHAIN_CNT NUMBER, 
	AVG_ROW_LEN NUMBER, 
	AVG_SPACE_FREELIST_BLOCKS NUMBER, 
	NUM_FREELIST_BLOCKS NUMBER, 
	AVG_CACHED_BLOCKS NUMBER, 
	AVG_CACHE_HIT_RATIO NUMBER, 
	IM_IMCU_COUNT NUMBER, 
	IM_BLOCK_COUNT NUMBER, 
	IM_STAT_UPDATE_TIME TIMESTAMP (9), 
	SCAN_RATE NUMBER, 
	SAMPLE_SIZE NUMBER, 
	LAST_ANALYZED DATE, 
	GLOBAL_STATS VARCHAR2(3), 
	USER_STATS VARCHAR2(3), 
	STATTYPE_LOCKED VARCHAR2(5), 
	STALE_STATS VARCHAR2(3), 
	SCOPE VARCHAR2(7),
    created_ts             timestamp,
    modified_ts            timestamp  
);             
   
--------------------------------------------------------
PROMPT Creating table dd_synonyms 
--------------------------------------------------------
exec prc_drop_table('dd_synonyms');
   
create table &&OWNER..dd_synonyms
(
  OWNER              VARCHAR2(128),
  ACTUAL_OWNER       VARCHAR2(128), 
  SYNONYM_NAME       VARCHAR2(128), 
  TABLE_OWNER        VARCHAR2(128), 
  TABLE_NAME         VARCHAR2(128), 
  DB_LINK            VARCHAR2(128), 
  ORIGIN_CON_ID      NUMBER,
  created_ts             timestamp,
  modified_ts            timestamp  
);
                   
--------------------------------------------------------
PROMPT Creating table TEST_SWAP 
--------------------------------------------------------  
exec prc_drop_table('TEST_SWAP');
 
create table &&OWNER..TEST_SWAP 
   (	PAR_OWNER VARCHAR2(128 BYTE), 
	PAR_TAB VARCHAR2(128 BYTE), 
	CHILD_OWNER VARCHAR2(128 BYTE), 
	CHILD_TAB VARCHAR2(128 BYTE), 
	THECOUNT NUMBER
   );
   

--------------------------------------------------------
PROMPT Creating table ss_parallel_load_config
--------------------------------------------------------  

exec prc_drop_table('ss_parallel_load_config');

  CREATE TABLE &&OWNER..ss_parallel_load_config ( 
  actual_owner           VARCHAR2(128)  NOT NULL, 
  owner                  VARCHAR2(128)  NOT NULL,
  table_name             VARCHAR2(128)  NOT NULL,
  parallel_load          VARCHAR2(1)    DEFAULT 'Y' NOT NULL,
  created_ts             timestamp,
  modified_ts            timestamp  
);

ALTER TABLE &&OWNER..ss_parallel_load_config ADD CONSTRAINT chk_parallel_load_yn CHECK (parallel_load IN ('Y','N'));    
ALTER TABLE &&OWNER..ss_parallel_load_config add constraint pk_ss_parallel_load_config primary key(actual_owner,owner,table_name);   
   
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_TRANS_SHARE_ALLOCATIONS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SHARE_HOLDING_DISPS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_CASH_MOVEMENTS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','DISCRET_OPT_TRANSACTIONS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_CASH_CONTRIBS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_MATCH_SHARE_BAND_ALLOCS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'CASH_MANAGEMENT','CASH_MANAGEMENT','PAYMENT_STATUS_HISTORY','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'CASH_MANAGEMENT','CASH_MANAGEMENT','CASH_TRANSACTIONS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'CASH_MANAGEMENT','CASH_MANAGEMENT','PAYMENTS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_PAYMENT_DETAILS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_TRANS_CONTRIBUTIONS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_TRANS_CONTRIB_PAYMENTS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_TRANS_CONTRIB_RECEIPTS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_SHARE_MOVEMENTS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_SHARE_HOLDINGS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_PAYMENTS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','SIP_TRANSACTIONS','Y');
insert into ss_parallel_load_config (actual_owner, owner, table_name, parallel_load) values (substr('&&OWNER',1,instr('&&OWNER','_',1))||'PRISM_CORE','PRISM_CORE','HOLDER_MARKERS','Y');
commit;

--------------------------------------------------------
PROMPT Creating table ss_job_execution
--------------------------------------------------------  

exec prc_drop_table('ss_job_execution');

  CREATE TABLE &&OWNER..ss_job_execution ( 
  job_actual_owner       VARCHAR2(128)  NOT NULL,  
  job_name               VARCHAR2(128)  NOT NULL,
  job_type               VARCHAR2(30)   NOT NULL,
  start_timestamp        TIMESTAMP,   
  run_duration    		 INTERVAL DAY(3) TO SECOND(0),
  status                 VARCHAR2(30),
  completed_yn           VARCHAR2(1) DEFAULT 'N' NOT NULL,
  created_ts             timestamp,
  modified_ts            timestamp  
);

ALTER TABLE &&OWNER..ss_job_execution add constraint pk_job_execution primary key(job_actual_owner,job_name);
ALTER TABLE &&OWNER..ss_job_execution ADD CONSTRAINT chk_completed_yn CHECK (completed_yn IN ('Y','N'));    

declare

  cursor c_tabs
  is
    select distinct table_name
      from dba_tab_columns
     where owner = '&&OWNER'
       and (   table_name like'SS%'
            or table_name like'DD%'
            or table_name like'MD%' )
    order by table_name;			

  v_ddl    varchar2(4000);
  
begin

  for r in c_tabs
  loop
      v_ddl := 'create or replace TRIGGER &&OWNER..'||r.table_name||'_biu'||
                  ' BEFORE INSERT OR UPDATE ON &&OWNER..'||r.table_name||
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
   end loop;
end;
/

PROMPT ***********************************
PROMPT Add - Packages
PROMPT ***********************************

PROMPT ***************************
PROMPT Running const.pks
PROMPT ***************************

@const.pks

PROMPT ***************************
PROMPT Running excep.pks
PROMPT ***************************

@excep.pks

PROMPT ***************************
PROMPT Running gen_metadata.pks/gen_metadata.pkb
PROMPT ***************************

@gen_metadata.pks
@gen_metadata.pkb

PROMPT ***************************
PROMPT Running Running metadata_utilities.pks/metadata_utilities.pkb
PROMPT ***************************

@metadata_utilities.pks
@metadata_utilities.pkb

PROMPT ***********************************
PROMPT Re-Compiling &&OWNER schema
PROMPT ***********************************

BEGIN
  DBMS_UTILITY.COMPILE_SCHEMA('&&OWNER');
END;
/   

spool off

/**********************************************************************************************************************************************
Set up notes:

For install run as DBA user ALTER SESSION SET CURRENT_SCHEMA=ANONR102CP_PRISM_UTILITIES;

Run as UTILITIES user:
ALTER SESSION SET nls_date_format = 'DD-MON-YYYY HH24:MI:SS';
progress can be monitored in util_log

-- Main run for initialisation and generation of data for original company list
exec gen_metadata.load_metadata(p_comp_list);  -- comma separated company list  'A334,A496,B686,B738,C915,D401,K893,P192'
exec gen_metadata.load_metadata('A334,A496,B686,B738,C915,D401,K893,P192');

-- if adding further companies after initial metadata generation then
exec gen_metadata.add_companies(p_comp_list);

Partition views are only created for partition tables where there is an entry in ss_parallel_load_config with parallel_load = 'Y'.
So if new entities are added to ss_parallel_load_config, or parallel_load is subsequently set to 'Y' from 'N' then it is necessary to generate the load views for the schema
e.g. exec gen_metadata.build_load_views('ANONR102KS_PRISM_CORE','P_A334,P_A496,P_B686,P_B738,P_C915,P_D401,P_K893,P_P192');

**********************************************************************************************************************************************/