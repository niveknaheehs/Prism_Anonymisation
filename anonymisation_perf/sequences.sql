set verify on

--------------------------------------------------------
PROMPT Creating Sequence NOTES_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  &&ANON_OWNER..NOTES_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;
--------------------------------------------------------
PROMPT Creating Sequence OBFUSCATION_LOG_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  &&ANON_OWNER..OBFUSCATION_LOG_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;
   
--------------------------------------------------------
PROMPT Creating Sequence OBFUS_RUN_ID_SEQ
--------------------------------------------------------   
   
   CREATE SEQUENCE  &&ANON_OWNER..OBFUS_RUN_ID_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE NOCYCLE;

--------------------------------------------------------
PROMPT Creating Sequences NS, NS1 and NS2
--------------------------------------------------------   
   create sequence &&ANON_OWNER..ns start with 1;
   create sequence &&ANON_OWNER..ns1 start with 1;
   create sequence &&ANON_OWNER..ns2 start with 1;
   
--------------------------------------------------------
PROMPT Creating Sequences surname_seq and forename_seq
--------------------------------------------------------   
   create sequence &&ANON_OWNER..surname_seq start with 1;
   create sequence &&ANON_OWNER..forename_seq start with 1;
 
--------------------------------------------------------
PROMPT Creating Sequence EXECUTION_SEQ   
--------------------------------------------------------   
   CREATE SEQUENCE &&ANON_OWNER..EXECUTION_SEQ  MINVALUE 1 MAXVALUE 999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL;   
   
--------------------------------------------------------
PROMPT Creating Sequences partition_update_seq
--------------------------------------------------------   
   create sequence &&ANON_OWNER..partition_update_seq start with 1;   
