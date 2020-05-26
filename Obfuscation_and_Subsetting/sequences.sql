set verify on

--------------------------------------------------------
PROMPT Creating Sequence NOTES_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  &&OS_OWNER..NOTES_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;
--------------------------------------------------------
PROMPT Creating Sequence OBFUSCATION_LOG_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  &&OS_OWNER..OBFUSCATION_LOG_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE;
   
--------------------------------------------------------
PROMPT Creating Sequence OBFUS_RUN_ID_SEQ
--------------------------------------------------------   
   
   CREATE SEQUENCE  &&OS_OWNER..OBFUS_RUN_ID_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE NOCYCLE;

--------------------------------------------------------
PROMPT Creating Sequences NS, NS1 and NS2
--------------------------------------------------------   
   create sequence &&OS_OWNER..ns start with 1;
   create sequence &&OS_OWNER..ns1 start with 1;
   create sequence &&OS_OWNER..ns2 start with 1;
   
--------------------------------------------------------
PROMPT Creating Sequences surname_seq and forename_seq
--------------------------------------------------------   
   create sequence &&OS_OWNER..surname_seq start with 1;
   create sequence &&OS_OWNER..forename_seq start with 1;
 
--------------------------------------------------------
PROMPT Creating Sequence EXECUTION_SEQ   
--------------------------------------------------------   
   CREATE SEQUENCE &&OS_OWNER..EXECUTION_SEQ  MINVALUE 1 MAXVALUE 999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE NOCYCLE  NOKEEP  NOSCALE  GLOBAL;   
   
--------------------------------------------------------
PROMPT Creating Sequences partition_update_seq
--------------------------------------------------------   
   create sequence &&OS_OWNER..partition_update_seq start with 1;   

--------------------------------------------------------
PROMPT Creating Sequence SS_RUN_ID_SEQ
--------------------------------------------------------   
   
   CREATE SEQUENCE  &&OS_OWNER..SS_RUN_ID_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE NOCYCLE;

   --------------------------------------------------------   
PROMPT Creating Sequence SS_LOG_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  &&OS_OWNER..SS_LOG_ID_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE  NOCYCLE;

--------------------------------------------------------   
 PROMPT Creating Sequence SS_STAGE_ORDER_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  &&OS_OWNER..SS_STAGE_ORDER_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE NOCYCLE;
  
--------------------------------------------------------   
 PROMPT Creating Sequence SS_exec_drop_order_seq
--------------------------------------------------------

   CREATE SEQUENCE  &&OS_OWNER..SS_exec_drop_order_seq   MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE NOCYCLE;         

   --------------------------------------------------------   
 PROMPT Creating Sequence ss_success_drop_order_seq
--------------------------------------------------------

   CREATE SEQUENCE  &&OS_OWNER..ss_success_drop_order_seq  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE NOCYCLE;   

--------------------------------------------------------   
 PROMPT Creating Sequence SS_EXEC_INS_PART_SEQ
--------------------------------------------------------
   
   CREATE SEQUENCE  &&OS_OWNER..SS_EXEC_INS_PART_SEQ MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE  NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;

   --------------------------------------------------------   
 PROMPT Creating Sequence SS_CRE_PART_ORDER_SEQ
--------------------------------------------------------
   
   CREATE SEQUENCE  &&OS_OWNER..SS_CRE_PART_ORDER_SEQ MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE  NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;

   --------------------------------------------------------   
 PROMPT Creating Sequence SS_SUCCESS_INS_PART_ORDER_SEQ
--------------------------------------------------------
   
   CREATE SEQUENCE  &&OS_OWNER..SS_SUCCESS_INS_PART_ORDER_SEQ MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 NOCACHE  NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;
 
   --------------------------------------------------------   
 PROMPT Creating Sequence SS_RULE_ID_SEQ
--------------------------------------------------------
 
   CREATE SEQUENCE  &&OS_OWNER..SS_RULE_ID_SEQ  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;

