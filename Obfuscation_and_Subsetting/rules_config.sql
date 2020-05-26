/* Load Rules Summary 

1. '*' - All data unconditionally, eqivalent to a TYPE_ALL load type
2. 'C' -  Only take the company subset from this table, the table must be eiher paritioned by compnay or contain a mandatory comp_code, if not, R rules (3) should be used 
3. R1,..R(n) or R(n) - The parent table of R(n) must be eiher paritioned by comapny or coontain a mandatory comp_code

*/


set define off;

delete from ss_rules_config;
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (1,'PRISM_CAG_PARAMS','CAG_EVENTS','C');




-- If The Audit schema is excluded from the dataload, the PRISM_CORE.ELECTS_REQUEST_PARAMETERS and REG_ELECTION_REQUESTS
-- tables have parent references in PSM_AUDIT, so load these tables to support this relationship.

--AHR_BATCH_STATUS_TYPES : All records (This is seed data)
--AUDIT_PARAMETER_TYPES : All records (This is seed data)
--AUDIT_HEADERS has mandatory COMP_CODE and is partitioned by comp_code has a fk to AHR_BATCH_STATUS_TYPES => C rule


-- Under test
--insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (2,'PRISM_AUDIT',AHR_BATCH_STATUS_TYPES,'*');
--insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (3,'PRISM_AUDIT',AUDIT_PARAMETER_TYPES ,'*');
--insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (4,'PRISM_AUDIT',AUDIT_HEADERS,'C');




/***************************************************************************************************************************
* Currently not supporting rules for REFERENCE PARTITION tables (although statements can now be interleaved with SUBSET LOAD)
* Problem is rule is filtering out data from partition but related entity partitions still load full partition data, so get
* parent key not found violations. Population of individual tables using referential rules needs to take into account all of the data required
* so needs to use all FKs in the table to ensure a full dataload.

insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (2,'PRISM_CORE','HOLDER_AWARD_APPLICATIONS','HAE_HAA1_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (3,'PRISM_CORE','HOLDER_AWARD_APPLICATIONS','HAE_HAA2_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (4,'PRISM_CORE','HOLDER_PLAN_CASH_ACCOUNTS', 'H_HPCA_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (5,'PRISM_CORE','INTEGRITY_EXCEPTIONS', 'CIS_IE_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (6,'PRISM_CORE','PARTICIPATION_CHOICES', 'H_PC_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (7,'PRISM_CORE','SIP_SHARE_MOVEMENTS', 'ST_SHM_FK,SE_ST_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (8,'PRISM_CORE','SIP_TRANS_SHARE_ALLOCATIONS', 'ST_STSA_FK,SE_ST_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (9,'PRISM_CORE','SIP_TRANS_RIGHTS_ISSUES', 'ST_STRIGI_FK,SE_ST_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (10,'PRISM_CORE','SIP_TRANS_CAPITAL_RECONSTS', 'ST_STCAPR_FK,SE_ST_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (11,'PRISM_CORE','SIP_CASH_MOVEMENTS', 'ST_SCM_FK,SE_ST_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (12,'PRISM_CORE','SIP_PAYMENT_DETAILS', 'H_SPD_FK');
insert into ss_rules_config(rule_id,OWNER,TABLE_NAME,SS_RULE) values (13,'PRISM_CORE','SIP_TRANSACTIONS', 'SE_ST_FK');
***************************************************************************************************************************/

commit;