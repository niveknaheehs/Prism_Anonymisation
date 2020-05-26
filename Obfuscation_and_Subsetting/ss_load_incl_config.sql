

set define off;
delete from  ss_load_incl_config;
--insert into ss_load_incl_config(owner,table_name) values ('OWNER','TAB');

insert into ss_load_incl_config(owner,table_name) values ('AUDIT','BATCH_DEL_REQ_STATUS_TYPES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','BATCH_TASK_STATUS_TYPES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_PARAMETER_TYPES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','HOLDER_TABLE_TYPES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_EVENT_CATEGORIES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_JOURNAL_DESPATCH_NOTES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_TRAN_GROUPS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_INVESTOR_DETAILS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','BATCH_CHECKING_CONTROLS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','BATCH_CHK_CTL_STATUS_TYPES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AHR_BATCH_STATUS_TYPES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','BATCH_DEL_REQ_TYPES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_TABLES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_HEADERS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','UPD_CTRL_ALLOWABLE_TASKS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_TRAN_DTLS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','INSTANCE_TYPE_LOOKUPS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','FOLLOW_ON_TASK_SELECTIONS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_SUPPRESS_OUTPUTS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_SHADOW_MAPPINGS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_EVENT_SUBEVENTS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_TJL_CONTEXT');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_PARAMETERS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','BATCH_DELETE_REQUEST_PARAMS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_PERM_COMPANIES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','FOLLOW_ON_ALLOWABLE_TASKS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','BATCH_DELETE_REQUESTS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_EVENTS');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_TRAN_MESSAGES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_PARAM_DATA_ENTRY_TYPE');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','COMPANY_UNBATCH_LAST_NUMBER');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_TOTAL_TYPES');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','COMPANY_BATCH_LAST_NUMBER');
insert into ss_load_incl_config(owner,table_name) values ('AUDIT','AUDIT_TOTALS');


commit;