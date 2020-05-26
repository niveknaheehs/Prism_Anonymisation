DELETE obfus_ctrl_manual_config;

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (1,1,'BANKS','PRISM_CORE','BANK_BRANCHES','P','anonymisation_process.anon_bank_branches','anonymisation_process.merge_bank_branches','Y');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (3,2,'HOLDERS','PRISM_CORE','HOLDER_NAMES','P','anonymisation_process.anon_holder_names','anonymisation_process.merge_holder_names','Y');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (4,2,'HOLDERS','PRISM_CORE','HOLDERS','P','anonymisation_process.anon_holder','anonymisation_process.merge_holders','Y');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (5,2,'HOLDERS','PRISM_CORE','HOLDER_EMPLOYEE_DETAILS','P','anonymisation_process.anon_holder_employee_details','anonymisation_process.merge_holder_employee_details','Y');
 
INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (6,2,'HOLDERS','PRISM_CORE','HOLDER_LABELS','P','anonymisation_process.anon_holder_labels','anonymisation_process.merge_holder_labels','Y');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (7,2,'HOLDERS','PRISM_CORE','HOLDER_ADDRESSES','P','anonymisation_process.anon_holder_addresses','anonymisation_process.merge_holder_address','Y');
    
INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (8,2,'HOLDERS','PRISM_CORE','HOLDER_MANDATES','P','anonymisation_process.anon_holder_mandates','anonymisation_process.merge_holder_mandate_details','Y');   

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (10,3,'PAYMENTS','CASH_MANAGEMENT','PAYMENTS','P','anonymisation_process.anon_payments','anonymisation_process.merge_payments','Y');  

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (11,3,'PAYMENTS','PRISM_CAG_DATA','CASH_IVC_CLASS_COPIES','P','anonymisation_process.anon_cash_ivc_class_copies','anonymisation_process.merge_cash_ivc_class_copies','Y'); 
 
INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (12,3,'PAYMENTS','PRISM_CORE','COMP_PAYEE_MANDATES','P','anonymisation_process.anon_comp_payee_mandates','anonymisation_process.merge_comp_payee_mandates','Y'); 

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (13,4,'DISC_EXER','PRISM_CORE','DISC_EXER_SPOUSE_DTLS','P','anonymisation_process.anon_disc_exer_spouse_dtls','anonymisation_process.merge_disc_exer_spouse_dtls','Y');  

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (14,4,'DISC_EXER','PRISM_CORE','DISC_EXER_REQ_MANDATES','P','anonymisation_process.anon_disc_exer_req_mandates','anonymisation_process.merge_disc_exer_req_mandates','Y');  
  
INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (15,5,'MIFID','EGIS','MIFID_ENTITIES','P','anonymisation_process.anon_mifid_entities','anonymisation_process.merge_mifid_entities','Y');  

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (16,5,'MIFID','INTEGRATION','MIFID_TRANSACTION_DETAILS','P','anonymisation_process.anon_mifid_trans_details','anonymisation_process.merge_mifid_trans_details','Y');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (17,5,'MIFID','INTEGRATION','MIFID_BULK_TRADES','P','anonymisation_process.anon_mifid_bulk_trades','anonymisation_process.merge_mifid_bulk_trades','Y');  

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (18,6,'MERGE_PATCHES','IMPORT','RR556_CNB_REVERSAL_REPORT','P','anonymisation_process.anon_rr556_cnb_reversal_rpt','anonymisation_process.merge_rr556_cnb_reversal_rpt','Y');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (19,6,'MERGE_PATCHES','IMPORT','RR556_CNB_BANK_REVERSAL_TOTALS','P','anonymisation_process.anon_rr556_cnb_bank_reversal','anonymisation_process.merge_rr556_cnb_bank_reversal','N');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (20,6,'MERGE_PATCHES','CREST','CREST_PARTICIPANTS','P','anonymisation_process.anon_crest_participants','anonymisation_process.merge_crest_participants','Y');  
     
INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (21,6,'MERGE_PATCHES','INTEGRATION','FATCA_CLSF_EXTRACTS','P','anonymisation_process.anon_fatca_clsf_extracts','anonymisation_process.merge_fatca_clsf_extracts','Y');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (22,6,'MERGE_PATCHES','CASH_MANAGEMENT','CASH_TRANSACTIONS','P','anonymisation_process.anon_cash_transactions','anonymisation_process.merge_cash_transactions','Y');
 
INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (23,6,'MERGE_PATCHES','PRISM_CORE','IDEAL_TRANS','P','anonymisation_process.anon_ideal_trans','anonymisation_process.merge_ideal_trans','Y');

INSERT INTO obfus_ctrl_manual_config (entity_order,entity_group_id,entity_group_name,owner,table_name,step_type,anon_proc,merge_proc,enabled_yn)
VALUES (24,6,'MERGE_PATCHES','PRISM_CORE','MONEY_LAUNDER_CTLS','P','anonymisation_process.anon_money_launder_ctls','anonymisation_process.merge_money_launder_ctls','Y');

commit;