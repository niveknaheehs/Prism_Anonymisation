create or replace PACKAGE anonymisation_process
AUTHID CURRENT_USER
AS

  g_obfus_run_id  obfus_control.obfus_run_id%TYPE := null;
  g_src_prefix    obfus_control.src_prefix%TYPE   := null; -- Prism Source Prefix e.g. 'ANONPRE8201'
  g_tgt_prefix    obfus_control.tgt_prefix%TYPE   := null; -- Prism Target Prefix e.g. 'ANONPOST8201B'; 
  g_run_env       obfus_control.run_env%TYPE      := null; -- Schema name for the run evironment e.g. 'ANONDEV_ANONYMISE'  
  g_anon_version  obfus_control.anon_version%TYPE := null; -- e.g. 'OB8_R82_01'
  
  g_escape       varchar2(1)  := '\'; 
  g_max_rnd_note  number;
  g_run_date  date;
  g_code NUMBER;
  g_errm varchar2(4000); 
  g_module varchar2(32);
  
  g_max_rnd_address_line_1_seq number;
  g_max_rnd_address_line_2_seq number;
  g_max_rnd_address_line_3_seq number;
  g_max_rnd_postcode_seq number;    
  g_max_rnd_surname_seq number;
  g_max_rnd_forename_seq number;
  
  x_table_not_exist EXCEPTION;
  PRAGMA exception_init (x_table_not_exist, -00942);
                    
  procedure merge_fix_anonalies;
  procedure anonymise;
  procedure process_manual_obfus;
  procedure merge_to_target (pi_schemaprefix varchar2);
  procedure run_purge_data (p_prefix varchar2);
  procedure merge_patches;
  procedure anon_holder_names;
  procedure anon_holder_addresses;
  procedure anon_holder;
  procedure anon_bank_accounts; 
  procedure anon_payments; 
  procedure anon_holder_mandates;
  procedure merge_bank_accounts;
  procedure merge_holder_names; 
  procedure merge_holders;
  procedure merge_holder_address;
  procedure merge_holder_employee_details;
  procedure merge_holder_labels;
  procedure merge_holder_mandate_details;
  procedure merge_payments;
  procedure process_privacy_catalog(p_prefix varchar2);
  procedure generate_stats(prepost_anomolies number);
  procedure generate_qa_reports;
  procedure merge_privacy;
  procedure gen_rnd_notes;
  procedure anon_bank_branches;
  procedure gen_sortkey;
  procedure reset_synonyms;
  procedure address_line1_shuffle;
  procedure merge_bank_branches;
  procedure merge_cheque_ranges; 
  procedure obfus_log(p_log_msg VARCHAR2,p_code NUMBER,p_errm varchar2,p_module varchar2);
  procedure address_line456_shuffle;
  procedure anon_holder_labels;
  procedure gen_rnd_addresses;
  procedure gen_rnd_names;
  procedure anon_disc_exer_spouse_dtls;
  procedure merge_disc_exer_spouse_dtls;
  procedure set_globals;
  procedure anon_disc_exer_req_mandates;
  procedure merge_disc_exer_req_mandates;
  procedure anon_mifid_entities;
  procedure merge_mifid_entities;
  procedure anon_mifid_integration;
  procedure anon_comp_payee_mandates;
  procedure anon_cash_ivc_class_copies;
  procedure merge_comp_payee_mandates;
  procedure merge_cash_ivc_class_copies;
  procedure apply_temp_patches;
END;