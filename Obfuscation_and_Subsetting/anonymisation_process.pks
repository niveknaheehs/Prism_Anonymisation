create or replace PACKAGE anonymisation_process
AUTHID CURRENT_USER
AS

  g_obfus_run_id  obfus_control.obfus_run_id%TYPE; -- := null;
  g_src_prefix    obfus_control.src_prefix%TYPE;   -- := 'ANONPRE9001';
  g_tgt_prefix    obfus_control.tgt_prefix%TYPE;   -- := 'ANONPRE9001';
  g_run_env       obfus_control.run_env%TYPE;      -- := 'ANONPERF_ANONYMISE'; -- Schema name for the run evironment e.g. 'ANONDEV_ANONYMISE'
  g_anon_version  obfus_control.anon_version%TYPE; -- := 'OB8_R90_01';

  g_run_date  date;
  g_code NUMBER;
  g_errm varchar2(4000);
  g_module varchar2(62);

  g_max_rnd_address_line_1_seq number;
  g_max_rnd_address_line_2_seq number;
  g_max_rnd_address_line_3_seq number;
  g_max_rnd_postcode_seq number;
  g_max_rnd_surname_seq number;
  g_max_rnd_forename_seq number;

  procedure  set_globals ( p_obfus_run_id in number,
                           p_src_prefix   in varchar2,
                           p_tgt_prefix   in varchar2,
                           p_run_env      in varchar2,
                           p_anon_version in varchar2 );
  procedure reset_schema;
-- procedure merge_fix_anonalies;
  procedure table_merge_fix_anomalies(p_owner varchar2,p_table_name varchar2,p_src_prefix varchar2,p_run_date date,p_anon_version varchar2);
  procedure run_purge_data (p_prefix varchar2);

--  procedure process_privacy_catalog(p_prefix varchar2);
--  procedure generate_stats(prepost_anomolies number);
  procedure generate_table_stats(p_owner varchar2,p_table_name varchar2, p_src_prefix varchar2,p_run_date date,p_anon_version varchar2,p_prepost_anomolies number);
  
--  procedure generate_qa_reports;
  procedure generate_table_qa_reports(p_owner varchar2,p_table_name varchar2, p_src_prefix varchar2,p_run_date date,p_anon_version varchar2,p_src_rep_syn_prefix varchar2,p_tgt_rep_syn_prefix varchar2);
  procedure merge_privacy;

  procedure gen_rnd_notes;
  procedure gen_rnd_addresses;
  procedure gen_rnd_names;

  procedure address_line1_shuffle;
  procedure address_line456_shuffle;

  procedure anon_bank_branches;
  procedure anon_holder_names;
  procedure anon_holder;
  procedure anon_holder_addresses;
  procedure anon_holder_employee_details;
  procedure anon_holder_labels;
  procedure anon_holder_mandates;
  procedure anon_payments;

  procedure merge_bank_branches;
  procedure merge_holder_names;
  procedure merge_holders;
  procedure merge_holder_address;
  procedure merge_holder_employee_details;
  procedure merge_holder_labels;
  procedure merge_holder_mandate_details;
  procedure merge_payments;
  procedure merge_payments (p_partition_name in varchar2, p_part_update_seq in number);

  procedure anon_disc_exer_spouse_dtls;
  procedure merge_disc_exer_spouse_dtls;
  procedure anon_disc_exer_req_mandates;
  procedure merge_disc_exer_req_mandates;
  procedure anon_mifid_entities;
  procedure merge_mifid_entities;
  procedure anon_mifid_trans_details;
  procedure anon_mifid_bulk_trades;
  procedure merge_mifid_trans_details;
  procedure merge_mifid_bulk_trades;
  procedure anon_comp_payee_mandates;
  procedure anon_cash_ivc_class_copies;
  procedure merge_comp_payee_mandates;
  procedure merge_cash_ivc_class_copies;

  procedure anon_rr556_cnb_reversal_rpt;
  --procedure anon_rr556_cnb_bank_reversal;
  procedure anon_crest_participants;
  procedure anon_fatca_clsf_extracts;
  procedure anon_cash_transactions;
  procedure anon_ideal_trans;
  procedure anon_money_launder_ctls;
  procedure merge_rr556_cnb_reversal_rpt;
  --procedure merge_rr556_cnb_bank_reversal;
  procedure merge_crest_participants;
  procedure merge_fatca_clsf_extracts;
  procedure update_cash_transactions (p_partition_name in varchar2, p_part_update_seq in number);
  procedure parallel_partition_update (p_table_owner in varchar2, p_table_name in varchar2, p_part_update_seq in number);
  procedure merge_cash_transactions;
  procedure merge_ideal_trans;
  procedure merge_money_launder_ctls;

  procedure apply_temp_patches;

  procedure per_col_masking_exceptions (p_owner VARCHAR2, p_table_name VARCHAR2,p_rep_tgt_syn_prefix VARCHAR2);
  procedure apply_fast_mask(p_owner VARCHAR2, p_table_name VARCHAR2);
  procedure apply_mask(p_owner VARCHAR2, p_table_name VARCHAR2);

END;
/