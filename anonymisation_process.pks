CREATE OR REPLACE PACKAGE anonymisation_process
AUTHID CURRENT_USER
AS
   PROCEDURE run_purge_data (p_prefix VARCHAR2);


   PROCEDURE validate_purge (p_prefix VARCHAR2);

   PROCEDURE run_anonymisation (p_schemaprefix      IN VARCHAR2,
                                p_anonymise_owner   IN VARCHAR2);

   PROCEDURE load_anon_holder_names;

   PROCEDURE load_ref_name_title_type;

   PROCEDURE load_ref_name_forenames;

   PROCEDURE load_ref_name_surnames;

   PROCEDURE load_ref_name_suffix;

   PROCEDURE load_ref_name_other_title;

   PROCEDURE load_ref_name_preferred_name;

   PROCEDURE load_ref_name_trust_name;

   PROCEDURE load_ref_name_trustee_name;

   PROCEDURE load_ref_name_company_name;

   PROCEDURE load_anon_holder;

   PROCEDURE load_anon_holder_addresses;

   PROCEDURE load_anon_bank_accounts;

   PROCEDURE anon_holder_names;

   PROCEDURE anon_holder_addresses;

   PROCEDURE anon_weakanon_addresses;

   PROCEDURE anon_capitareference;

   PROCEDURE anon_holder;

   PROCEDURE anon_holder_ninumber;
   
   PROCEDURE anon_bank_accounts;
   
   PROCEDURE merge_bank_accounts;

   PROCEDURE merge_holder_names;

   PROCEDURE merge_holder_address;

   PROCEDURE merge_holder_employee_details;

   PROCEDURE merge_holder_labels;

   PROCEDURE merge_holder_mandate_details;

   PROCEDURE merge_holder_payments;
END;
/