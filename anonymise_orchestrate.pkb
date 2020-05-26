CREATE OR REPLACE PACKAGE BODY ANONPOST_ANONYMISE.anonymisation_orchestrate
AS
   PROCEDURE reset_schema
   AS
   BEGIN
      EXECUTE IMMEDIATE ('truncate table ANON_HOLDER');

      EXECUTE IMMEDIATE ('truncate table ANON_HOLDER_ADDRESSES');

      EXECUTE IMMEDIATE ('truncate table ANON_HOLDER_NAMES');

      EXECUTE IMMEDIATE ('truncate table ANON_LOGS');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_COMPANY_NAME');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_FORENAMES');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_OTHER_TITLE');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_PREFERRED_NAME');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_SUFFIX');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_SURNAMES');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_TITLE_TYPE');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_TRUST_NAME');

      EXECUTE IMMEDIATE ('truncate table REF_HOLDER_NAME_TRUSTEE_NAME');
   END;

   PROCEDURE anonymise
   AS
   BEGIN
      reset_schema ();

      anonymisation_process.load_anon_holder_names;

      anonymisation_process.load_ref_name_title_type;

      anonymisation_process.load_ref_name_forenames;

      anonymisation_process.load_ref_name_surnames;

      anonymisation_process.load_ref_name_suffix;

      anonymisation_process.load_ref_name_other_title;

      anonymisation_process.load_ref_name_preferred_name;

      anonymisation_process.load_ref_name_trust_name;

      anonymisation_process.load_ref_name_trustee_name;

      anonymisation_process.load_ref_name_company_name;

      anonymisation_process.load_anon_holder_addresses;

      anonymisation_process.load_anon_bank_accounts;

      anonymisation_process.load_anon_holder;

      anonymisation_process.anon_bank_accounts;

      anonymisation_process.anon_holder_names;

      anonymisation_process.anon_holder_addresses;

      anonymisation_process.anon_weakanon_addresses;

      anonymisation_process.anon_holder;

      anonymisation_process.anon_holder_ninumber;



   END;



   PROCEDURE synchronise (pi_schemaprefix VARCHAR2)
   AS
   BEGIN



BEGIN     EXECUTE IMMEDIATE
         (   'INSERT INTO '
          || pi_schemaprefix
          || '_audit.audit_events (event_id, comp_code)
           VALUES (1, NULL)');
EXCEPTION WHEN OTHERS THEN
dbms_output.put_line('Audit Event already exists');
END;
      DBMS_SESSION.set_identifier ('ADFCS\ANON:1');


      EXECUTE IMMEDIATE
         (   'update '
          || pi_schemaprefix
          || '_correspondence.ftp_credentials set username = ''CHANGE'', password = ''CHANGE'', private_key=null');

   --Privacy Catalog led anonymisation routine
      anonymisation_process.run_anonymisation (
         pi_schemaprefix,
         pi_schemaprefix || '_ANONYMISE');
    --Clean up Capita References in Bank Accounts
    --anonymisation_process.anon_capitareference; --Superseded by anon_bank_accounts

--Synchronise Data
      anonymisation_process.merge_holder_names;

      anonymisation_process.merge_holder_address;

      anonymisation_process.merge_holder_labels;

      anonymisation_process.merge_holder_employee_details;

      anonymisation_process.merge_holder_mandate_details;

      anonymisation_process.merge_bank_accounts;

      anonymisation_process.merge_holder_payments;

      anonymisation_process.run_purge_data (pi_schemaprefix);
   END;
END;
/