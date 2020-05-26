
--Create a user to hold temporary anonymisation tables.
DEFINE prefix=&Please_Define_Schema_Prefix;

CREATE USER &prefix._anonymise IDENTIFIED BY &prefix._anonymise;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO &prefix._anonymise;

grant psm_developers_dba to &prefix._anonymise;
grant select any table to &prefix._anonymise;
grant update any table to &prefix._anonymise;
grant delete any table to &prefix._anonymise;

grant insert,update,delete on &prefix._reporting.rptinstances to &prefix._anonymise;
create synonym &prefix._anonymise.rptinstances for &prefix._reporting.rptinstances;

grant insert,update,delete on &prefix._audit.audit_events to &prefix._anonymise;
create synonym &prefix._anonymise.audit_events for &prefix._audit.audit_events;

grant select on &prefix._PRISM_CORE.TITLE_TYPES to &prefix._anonymise;
create synonym  &prefix._anonymise.title_types for &prefix._PRISM_CORE.TITLE_TYPES;

grant select,insert,update,delete on &prefix._prism_core.holders to &prefix._anonymise;
grant select,insert,update,delete on &prefix._prism_core.holder_employee_details to &prefix._anonymise;
grant select,insert,update,delete on &prefix._prism_core.holder_employee_detail_hist to &prefix._anonymise;
grant select,insert,update,delete on &prefix._prism_core.HOLDER_NAMES to &prefix._anonymise;
grant select,insert,update,delete on &prefix._prism_core.HOLDER_ADDRESSES to &prefix._anonymise;
grant select,insert,update,delete on &prefix._prism_core.HOLDER_LABELS to &prefix._anonymise;
grant select,insert,update,delete on &prefix._prism_core.HOLDER_MANDATES to &prefix._anonymise;
grant select,insert,update,delete on &prefix._cash_management.PAYMENTS to &prefix._anonymise;
grant select,insert,update,delete on &prefix._cash_management.CASH_TRANSACTIONS to &prefix._anonymise;
grant select,insert,update,delete on &prefix._cash_management.CASH_ACCOUNTS to &prefix._anonymise;
grant select,insert,update,delete on &prefix._cash_management.BANK_ACCOUNTS to &prefix._anonymise;
grant select,insert,update,delete on &prefix._correspondence.FTP_CREDENTIALS to &prefix._anonymise;

create synonym  &prefix._anonymise.holders for &prefix._prism_core.holders;
create synonym  &prefix._anonymise.holder_employee_details for &prefix._prism_core.holder_employee_details;
create synonym  &prefix._anonymise.holder_employee_detail_hist for &prefix._prism_core.holder_employee_detail_hist;
create synonym  &prefix._anonymise.holder_names for &prefix._prism_core.holder_names;
create synonym  &prefix._anonymise.holder_addresses for &prefix._prism_core.holder_addresses;
create synonym  &prefix._anonymise.holder_labels for &prefix._prism_core.holder_labels;
create synonym  &prefix._anonymise.holder_mandates for &prefix._prism_core.holder_mandates;
create synonym  &prefix._anonymise.payments for &prefix._cash_management.payments;
create synonym  &prefix._anonymise.cash_transactions for &prefix._cash_management.cash_transactions;
create synonym  &prefix._anonymise.cash_accounts for &prefix._cash_management.cash_accounts;
create synonym  &prefix._anonymise.bank_accounts for &prefix._cash_management.bank_accounts;
create synonym  &prefix._anonymise.ftp_credentials for &prefix._correspondence.ftp_credentials;

grant select on sys.all_tables to &prefix._anonymise;


grant execute on &prefix._prism_core.HOLDER_CRUD to &prefix._anonymise;
create synonym  &prefix._anonymise.HOLDER_CRUD  for &prefix._prism_core.HOLDER_CRUD;

alter table &prefix._cash_management.payments add payer_bank_account_id number;

ALTER SESSION SET current_schema=&prefix._anonymise;

CREATE TABLE ANON_LOGS
(
  DATETIME  DATE,
  MESSAGE   VARCHAR2(4000 BYTE)
);


CREATE TABLE PRIVACY_CATALOG
(
  MAPPING_NOTE                VARCHAR2(37 BYTE),
  PROP_ID                     VARCHAR2(4000 BYTE),
  CLASS_APPLIEDSTEREOTYPE     VARCHAR2(4000 BYTE),
  PROPERTY_APPLIEDSTEREOTYPE  VARCHAR2(4000 BYTE),
  PROPERTY_QUALIFIEDNAME      VARCHAR2(4000 BYTE),
  PROPERTY_OWNER              VARCHAR2(4000 BYTE),
  PROPERTY_NAME               VARCHAR2(4000 BYTE),
  COMPONENT_NAME              VARCHAR2(30 BYTE),
  COMPONENT_VERSION           VARCHAR2(1 BYTE),
  OWNER                       VARCHAR2(30 BYTE),
  TABLE_NAME                  VARCHAR2(30 BYTE),
  COLUMN_NAME                 VARCHAR2(30 BYTE),
  COLUMN_MANDATORY            VARCHAR2(1 BYTE),
  COLUMN_TYPE                 VARCHAR2(106 BYTE),
  COLUMN_LENGTH               NUMBER,
  COLUMN_DEFAULT              VARCHAR2(10 BYTE),
  COLUMN_COMMENTS             VARCHAR2(1 BYTE),
  PDM_LOOKUP                  VARCHAR2(62 BYTE),
  TRACE_RELEASE               VARCHAR2(4000 BYTE),
  TABLE_MAPPING               VARCHAR2(4000 BYTE)
);

--Create Sequence for ANON_HOLDER_NAMES table

CREATE SEQUENCE anon_holder_names_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;
/
 --Create Table to hold Anonymised Holder Names
CREATE TABLE anon_holder_names
(
   anon_holder_name_id    NUMBER (*, 0),
   holder_name_id         NUMBER (*, 0),
   comp_code              VARCHAR2 (4 CHAR),
   ivc_code               VARCHAR2 (11 CHAR),
   holder_seq             NUMBER (*, 0),
   holder_type_code       VARCHAR2 (1 CHAR),
   o_title_type_code      VARCHAR2 (10 CHAR),
   o_title_type_gender    VARCHAR2 (10 CHAR),
   a_title_type_code      VARCHAR2 (10 CHAR),
   o_surname              VARCHAR2 (50 CHAR),
   a_surname              VARCHAR2 (50 CHAR),
   o_forename             VARCHAR2 (70 CHAR),
   a_forename             VARCHAR2 (70 CHAR),
   o_suffix               VARCHAR2 (20 CHAR),
   a_suffix               VARCHAR2 (20 CHAR),
   o_salutation           VARCHAR2 (75 CHAR),
   a_salutation           VARCHAR2 (75 CHAR),
   o_other_title          VARCHAR2 (20 CHAR),
   a_other_title          VARCHAR2 (20 CHAR),
   o_preferred_name       VARCHAR2 (25 CHAR),
   a_preferred_name       VARCHAR2 (25 CHAR),
   o_trust_name           VARCHAR2 (100 CHAR),
   a_trust_name           VARCHAR2 (100 CHAR),
   o_trustee_name         VARCHAR2 (50 CHAR),
   a_trustee_name         VARCHAR2 (50 CHAR),
   o_company_name         VARCHAR2 (150 CHAR),
   a_company_name         VARCHAR2 (150 CHAR),
   o_concat_name_string   VARCHAR2 (250),
   a_concat_name_string   VARCHAR2 (250)
);
/

CREATE SEQUENCE ref_hld_name_title_type_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;


CREATE TABLE ref_holder_name_title_type
(
   pk                  NUMBER (38),
   o_title_type_code   VARCHAR2 (10 CHAR),
   gender              VARCHAR (1)
);


CREATE TABLE ref_holder_name_forenames
(
   pk           NUMBER (38),
   o_forename   VARCHAR2 (70 CHAR),
   gender       VARCHAR (1)
);


  --Holder Surnames

CREATE SEQUENCE ref_hld_name_surnames_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;


CREATE TABLE ref_holder_name_surnames
(
   pk          NUMBER (38),
   o_surname   VARCHAR2 (50 CHAR)
);

--Holder Suffix

CREATE SEQUENCE ref_hld_name_suffix_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;


CREATE TABLE ref_holder_name_suffix
(
   pk         NUMBER (38),
   o_suffix   VARCHAR2 (20 CHAR)
);



  --Holder Other Title

CREATE SEQUENCE ref_hld_name_other_title_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;


CREATE TABLE ref_holder_name_other_title
(
   pk              NUMBER (38),
   o_other_title   VARCHAR2 (20 CHAR)
);



  --Holder Preferred Name

CREATE SEQUENCE ref_hld_name_pref_name_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;

CREATE TABLE ref_holder_name_preferred_name
(
   pk                 NUMBER (38),
   o_preferred_name   VARCHAR2 (25 CHAR)
);


  --Holder Trust Name

CREATE SEQUENCE ref_hld_name_trust_name_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;

CREATE TABLE ref_holder_name_trust_name
(
   pk             NUMBER (38),
   o_trust_name   VARCHAR2 (100 CHAR)
);



  --Holder Trustee Name

CREATE SEQUENCE ref_hld_name_trustee_name_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;

CREATE TABLE ref_holder_name_trustee_name
(
   pk               NUMBER (38),
   o_trustee_name   VARCHAR2 (50 CHAR)
);


  --Holder Company Name

CREATE SEQUENCE ref_hld_name_company_name_seq
   MINVALUE 1
   MAXVALUE 999999999999999999999999999
   INCREMENT BY 1
   START WITH 1
   CACHE 20
   NOORDER
   NOCYCLE;


CREATE TABLE ref_holder_name_company_name
(
   pk               NUMBER (38),
   o_company_name   VARCHAR2 (150 CHAR)
);


CREATE TABLE anon_holder_addresses
(
   pk                          NUMBER (*, 0),
   holder_address_id           NUMBER (*, 0),
   comp_code                   VARCHAR2 (4 CHAR),
   ivc_code                    VARCHAR2 (11 CHAR),
   address_type_id             NUMBER (*, 0),
   correspondence_yn           VARCHAR2 (1 CHAR),
   o_address_line1             VARCHAR2 (35 CHAR),
   a_address_line1             VARCHAR2 (35 CHAR),
   o_address_line2             VARCHAR2 (35 CHAR),
   a_address_line2             VARCHAR2 (35 CHAR),
   o_address_line3             VARCHAR2 (35 CHAR),
   a_address_line3             VARCHAR2 (35 CHAR),
   o_address_line4             VARCHAR2 (35 CHAR),
   a_address_line4             VARCHAR2 (35 CHAR),
   o_address_line5             VARCHAR2 (35 CHAR),
   a_address_line5             VARCHAR2 (35 CHAR),
   o_address_line6             VARCHAR2 (35 CHAR),
   a_address_line6             VARCHAR2 (35 CHAR),
   o_postcode_left             VARCHAR2 (4 CHAR),
   a_postcode_left             VARCHAR2 (4 CHAR),
   o_postcode_right            VARCHAR2 (3 CHAR),
   a_postcode_right            VARCHAR2 (3 CHAR),
   o_concate_postcode          VARCHAR2 (8 CHAR),
   a_concate_postcode          VARCHAR2 (8 CHAR),
   o_country_code              VARCHAR2 (2 CHAR),
   a_country_code              VARCHAR2 (2 CHAR),
   o_irish_distribution_code   VARCHAR2 (10 CHAR),
   a_irish_distribution_code   VARCHAR2 (10 CHAR),
   o_concat_address_string     VARCHAR2 (500),
   a_concat_address_string     VARCHAR2 (500)
);

CREATE TABLE anon_holder
(
   comp_code                    VARCHAR2 (4 BYTE) NOT NULL ENABLE,
   ivc_code                     VARCHAR2 (11 BYTE) NOT NULL ENABLE,
   amalgamated_ivc              VARCHAR2 (11 CHAR),
   o_sort_key                   VARCHAR2 (34 CHAR),
   a_sort_key                   VARCHAR2 (34 CHAR),
   o_participant_code           VARCHAR2 (16 CHAR),
   a_participant_code           VARCHAR2 (16 CHAR),
   o_share_master_holder_code   VARCHAR2 (8 CHAR),
   a_share_master_holder_code   VARCHAR2 (8 CHAR),
   o_reference_ivc_code         VARCHAR2 (11 CHAR),
   a_reference_ivc_code         VARCHAR2 (11 CHAR),
   o_designation_name           VARCHAR2 (8 CHAR),
   a_designation_name           VARCHAR2 (8 CHAR),
   o_country_code               VARCHAR2 (2 CHAR),
   a_country_code               VARCHAR2 (2 CHAR),
   o_date_of_death              DATE,
   a_date_of_death              DATE,
   o_gender                     VARCHAR2 (1 CHAR),
   a_gender                     VARCHAR2 (1 CHAR),
   o_date_of_birth              DATE,
   a_date_of_birth              DATE,
   o_national_insurance_no      VARCHAR2 (25 CHAR),
   a_national_insurance_no      VARCHAR2 (25 CHAR),
   o_personnel_number           VARCHAR2 (25 CHAR),
   a_personnel_number           VARCHAR2 (25 CHAR),
   o_payroll_number             VARCHAR2 (25 CHAR),
   a_payroll_number             VARCHAR2 (25 CHAR)
);

CREATE TABLE anon_bank_accounts
(
   BANK_ACCOUNT_ID                 INTEGER NOT NULL,
   BANK_NAME                       VARCHAR2 (50 CHAR),
   BRANCH_NAME                     VARCHAR2 (50 CHAR),
   ALIAS_NAME                      VARCHAR2 (50 CHAR),
   ACCOUNT_NAME                    VARCHAR2 (108 CHAR),
   CHEQUEADDRESS_LINE1_TEXT        VARCHAR2 (35 CHAR),
   CHEQUEADDRESS_LINE2_TEXT        VARCHAR2 (35 CHAR),
   CHEQUEADDRESS_LINE3_TEXT        VARCHAR2 (35 CHAR),
   CHEQUEADDRESS_LINE4_TEXT        VARCHAR2 (35 CHAR),
   CHEQUEADDRESS_LINE5_TEXT        VARCHAR2 (35 CHAR),
   CHEQUEADDRESS_LINE6_TEXT        VARCHAR2 (35 CHAR),
   BIC                             VARCHAR2 (29 CHAR),
   SORTCODE                        VARCHAR2 (9 CHAR),
   O_ACCOUNT_NO                    VARCHAR2 (22 CHAR),
   A_ACCOUNT_NO                    VARCHAR2 (22 CHAR),
   O_IBAN                          VARCHAR2 (34 CHAR),
   A_IBAN                          VARCHAR2 (34 CHAR),
   O_INTERNATIONAL_ACCOUNT_NO      VARCHAR2 (34 CHAR),
   A_INTERNATIONAL_ACCOUNT_NO      VARCHAR2 (34 CHAR),
   O_BUILDING_SOCIETY_ACCOUNT_NO   VARCHAR2 (10 CHAR),
   A_BUILDING_SOCIETY_ACCOUNT_NO   VARCHAR2 (10 CHAR),
   O_CAPITA_REFERENCE              VARCHAR2 (20 CHAR),
   A_CAPITA_REFERENCE              VARCHAR2 (20 CHAR),
   CURRENCY_CODE                   VARCHAR2 (3 CHAR) NOT NULL,
   BANK_ACCOUNT_TYPE_CODE          VARCHAR2 (10 CHAR) NOT NULL,
   REG_STATUS_TYPE_CODE            VARCHAR2 (10 CHAR) NOT NULL,
   STATUS_TYPE_CODE                VARCHAR2 (10 CHAR) NOT NULL,
   COUNTRY_CODE                    VARCHAR2 (2 CHAR) NOT NULL,
   SECOND_SORTCODE                 VARCHAR2 (9 CHAR),
   O_BACS_CODE                     VARCHAR2 (6 CHAR),
   A_BACS_CODE                     VARCHAR2 (6 CHAR)
);
