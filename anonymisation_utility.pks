CREATE OR REPLACE PACKAGE anonymisation_utility
AS
   FUNCTION afn_randomise_digit (p_code VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION afn_randomise_date (p_date DATE)
      RETURN DATE;

   FUNCTION afn_randomise_textdigit (p_code VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION afn_gen_upd_date_stmt (p_owner           VARCHAR2,
                                   p_table_name      VARCHAR2,
                                   p_column_name     VARCHAR2,
                                   p_anonymise       VARCHAR2,
                                   p_schemaprefix    VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION afn_gen_upd_digittext_stmt (p_owner           VARCHAR2,
                                        p_table_name      VARCHAR2,
                                        p_column_name     VARCHAR2,
                                        p_anonymise       VARCHAR2,
                                        p_schemaprefix    VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION afn_gen_upd_reord_digit_stmt (p_owner            VARCHAR2,
                                          p_table_name       VARCHAR2,
                                          p_column_name      VARCHAR2,
                                          p_anonymise        VARCHAR2,
                                          p_schema_prefix    VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION afn_holder_forenames (pi_gender VARCHAR2)
      RETURN VARCHAR2;
      
FUNCTION afn_holder_surnames
RETURN VARCHAR2;      
      

   PROCEDURE afn_holder_address (p_holder_address_id NUMBER);

   PROCEDURE afn_national_insurance;

   PROCEDURE run_afn_holder_address;

END;
/