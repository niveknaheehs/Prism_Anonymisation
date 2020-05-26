CREATE OR REPLACE PACKAGE BODY anonymisation_process
AS
   PROCEDURE validate_purge (p_prefix VARCHAR2)
   IS
      CURSOR lc_audittables
      IS
         SELECT owner, table_name, num_rows
           FROM sys.all_tables
          WHERE     owner LIKE p_prefix || '_AUDIT'
                AND table_name LIKE 'A\_%' ESCAPE '\';

      l_stmt      VARCHAR2 (32000);
      l_count     NUMBER;
      l_counter   NUMBER;
   BEGIN
      l_counter := 0;

      FOR i IN lc_audittables
      LOOP
         l_stmt := 'select count(*) from ' || i.owner || '.' || i.table_name;

         EXECUTE IMMEDIATE (l_stmt) INTO l_count;

         IF l_count > 0
         THEN
            DBMS_OUTPUT.put_line (
               'Error truncating ' || i.owner || '.' || i.table_name);
            l_counter := l_counter + 1;
         END IF;
      END LOOP;

      IF l_counter < 1
      THEN
         DBMS_OUTPUT.put_line ('All Audit tables appear to be truncated');
      END IF;
   END;



   PROCEDURE run_purge_data (p_prefix VARCHAR2)
   IS
      --Purge the shadow Audit_tables

      CURSOR lc_audittables
      IS
         SELECT owner, table_name, num_rows
           FROM sys.all_tables
          WHERE     owner LIKE p_prefix || '_AUDIT'
                AND table_name LIKE 'A\_%' ESCAPE '\';

      l_stmt   VARCHAR2 (32000);


      CURSOR lc_integrationtabs
      IS
         SELECT owner, table_name, iot_name
           FROM all_tables
          WHERE     owner LIKE p_prefix || '_INTEGRATION'
                AND iot_name IS NULL
                AND table_name NOT IN            --Excluded Seeded Data Tables
                                      ('DATA_LOCATION_TYPES',
                                       'DISPATCH_BILLING_STATS_TYPES',
                                       'DISPATCH_FTP_DEST_TYPES',
                                       'DISPATCH_GROUPS',
                                       'DISPATCH_GROUP_STATUS_TYPES',
                                       'DISPATCH_GRPS_LIST_TYPES',
                                       'DISPATCH_ITEM_SOURCE_TYPES',
                                       'DISPATCH_ITEM_STATUS_TYPES',
                                       'DISPATCH_TYPES',
                                       'INTERFACE_SETTINGS',
                                       'INT_PAYMENT_STATUS_TYPES',
                                       'JOB_SCHEDULE_INTERVAL_TYPES',
                                       'JOB_SCHEDULE_STAT_TYPES',
                                       'JOB_STATUS_TYPES',
                                       'JOB_TYPES',
                                       'OM_PRISM_COMP_CTRL',
                                       'SOURCE_EMAIL_ADDRESS_TYPES',
                                       'SYSTEMS',
                                       'WUP_HEADER_STATUS_TYPES',
                                       'WUP_HEADER_SUB_UPDATE_TYPES',
                                       'WUP_HEADER_UPDATE_TYPES',
                                       'WUP_MARKER_ACTION_TYPES');
   BEGIN
      --Remove existing reports and references
      l_stmt := 'DELETE FROM ' || p_prefix || '_reporting.rptinstances';

      EXECUTE IMMEDIATE (l_stmt);

      --truncate Interface Tables


      FOR i IN lc_integrationtabs
      LOOP
         l_stmt :=
            'delete from ' || i.owner || '.' || i.table_name || ' cascade';

         EXECUTE IMMEDIATE (l_stmt);
      END LOOP;


      FOR i IN lc_audittables
      LOOP
         l_stmt := 'truncate table ' || i.owner || '.' || i.table_name;

         BEGIN
            EXECUTE IMMEDIATE (l_stmt);
         EXCEPTION
            WHEN OTHERS
            THEN
               BEGIN
                  l_stmt :=
                        'delete from '
                     || i.owner
                     || '.'
                     || i.table_name
                     || ' cascade';

                  EXECUTE IMMEDIATE (l_stmt);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     DBMS_OUTPUT.put_line ('Issue Executing:');
                     DBMS_OUTPUT.put_line (l_stmt);
               END;
         END;
      END LOOP;

      l_stmt := 'DELETE FROM ' || p_prefix || '_audit.audit_tables';

      EXECUTE IMMEDIATE (l_stmt);

      l_stmt := 'DELETE FROM ' || p_prefix || '_audit.audit_event_subevents';

      EXECUTE IMMEDIATE (l_stmt);

      l_stmt := 'DELETE FROM ' || p_prefix || '_audit.audit_events';

      EXECUTE IMMEDIATE (l_stmt);
   END;


   PROCEDURE run_anonymisation (p_schemaprefix      IN VARCHAR2,
                                p_anonymise_owner   IN VARCHAR2)
   IS
      /*
      This procedure orchestrates the anonymisation for each type of PII column.
      PII Address -- ANON_UPDATE_COLUMN_STMT (Shuffle and Replace Digits - although performance issues may prevent this)
      --For the categories below, the randomise_digittext function is applied;
      --This replaces all alpha with a single alpha, and all numerics with a single numeric.
      PII Obscure --afn_randomise_digittext
      PII Web --afn_randomise_digittext
      PII Comp Contact --afn_randomise_digittext
      PII Comp Sensitive --afn_randomise_digittext
      PII CREST --afn_randomise_digittext
      PII Email -- afn_randomise_digittext
      PII Financial --afn_randomise_digittext
      PII Telephone --afn_randomise_digittext
      --For the categories below the table owning the fields is truncated
      PII Configuration -- Truncate
      --For the categories below, a mapping is applied to update fields based on the holder anonymisation indexes
      PII Beneficial Holder Name --Map
      PII Holder --Map
      PII Holder Address --Map
      PII Holder Address Sync --Map
      PII Holder Date --Map
      PII Holder Date Sync --Map
      PII Holder Name --Map
      PII Holder Name Sync --Map
      PII Holder Sync --Map
      */
      CURSOR lc_afn_reord_digit
      IS
         SELECT owner,
                table_name,
                column_name,
                column_type,
                REGEXP_SUBSTR (property_appliedstereotype, 'PII+[^\|]+', 1)
                   AS privacy_type
           FROM privacy_catalog
          WHERE     REGEXP_SUBSTR (property_appliedstereotype,
                                   'PII+[^\|]+',
                                   1) LIKE
                       'PII Address'
                --Do not anonymise country codes.
                AND column_name NOT LIKE 'COUNTRY_CODE';

      CURSOR lc_afn_digittext
      IS
         SELECT owner,
                table_name,
                column_name,
                column_type,
                REGEXP_SUBSTR (property_appliedstereotype, 'PII+[^\|]+', 1)
                   AS privacy_type
           FROM privacy_catalog
          WHERE     REGEXP_SUBSTR (property_appliedstereotype,
                                   'PII+[^\|]+',
                                   1) IN
                       ('PII Obscure',
                        'PII Web',
                        'PII Comp Contact',
                        'PII Comp Sensitive',
                        'PII CREST',
                        'PII Email',
                        'PII Financial',
                        'PII Telephone')
                AND column_name NOT IN ('BANK_SORT_CODE', 'CAPITA_REFERENCE');

      CURSOR lc_afn_truncate
      IS
         SELECT DISTINCT owner, table_name
           FROM privacy_catalog
          WHERE REGEXP_SUBSTR (property_appliedstereotype, 'PII+[^\|]+', 1) IN
                   ('PII Configuration');



      l_stmt   VARCHAR2 (32000);
      l_rows   NUMBER;
   BEGIN
      --Loop through records to be shuffled, and subject to digit replacement
      FOR i IN lc_afn_reord_digit
      LOOP
         -- Note: afn_gen_upd_reord_digit_stmt is non-performant on large tables due to HASH join
         -- This needs to be reviewed to ensure that the code performs
         --l_stmt := afn_gen_upd_reord_digit_stmt( i.owner, i.table_name, i.column_name, p_anonymise_owner,p_schemaprefix);
         l_stmt :=
            anonymisation_utility.afn_gen_upd_digittext_stmt (
               i.owner,
               i.table_name,
               i.column_name,
               p_anonymise_owner,
               p_schemaprefix);

         BEGIN
            EXECUTE IMMEDIATE (l_stmt);

            l_rows := SQL%ROWCOUNT;
            l_stmt :=
                  'insert into anon_logs (datetime, message) values (sysdate,''Updated '
               || i.owner
               || '.'
               || i.table_name
               || '.'
               || i.column_name
               || ' rows updated:'
               || l_rows
               || ''')';

            EXECUTE IMMEDIATE (l_stmt);

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               DBMS_OUTPUT.put_line ('Issue executing:');
               DBMS_OUTPUT.put_line (l_stmt);
               RAISE;
               ROLLBACK;
         END;
      END LOOP;

      --Loop through records to be subject to alpha / digit replacement
      FOR i IN lc_afn_digittext
      LOOP
         --l_stmt := afn_gen_upd_reord_digit_stmt( i.owner, i.table_name, i.column_name, p_anonymise_owner,p_schemaprefix);
         l_stmt :=
            anonymisation_utility.afn_gen_upd_digittext_stmt (
               i.owner,
               i.table_name,
               i.column_name,
               p_anonymise_owner,
               p_schemaprefix);

         BEGIN
            EXECUTE IMMEDIATE (l_stmt);

            l_rows := SQL%ROWCOUNT;
            l_stmt :=
                  'insert into anon_logs (datetime, message) values (sysdate,''Updated '
               || i.owner
               || '.'
               || i.table_name
               || '.'
               || i.column_name
               || ' rows updated:'
               || l_rows
               || ''')';

            EXECUTE IMMEDIATE (l_stmt);

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               DBMS_OUTPUT.put_line ('Issue executing:');
               DBMS_OUTPUT.put_line (l_stmt);
               RAISE;
               ROLLBACK;
         END;
      END LOOP;

      --Truncate Configuration Tables
      FOR i IN lc_afn_truncate
      LOOP
         --l_stmt := afn_gen_upd_reord_digit_stmt( i.owner, i.table_name, i.column_name, p_anonymise_owner,p_schemaprefix);
         l_stmt :=
               'truncate table '
            || p_schemaprefix
            || '_'
            || i.owner
            || '.'
            || i.table_name;

         BEGIN
            EXECUTE IMMEDIATE (l_stmt);

            l_stmt :=
                  'insert into anon_logs (datetime, message) values (sysdate,''Truncated '
               || i.owner
               || '.'
               || i.table_name
               || ''')';

            EXECUTE IMMEDIATE (l_stmt);

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               DBMS_OUTPUT.put_line ('Issue executing:');
               DBMS_OUTPUT.put_line (l_stmt);

               BEGIN
                  l_stmt :=
                        'delete from '
                     || p_schemaprefix
                     || '_'
                     || i.owner
                     || '.'
                     || i.table_name
                     || ' cascade';

                  EXECUTE IMMEDIATE (l_stmt);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     RAISE;
                     ROLLBACK;
               END;
         END;
      END LOOP;
   END;


   PROCEDURE load_anon_holder_names
   IS
   BEGIN
      INSERT INTO ANON_HOLDER_NAMES (ANON_HOLDER_NAME_ID,
                                     HOLDER_NAME_ID,
                                     COMP_CODE,
                                     IVC_CODE,
                                     HOLDER_SEQ,
                                     HOLDER_TYPE_CODE,
                                     O_TITLE_TYPE_CODE,
                                     O_TITLE_TYPE_GENDER,
                                     O_SURNAME,
                                     O_FORENAME,
                                     O_SUFFIX,
                                     O_SALUTATION,
                                     O_OTHER_TITLE,
                                     O_PREFERRED_NAME,
                                     O_TRUST_NAME,
                                     O_TRUSTEE_NAME,
                                     O_COMPANY_NAME)
         (SELECT anon_holder_names_seq.NEXTVAL,          --ANON_HOLDER_NAME_ID
                 hn.HOLDER_NAME_ID,
                 hn.COMP_CODE,
                 hn.IVC_CODE,
                 hn.HOLDER_SEQ,
                 hn.HOLDER_TYPE_CODE,
                 hn.TITLE_TYPE_CODE,
                 NVL (tt.gender, 'U') AS gender,
                 hn.SURNAME,
                 hn.FORENAME,
                 hn.SUFFIX,
                 hn.SALUTATION,
                 hn.OTHER_TITLE,
                 hn.PREFERRED_NAME,
                 hn.TRUST_NAME,
                 hn.TRUSTEE_NAME,
                 hn.COMPANY_NAME
            FROM holder_names hn
                 LEFT OUTER JOIN title_types tt
                    ON hn.title_type_code = tt.title_type_code);
   END;


   PROCEDURE load_ref_name_title_type
   IS
   BEGIN
      INSERT INTO ref_holder_name_title_type
         (SELECT ref_hld_name_title_type_seq.NEXTVAL AS pk,
                 o_title_type_code,
                 gender
            FROM (  SELECT DISTINCT
                           hn.o_title_type_code, NVL (tt.gender, 'U') AS gender
                      FROM anon_holder_names hn, title_types tt
                     WHERE     hn.o_title_type_code = tt.title_type_code
                           AND o_title_type_code IS NOT NULL
                  ORDER BY o_title_type_code ASC));
   END;


   PROCEDURE load_ref_name_forenames
   IS
   BEGIN
      INSERT INTO ref_holder_name_forenames
         (SELECT pk, o_forename, gender
            FROM (  SELECT DISTINCT
                           o_forename,
                           gender,
                           DENSE_RANK ()
                           OVER (PARTITION BY gender ORDER BY o_forename ASC)
                              AS pk
                      FROM anon_holder_names hn, ref_holder_name_title_type tt
                     WHERE     hn.o_title_type_code = tt.o_title_type_code
                           AND o_forename IS NOT NULL
                  ORDER BY gender, o_forename ASC));
   END;


   PROCEDURE load_ref_name_surnames
   IS
   BEGIN
      INSERT INTO ref_holder_name_surnames
         (SELECT ref_hld_name_surnames_seq.NEXTVAL AS pk, o_surname
            FROM (  SELECT DISTINCT o_surname
                      FROM anon_holder_names
                     WHERE o_surname IS NOT NULL
                  ORDER BY o_surname ASC));
   END;



   PROCEDURE load_ref_name_suffix
   IS
   BEGIN
      INSERT INTO ref_holder_name_suffix
         (SELECT ref_hld_name_suffix_seq.NEXTVAL AS pk, o_suffix
            FROM (  SELECT DISTINCT o_suffix
                      FROM anon_holder_names
                     WHERE o_suffix IS NOT NULL
                  ORDER BY o_suffix ASC));
   END;


   PROCEDURE load_ref_name_other_title
   IS
   BEGIN
      INSERT INTO ref_holder_name_other_title
         (SELECT ref_hld_name_other_title_seq.NEXTVAL AS pk, o_other_title
            FROM (  SELECT DISTINCT o_other_title
                      FROM anon_holder_names
                     WHERE o_other_title IS NOT NULL
                  ORDER BY o_other_title ASC));
   END;


   PROCEDURE load_ref_name_preferred_name
   IS
   BEGIN
      INSERT INTO ref_holder_name_preferred_name
         (SELECT ref_hld_name_pref_name_seq.NEXTVAL AS pk, o_preferred_name
            FROM (  SELECT DISTINCT o_preferred_name
                      FROM anon_holder_names
                     WHERE o_preferred_name IS NOT NULL
                  ORDER BY o_preferred_name ASC));
   END;


   PROCEDURE load_ref_name_trust_name
   IS
   BEGIN
      INSERT INTO ref_holder_name_trust_name
         (SELECT ref_hld_name_trust_name_seq.NEXTVAL AS pk, o_trust_name
            FROM (  SELECT DISTINCT o_trust_name
                      FROM anon_holder_names
                     WHERE o_trust_name IS NOT NULL
                  ORDER BY o_trust_name ASC));
   END;



   PROCEDURE load_ref_name_trustee_name
   IS
   BEGIN
      INSERT INTO ref_holder_name_trustee_name
         (SELECT ref_hld_name_trustee_name_seq.NEXTVAL AS pk, o_trustee_name
            FROM (  SELECT DISTINCT o_trustee_name
                      FROM anon_holder_names
                     WHERE o_trustee_name IS NOT NULL
                  ORDER BY o_trustee_name ASC));
   END;



   PROCEDURE load_ref_name_company_name
   IS
   BEGIN
      INSERT INTO ref_holder_name_company_name
         SELECT ref_hld_name_company_name_seq.NEXTVAL AS pk, o_company_name
           FROM (  SELECT DISTINCT o_company_name
                     FROM anon_holder_names
                    WHERE o_company_name IS NOT NULL
                 ORDER BY o_company_name ASC);
   END;



   PROCEDURE anon_holder_names
   IS
   BEGIN
      UPDATE anon_holder_names
         SET a_forename =
                anonymisation_utility.afn_holder_forenames (
                   o_title_type_gender)
       WHERE o_forename IS NOT NULL;

      UPDATE anon_holder_names
         SET a_surname = anonymisation_utility.afn_holder_surnames ()
       WHERE o_surname IS NOT NULL;



      UPDATE anon_holder_names
         SET a_title_type_code = o_title_type_code;

      COMMIT;

      UPDATE anon_holder_names
         SET a_other_title = o_other_title;

      COMMIT;

      --Regenerate Salutation
      UPDATE anon_holder_names
         SET a_salutation =
                CASE
                   WHEN a_title_type_code = 'OTHER'
                   THEN
                      a_other_title || ' ' || a_surname
                   ELSE
                      a_title_type_code || ' ' || a_surname
                END;


      --Anonymise Preferred Name
      /*Substring the Forename
      */
      UPDATE anon_holder_names
         SET a_preferred_name =
                SUBSTR (
                   NVL (SUBSTR (a_forename, 1, INSTR (a_forename, ' ')),
                        a_forename),
                   1,
                   25)
       WHERE o_preferred_name IS NOT NULL;


      --Anonymise Trustee Name
      /*Kept constant in this release:
      No test data has a Trustee Name
      */
      UPDATE anon_holder_names
         SET a_trustee_name = o_trustee_name
       WHERE o_trustee_name IS NOT NULL;


      --Anonymise Trust Name
      /*Kept constant in this release:
      No test data has a Trust Name
      */
      UPDATE anon_holder_names
         SET a_trust_name = o_trust_name
       WHERE o_trust_name IS NOT NULL;



      --Update Concat_Name_Strings.
      UPDATE anon_holder_names
         SET o_concat_name_string =
                REPLACE (
                      DECODE (o_title_type_code,
                              'OTHER', o_other_title,
                              o_title_type_code)
                   || ' '
                   || o_forename
                   || ' '
                   || o_surname
                   || ' '
                   || o_suffix
                   || ' '
                   || DECODE (NVL (o_trust_name, 1),
                              '1', '',
                              o_trust_name || '+' || o_trustee_name)
                   || ' '
                   || o_company_name,
                   '  ',
                   ' '),
             a_concat_name_string =
                REPLACE (
                      DECODE (a_title_type_code,
                              'OTHER', a_other_title,
                              a_title_type_code)
                   || ' '
                   || a_forename
                   || ' '
                   || a_surname
                   || ' '
                   || a_suffix
                   || ' '
                   || DECODE (NVL (a_trust_name, 1),
                              '1', '',
                              a_trust_name || '+' || a_trustee_name)
                   || ' '
                   || a_company_name,
                   '  ',
                   ' ');
   END;


   PROCEDURE load_anon_holder_addresses
   IS
   BEGIN
      --Populate ANON_HOLDER_ADDRESSES table
      INSERT INTO anon_holder_addresses (PK,
                                         HOLDER_ADDRESS_ID,
                                         COMP_CODE,
                                         IVC_CODE,
                                         ADDRESS_TYPE_ID,
                                         CORRESPONDENCE_YN,
                                         O_ADDRESS_LINE1,
                                         O_ADDRESS_LINE2,
                                         O_ADDRESS_LINE3,
                                         O_ADDRESS_LINE4,
                                         O_ADDRESS_LINE5,
                                         O_ADDRESS_LINE6,
                                         O_POSTCODE_LEFT,
                                         O_POSTCODE_RIGHT,
                                         O_COUNTRY_CODE,
                                         O_IRISH_DISTRIBUTION_CODE)
         (SELECT DENSE_RANK ()
                 OVER (PARTITION BY country_code
                       ORDER BY holder_address_id ASC)
                    AS pk,
                 HOLDER_ADDRESS_ID,
                 COMP_CODE,
                 IVC_CODE,
                 ADDRESS_TYPE_ID,
                 CORRESPONDENCE_YN,
                 ADDRESS_LINE1,
                 ADDRESS_LINE2,
                 ADDRESS_LINE3,
                 ADDRESS_LINE4,
                 ADDRESS_LINE5,
                 ADDRESS_LINE6,
                 POSTCODE_LEFT,
                 POSTCODE_RIGHT,
                 COUNTRY_CODE,
                 IRISH_DISTRIBUTION_CODE
            FROM holder_addresses);
   END;


   PROCEDURE anon_weakanon_addresses
   IS
      --identify countries with less than 20 holders
      CURSOR lc_weakanon
      IS
           SELECT o_country_code
             FROM anon_holder_addresses
         GROUP BY o_country_code
           HAVING COUNT (*) <= 20;

      --identify postcodes having less than 500 holders resident
      CURSOR lc_weakpostcode
      IS
         SELECT holder_address_id, a_concate_postcode
           FROM ANON_HOLDER_ADDRESSES
          WHERE SUBSTR (o_postcode_left, 1, 2) IN
                   (  SELECT SUBSTR (o_postcode_left, 1, 2) AS postsubstr
                        FROM ANON_HOLDER_ADDRESSES
                    GROUP BY SUBSTR (o_postcode_left, 1, 2)
                      HAVING COUNT (*) < 500);

      --identify irish dist codes having less than 500 holders resident
      CURSOR lc_weakirishdist
      IS
         SELECT a_irish_distribution_code, holder_address_id
           FROM ANON_HOLDER_ADDRESSES
          WHERE SUBSTR (O_IRISH_DISTRIBUTION_CODE, 1, 2) IN
                   (  SELECT SUBSTR (O_IRISH_DISTRIBUTION_CODE, 1, 2)
                                AS irishsubstr
                        FROM ANON_HOLDER_ADDRESSES
                    GROUP BY SUBSTR (O_IRISH_DISTRIBUTION_CODE, 1, 2)
                      HAVING COUNT (*) < 500);

      l_a_concate_postcode          VARCHAR2 (10);
      l_a_irish_distribution_code   VARCHAR2 (10);
   BEGIN
      FOR i IN lc_weakanon
      LOOP
         UPDATE anon_holder_addresses
            SET A_ADDRESS_LINE1 =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_ADDRESS_LINE1),
                A_ADDRESS_LINE2 =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_ADDRESS_LINE2),
                A_ADDRESS_LINE3 =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_ADDRESS_LINE3),
                A_ADDRESS_LINE4 =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_ADDRESS_LINE4),
                A_ADDRESS_LINE5 =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_ADDRESS_LINE5),
                A_ADDRESS_LINE6 =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_ADDRESS_LINE6),
                A_CONCATE_POSTCODE =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_CONCATE_POSTCODE),
                A_CONCAT_ADDRESS_STRING =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_CONCAT_ADDRESS_STRING),
                A_IRISH_DISTRIBUTION_CODE =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_IRISH_DISTRIBUTION_CODE),
                A_POSTCODE_LEFT =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_POSTCODE_LEFT),
                A_POSTCODE_RIGHT =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_POSTCODE_RIGHT)
          WHERE o_country_code = i.o_country_code;
      END LOOP;


      FOR i IN lc_weakpostcode
      LOOP
         l_a_concate_postcode := i.a_concate_postcode;

         UPDATE anon_holder_addresses
            SET A_CONCATE_POSTCODE =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_CONCATE_POSTCODE),
                A_POSTCODE_LEFT =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_POSTCODE_LEFT),
                A_POSTCODE_RIGHT =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_POSTCODE_RIGHT)
          WHERE holder_address_id = i.holder_address_id;

         UPDATE anon_holder_addresses
            SET A_CONCAT_ADDRESS_STRING =
                   REPLACE (A_CONCAT_ADDRESS_STRING,
                            l_a_concate_postcode,
                            A_CONCATE_POSTCODE)
          WHERE holder_address_id = i.holder_address_id;
      END LOOP;


      FOR i IN lc_weakirishdist
      LOOP
         l_a_irish_distribution_code := i.a_irish_distribution_code;

         UPDATE anon_holder_addresses
            SET A_IRISH_DISTRIBUTION_CODE =
                   anonymisation_utility.afn_randomise_textdigit (
                      A_IRISH_DISTRIBUTION_CODE)
          WHERE holder_address_id = i.holder_address_id;


         UPDATE anon_holder_addresses
            SET A_CONCAT_ADDRESS_STRING =
                   REPLACE (A_CONCAT_ADDRESS_STRING,
                            l_a_irish_distribution_code,
                            a_irish_distribution_code)
          WHERE holder_address_id = i.holder_address_id;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;



   PROCEDURE anon_holder_addresses
   IS
      CURSOR lc_address
      IS
         SELECT holder_address_id
           FROM anon_holder_addresses_gtt
          WHERE a_address_line1 IS NULL;
   BEGIN
      DELETE FROM anon_holder_addresses_gtt;

      INSERT INTO anon_holder_addresses_gtt
         (SELECT *
            FROM anon_holder_addresses);

      COMMIT;

      FOR i IN lc_address
      LOOP
         anonymisation_utility.afn_holder_address (i.holder_address_id);
      END LOOP;

      COMMIT;

      MERGE INTO anon_holder_addresses aha
           USING (SELECT holder_address_id,
                         A_ADDRESS_LINE1,
                         A_ADDRESS_LINE2,
                         A_ADDRESS_LINE3,
                         A_ADDRESS_LINE4,
                         A_ADDRESS_LINE5,
                         A_ADDRESS_LINE6,
                         A_POSTCODE_LEFT,
                         A_POSTCODE_RIGHT,
                         A_CONCATE_POSTCODE,
                         A_COUNTRY_CODE,
                         A_IRISH_DISTRIBUTION_CODE,
                         A_CONCAT_ADDRESS_STRING
                    FROM anon_holder_addresses_gtt) gtt
              ON (gtt.holder_address_id = aha.holder_address_id)
      WHEN MATCHED
      THEN
         UPDATE SET
            aha.A_ADDRESS_LINE1 = gtt.A_ADDRESS_LINE1,
            aha.A_ADDRESS_LINE2 = gtt.A_ADDRESS_LINE2,
            aha.A_ADDRESS_LINE3 = gtt.A_ADDRESS_LINE3,
            aha.A_ADDRESS_LINE4 = gtt.A_ADDRESS_LINE4,
            aha.A_ADDRESS_LINE5 = gtt.A_ADDRESS_LINE5,
            aha.A_ADDRESS_LINE6 = gtt.A_ADDRESS_LINE6,
            aha.A_POSTCODE_LEFT = gtt.A_POSTCODE_LEFT,
            aha.A_POSTCODE_RIGHT = gtt.A_POSTCODE_RIGHT,
            aha.A_CONCATE_POSTCODE = gtt.A_CONCATE_POSTCODE,
            aha.A_COUNTRY_CODE = gtt.A_COUNTRY_CODE,
            aha.A_IRISH_DISTRIBUTION_CODE = gtt.A_IRISH_DISTRIBUTION_CODE,
            aha.A_CONCAT_ADDRESS_STRING = gtt.A_CONCAT_ADDRESS_STRING;

      COMMIT;
   END;



   PROCEDURE load_anon_holder
   IS
   BEGIN
      INSERT INTO anon_holder (COMP_CODE,
                               IVC_CODE,
                               AMALGAMATED_IVC,
                               O_SORT_KEY,
                               O_PARTICIPANT_CODE,
                               O_SHARE_MASTER_HOLDER_CODE,
                               O_REFERENCE_IVC_CODE,
                               O_DESIGNATION_NAME,
                               O_COUNTRY_CODE,
                               O_DATE_OF_DEATH,
                               O_GENDER,
                               O_DATE_OF_BIRTH,
                               O_NATIONAL_INSURANCE_NO,
                               O_PERSONNEL_NUMBER,
                               O_PAYROLL_NUMBER)
         (SELECT hld.comp_code                AS comp_code,
                 hld.ivc_code                 AS ivc_code,
                 hld.amalgamated_ivc          AS amalgamated_ivc,
                 hld.sort_key                 AS o_sort_key,
                 hld.participant_code         AS o_participant_code,
                 hld.share_master_holder_code AS o_share_master_holder_code,
                 hld.reference_ivc_code       AS o_reference_ivc_code,
                 hld.designation_name         AS o_designation_name,
                 hld.country_code             AS o_country_code,
                 hld.date_of_death            AS o_date_of_death,
                 hed.gender                   AS o_gender,
                 hed.date_of_birth            AS o_date_of_birth,
                 hed.national_insurance_no    AS o_national_insurance_no,
                 hed.personnel_number         AS o_personnel_number,
                 hed.payroll_number           AS o_payroll_number
            FROM holders hld
                 LEFT OUTER JOIN holder_employee_details hed
                    ON     hld.comp_code = hed.comp_code
                       AND hld.ivc_code = hed.ivc_code);
   END;



   PROCEDURE anon_holder
   IS
   BEGIN
      UPDATE anon_holder ahld1
         SET (ahld1.A_SORT_KEY,
              ahld1.A_PARTICIPANT_CODE,
              ahld1.A_SHARE_MASTER_HOLDER_CODE,
              ahld1.A_REFERENCE_IVC_CODE,
              ahld1.A_DESIGNATION_NAME,
              ahld1.A_COUNTRY_CODE,
              ahld1.A_DATE_OF_DEATH,
              ahld1.A_GENDER,
              ahld1.A_DATE_OF_BIRTH,
              ahld1.A_NATIONAL_INSURANCE_NO,
              ahld1.A_PERSONNEL_NUMBER,
              ahld1.A_PAYROLL_NUMBER) =
                (SELECT A_SORT_KEY,
                        A_PARTICIPANT_CODE,
                        A_SHARE_MASTER_HOLDER_CODE,
                        A_REFERENCE_IVC_CODE,
                        A_DESIGNATION_NAME,
                        A_COUNTRY_CODE,
                        A_DATE_OF_DEATH,
                        A_GENDER,
                        A_DATE_OF_BIRTH,
                        A_NATIONAL_INSURANCE_NO,
                        A_PERSONNEL_NUMBER,
                        A_PAYROLL_NUMBER
                   FROM (SELECT COMP_CODE,
                                IVC_CODE,
                                'MYNEWKEY'           AS A_SORT_KEY,
                                o_participant_code   AS A_PARTICIPANT_CODE,
                                O_SHARE_MASTER_HOLDER_CODE
                                   AS A_SHARE_MASTER_HOLDER_CODE,
                                O_REFERENCE_IVC_CODE AS A_REFERENCE_IVC_CODE,
                                O_DESIGNATION_NAME   AS A_DESIGNATION_NAME,
                                O_COUNTRY_CODE       AS A_COUNTRY_CODE,
                                anonymisation_utility.afn_randomise_date (
                                   O_DATE_OF_DEATH)
                                   AS A_DATE_OF_DEATH,
                                O_GENDER             AS A_GENDER,
                                anonymisation_utility.afn_randomise_date (
                                   O_DATE_OF_BIRTH)
                                   AS A_DATE_OF_BIRTH,
                                --'NI'||lpad(DENSE_RANK() OVER(PARTITION BY COMP_CODE ORDER BY IVC_CODE ASC),6,'0')||'C' as A_NATIONAL_INSURANCE_NO,
                                'NI000000C'
                                   AS A_NATIONAL_INSURANCE_NO,
                                DECODE (O_PERSONNEL_NUMBER,
                                        NULL, NULL,
                                        'PE' || IVC_CODE)
                                   AS A_PERSONNEL_NUMBER,
                                DECODE (O_PAYROLL_NUMBER,
                                        NULL, NULL,
                                        'PR' || IVC_CODE)
                                   AS A_PAYROLL_NUMBER
                           FROM anon_holder ahld2)
                  WHERE     ivc_code = ahld1.ivc_code
                        AND comp_code = ahld1.comp_code);
   END;


   PROCEDURE anon_holder_ninumber
   IS
      CURSOR lc_updateni
      IS
         WITH getrank
              AS (SELECT ivc_code,
                            'NI'
                         || LPAD (
                               DENSE_RANK ()
                               OVER (PARTITION BY comp_code
                                     ORDER BY ivc_code ASC),
                               6,
                               '0')
                         || 'C'
                            AS newni
                    FROM anon_holder ahld1)
         SELECT    'update ANON_HOLDER set A_NATIONAL_INSURANCE_NO = '''
                || getrank.newni
                || ''' where ivc_code='''
                || getrank.ivc_code
                || ''''
                   AS stmt
           FROM getrank, anon_holder ahld2
          WHERE getrank.ivc_code = ahld2.ivc_code;
   BEGIN
      FOR i IN lc_updateni
      LOOP
         EXECUTE IMMEDIATE (i.stmt);
      END LOOP;
   END;

   PROCEDURE merge_holder_names
   IS
   BEGIN
      --
      MERGE INTO holder_names hn
           USING (SELECT HOLDER_NAME_ID,
                         A_TITLE_TYPE_CODE,
                         A_SURNAME,
                         A_FORENAME,
                         A_SUFFIX,
                         A_SALUTATION,
                         A_OTHER_TITLE,
                         A_PREFERRED_NAME,
                         A_TRUST_NAME,
                         A_TRUSTEE_NAME,
                         A_COMPANY_NAME
                    FROM anon_holder_names) ahn
              ON (ahn.holder_name_id = hn.holder_name_id)
      WHEN MATCHED
      THEN
         UPDATE SET hn.TITLE_TYPE_CODE = ahn.A_TITLE_TYPE_CODE,
                    hn.SURNAME = ahn.A_SURNAME,
                    hn.FORENAME = ahn.A_FORENAME,
                    hn.SUFFIX = ahn.A_SUFFIX,
                    hn.SALUTATION = ahn.A_SALUTATION,
                    hn.OTHER_TITLE = ahn.A_OTHER_TITLE,
                    hn.PREFERRED_NAME = ahn.A_PREFERRED_NAME,
                    hn.TRUST_NAME = ahn.A_TRUST_NAME,
                    hn.TRUSTEE_NAME = ahn.A_TRUSTEE_NAME,
                    hn.COMPANY_NAME = ahn.A_COMPANY_NAME;

      --Commit is necessary for the get_sort_key_function to work
      COMMIT;

      --Regenerate the Holder Sort Key in  anonymise schema with new details
      UPDATE anon_holder ah
         SET ah.a_sort_key =
                holder_crud.get_sort_key (ah.a_designation_name,
                                          ah.ivc_code,
                                          ah.comp_code);

      COMMIT;

      --Publish new sort key
      MERGE INTO holders hl
           USING (SELECT comp_code, ivc_code, a_sort_key
                    FROM anon_holder ah) ah
              ON (hl.comp_code = ah.comp_code AND hl.ivc_code = ah.ivc_code)
      WHEN MATCHED
      THEN
         UPDATE SET hl.sort_key = ah.a_sort_key;
   END;

   PROCEDURE anon_bank_accounts
   IS
   BEGIN
      --
      MERGE INTO anon_bank_accounts aba
           USING (SELECT bank_account_id AS bank_account_id,
                         LPAD (
                            DENSE_RANK ()
                               OVER (PARTITION BY 1 ORDER BY bank_account_id),
                            10,
                            0)
                            AS a_account_no,
                            sortcode
                         || LPAD (
                               DENSE_RANK ()
                               OVER (PARTITION BY 1 ORDER BY bank_account_id),
                               10,
                               0)
                            AS a_capita_reference,
                         CASE
                            WHEN LENGTH (o_bacs_code) > 0
                            THEN
                               LPAD (
                                  DENSE_RANK ()
                                  OVER (PARTITION BY 1
                                        ORDER BY bank_account_id),
                                  6,
                                  2)
                         END
                            AS a_bacs_code,
                         CASE
                            WHEN LENGTH (o_iban) > 0
                            THEN
                                  SUBSTR (o_iban, 1, 8)
                               || sortcode
                               || LPAD (
                                     DENSE_RANK ()
                                     OVER (PARTITION BY 1
                                           ORDER BY bank_account_id),
                                     10,
                                     0)
                         END
                            AS a_iban
                    FROM anon_bank_accounts) aba2
              ON (aba.bank_account_id = aba2.bank_account_id)
      WHEN MATCHED
      THEN
         UPDATE SET aba.a_account_no = aba2.a_account_no,
                    aba.a_capita_reference = aba2.a_capita_reference,
                    aba.a_bacs_code = aba2.a_bacs_code,
                    aba.a_iban = aba2.a_iban;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;

   PROCEDURE merge_bank_accounts
   IS
   BEGIN
      --
      MERGE INTO bank_accounts ba
           USING (SELECT bank_account_id,
                         a_account_no,
                         a_capita_reference,
                         a_bacs_code,
                         a_iban
                    FROM anon_bank_accounts) aba
              ON (ba.bank_account_id = aba.bank_account_id)
      WHEN MATCHED
      THEN
         UPDATE SET ba.account_no = aba.a_account_no,
                    ba.capita_reference = aba.a_capita_reference,
                    ba.bacs_code = aba.a_bacs_code,
                    ba.iban = aba.a_iban;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;


   PROCEDURE merge_holder_address
   IS
   BEGIN
      --
      MERGE INTO holder_addresses ha
           USING (SELECT HOLDER_ADDRESS_ID,
                         A_ADDRESS_LINE1,
                         A_ADDRESS_LINE2,
                         A_ADDRESS_LINE3,
                         A_ADDRESS_LINE4,
                         A_ADDRESS_LINE5,
                         A_ADDRESS_LINE6,
                         A_POSTCODE_LEFT,
                         A_POSTCODE_RIGHT,
                         A_CONCATE_POSTCODE,
                         A_COUNTRY_CODE,
                         A_IRISH_DISTRIBUTION_CODE,
                         A_CONCAT_ADDRESS_STRING
                    FROM anon_holder_addresses) aha
              ON (aha.holder_Address_id = ha.holder_address_id)
      WHEN MATCHED
      THEN
         UPDATE SET
            ha.ADDRESS_LINE1 = aha.A_ADDRESS_LINE1,
            ha.ADDRESS_LINE2 = aha.A_ADDRESS_LINE2,
            ha.ADDRESS_LINE3 = aha.A_ADDRESS_LINE3,
            ha.ADDRESS_LINE4 = aha.A_ADDRESS_LINE4,
            ha.ADDRESS_LINE5 = aha.A_ADDRESS_LINE5,
            ha.ADDRESS_LINE6 = aha.A_ADDRESS_LINE6,
            ha.POSTCODE_LEFT = aha.A_POSTCODE_LEFT,
            ha.POSTCODE_RIGHT = aha.A_POSTCODE_RIGHT,
            ha.country_code = aha.A_COUNTRY_CODE,
            ha.IRISH_DISTRIBUTION_CODE = aha.A_IRISH_DISTRIBUTION_CODE;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;

   PROCEDURE merge_holder_employee_details
   IS
   BEGIN
      --
      MERGE INTO holder_employee_details hed
           USING (SELECT comp_code,
                         ivc_code,
                         a_national_insurance_no,
                         a_date_of_birth,
                         a_personnel_number,
                         a_payroll_number,
                         a_gender
                    FROM anon_holder) ah
              ON (hed.comp_code = ah.comp_code AND hed.ivc_code = ah.ivc_code)
      WHEN MATCHED
      THEN
         UPDATE SET hed.NATIONAL_INSURANCE_NO = ah.a_national_insurance_no,
                    hed.date_of_birth = ah.a_date_of_birth,
                    hed.personnel_number = ah.a_personnel_number,
                    hed.payroll_number = ah.a_payroll_number;

      MERGE INTO holder_employee_detail_hist hedh
           USING (SELECT comp_code,
                         ivc_code,
                         a_national_insurance_no,
                         a_date_of_birth,
                         a_personnel_number,
                         a_payroll_number,
                         a_gender
                    FROM anon_holder) ah
              ON (    hedh.comp_code = ah.comp_code
                  AND hedh.ivc_code = ah.ivc_code)
      WHEN MATCHED
      THEN
         UPDATE SET
            hedh.NATIONAL_INSURANCE_NO =
               DECODE (hedh.NATIONAL_INSURANCE_NO,
                       NULL, NULL,
                       ah.a_national_insurance_no),
            hedh.date_of_birth =
               DECODE (hedh.date_of_birth, NULL, NULL, ah.a_date_of_birth),
            hedh.personnel_number =
               DECODE (hedh.personnel_number,
                       NULL, NULL,
                       ah.a_personnel_number),
            hedh.payroll_number =
               DECODE (hedh.payroll_number, NULL, NULL, ah.a_payroll_number);


      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;

   PROCEDURE merge_holder_labels
   IS
   BEGIN
      --
      MERGE INTO holder_labels hl
           USING (SELECT comp_code, ivc_code, a_concat_name_string
                    FROM anon_holder_names
                   WHERE holder_seq = 1) ahn
              ON (hl.comp_code = ahn.comp_code AND hl.ivc_code = ahn.ivc_code)
      WHEN MATCHED
      THEN
         UPDATE SET hl.line1_text = SUBSTR (ahn.a_concat_name_string, 1, 35),
                    line2_text = NULL,
                    line3_text = NULL,
                    line4_text = NULL;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;


   PROCEDURE merge_holder_mandate_details
   IS
   BEGIN
      --
      MERGE INTO holder_mandates hm
           USING (SELECT aha.comp_code,
                         aha.ivc_code,
                         aha.A_ADDRESS_LINE1,
                         aha.A_ADDRESS_LINE2,
                         aha.A_ADDRESS_LINE3,
                         aha.A_ADDRESS_LINE4,
                         aha.A_ADDRESS_LINE5,
                         aha.A_ADDRESS_LINE6,
                         aha.A_POSTCODE_LEFT,
                         aha.A_POSTCODE_RIGHT,
                         aha.A_CONCATE_POSTCODE,
                         aha.A_COUNTRY_CODE,
                         aha.A_IRISH_DISTRIBUTION_CODE,
                         aha.A_CONCAT_ADDRESS_STRING
                    FROM ANON_HOLDER_ADDRESSES aha
                   WHERE ADDRESS_TYPE_ID = 4) aha
              ON (hm.comp_code = aha.comp_code AND hm.ivc_code = aha.ivc_code)
      WHEN MATCHED
      THEN
         UPDATE SET
            hm.ADDRESS_LINE1 = aha.A_ADDRESS_LINE1,
            hm.ADDRESS_LINE2 = aha.A_ADDRESS_LINE2,
            hm.ADDRESS_LINE3 = aha.A_ADDRESS_LINE3,
            hm.ADDRESS_LINE4 = aha.A_ADDRESS_LINE4,
            hm.ADDRESS_LINE5 = aha.A_ADDRESS_LINE5,
            hm.ADDRESS_LINE6 = aha.A_ADDRESS_LINE6,
            hm.POSTCODE_LEFT = aha.A_POSTCODE_LEFT,
            hm.POSTCODE_RIGHT = aha.A_POSTCODE_RIGHT,
            hm.IRISH_DISTRIBUTION_CODE = aha.A_IRISH_DISTRIBUTION_CODE,
            hm.COUNTRY_CODE = aha.A_COUNTRY_CODE
                 WHERE        hm.ADDRESS_LINE1
                           || hm.ADDRESS_LINE2
                           || hm.ADDRESS_LINE3
                           || hm.ADDRESS_LINE4
                           || hm.ADDRESS_LINE5
                           || hm.POSTCODE_LEFT
                           || hm.POSTCODE_RIGHT
                           || hm.IRISH_DISTRIBUTION_CODE
                              IS NOT NULL
                       AND --*DEFECT FIX ensure International Manadate Address is not anonymised.
                          mandate_type_id NOT IN (4, 5, 6);

      MERGE INTO holder_mandates hm
           USING (SELECT ahn.comp_code,
                         ahn.ivc_code,
                         ahn.A_CONCAT_NAME_STRING
                    FROM ANON_HOLDER_NAMES ahn
                   WHERE NVL (holder_seq, 1) <= 1) ahn
              ON (hm.comp_code = ahn.comp_code AND hm.ivc_code = ahn.ivc_code)
      WHEN MATCHED
      THEN
         UPDATE SET hm.PAYEE_NAME = ahn.A_CONCAT_NAME_STRING
                 WHERE hm.PAYEE_NAME IS NOT NULL;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;



   PROCEDURE merge_holder_payments
   IS
   BEGIN
      --
      MERGE INTO payments py
           USING (SELECT py.PAYMENT_ID,
                         aha.comp_code,
                         aha.ivc_code,
                         aha.A_ADDRESS_LINE1,
                         aha.A_ADDRESS_LINE2,
                         aha.A_ADDRESS_LINE3,
                         aha.A_ADDRESS_LINE4,
                         aha.A_ADDRESS_LINE5,
                         aha.A_ADDRESS_LINE6,
                         aha.A_POSTCODE_LEFT,
                         aha.A_POSTCODE_RIGHT,
                         aha.A_CONCATE_POSTCODE,
                         aha.A_COUNTRY_CODE,
                         aha.A_IRISH_DISTRIBUTION_CODE,
                         aha.A_CONCAT_ADDRESS_STRING
                    FROM ANON_HOLDER_ADDRESSES aha,
                         PAYMENTS              py,
                         CASH_TRANSACTIONS     ct,
                         CASH_ACCOUNTS         ca
                   WHERE     py.cash_transaction_id = ct.cash_transaction_id
                         AND ct.cash_account_id = ca.cash_account_id
                         AND ca.COMP_CODE = aha.COMP_CODE
                         AND ca.IVC_CODE = aha.IVC_CODE
                         AND aha.address_type_id = 4) aha
              ON (py.payment_id = aha.payment_id)
      WHEN MATCHED
      THEN
         UPDATE SET
            DISPATCH_ADDRESS_LINE1 = A_ADDRESS_LINE1,
            DISPATCH_ADDRESS_LINE2 = A_ADDRESS_LINE2,
            DISPATCH_ADDRESS_LINE3 = A_ADDRESS_LINE3,
            DISPATCH_ADDRESS_LINE4 = A_ADDRESS_LINE4,
            DISPATCH_ADDRESS_LINE5 = A_ADDRESS_LINE5,
            DISPATCH_ADDRESS_LINE6 = A_ADDRESS_LINE6,
            POSTCODE_LEFT = A_POSTCODE_LEFT,
            POSTCODE_RIGHT = A_POSTCODE_RIGHT,
            IRISH_DISTRIBUTION_CODE = A_IRISH_DISTRIBUTION_CODE,
            COUNTRY_CODE = A_COUNTRY_CODE
                 WHERE    py.DISPATCH_ADDRESS_LINE1
                       || py.DISPATCH_ADDRESS_LINE2
                       || py.DISPATCH_ADDRESS_LINE3
                       || py.DISPATCH_ADDRESS_LINE4
                       || py.DISPATCH_ADDRESS_LINE5
                       || py.DISPATCH_ADDRESS_LINE6
                       || py.POSTCODE_LEFT
                       || py.POSTCODE_RIGHT
                       || py.IRISH_DISTRIBUTION_CODE
                          IS NOT NULL;


      MERGE INTO payments py
           USING (SELECT py.PAYMENT_ID,
                         aha.comp_code,
                         aha.ivc_code,
                         aha.A_ADDRESS_LINE1,
                         aha.A_ADDRESS_LINE2,
                         aha.A_ADDRESS_LINE3,
                         aha.A_ADDRESS_LINE4,
                         aha.A_ADDRESS_LINE5,
                         aha.A_ADDRESS_LINE6,
                         aha.A_POSTCODE_LEFT,
                         aha.A_POSTCODE_RIGHT,
                         aha.A_CONCATE_POSTCODE,
                         aha.A_COUNTRY_CODE,
                         aha.A_IRISH_DISTRIBUTION_CODE,
                         aha.A_CONCAT_ADDRESS_STRING
                    FROM ANON_HOLDER_ADDRESSES aha,
                         PAYMENTS              py,
                         CASH_TRANSACTIONS     ct,
                         CASH_ACCOUNTS         ca
                   WHERE     py.cash_transaction_id = ct.cash_transaction_id
                         AND ct.cash_account_id = ca.cash_account_id
                         AND ca.COMP_CODE = aha.COMP_CODE
                         AND ca.IVC_CODE = aha.IVC_CODE
                         AND aha.address_type_id = 4) aha
              ON (py.payment_id = aha.payment_id)
      WHEN MATCHED
      THEN
         UPDATE SET
            py.PAYEE_CHEQUEADDRESS_LINE1 = aha.A_ADDRESS_LINE1,
            py.PAYEE_CHEQUEADDRESS_LINE2 = aha.A_ADDRESS_LINE2,
            py.PAYEE_CHEQUEADDRESS_LINE3 = aha.A_ADDRESS_LINE3,
            py.PAYEE_CHEQUEADDRESS_LINE4 = aha.A_ADDRESS_LINE4,
            py.PAYEE_CHEQUEADDRESS_LINE5 = aha.A_ADDRESS_LINE5,
            py.PAYEE_CHEQUEADDRESS_LINE6 = aha.A_ADDRESS_LINE6,
            py.PAYEE_COUNTRY = aha.A_COUNTRY_CODE
                 WHERE    py.PAYEE_CHEQUEADDRESS_LINE1
                       || py.PAYEE_CHEQUEADDRESS_LINE2
                       || py.PAYEE_CHEQUEADDRESS_LINE3
                       || py.PAYEE_CHEQUEADDRESS_LINE4
                       || py.PAYEE_CHEQUEADDRESS_LINE5
                       || py.PAYEE_CHEQUEADDRESS_LINE6
                       || py.PAYEE_COUNTRY
                          IS NOT NULL;


      --PAYEE_NAME1, PAYEE_NAME2, PAYEE_NAME3, PAYEE_NAME4, PAYEE_NAME5, DISPATCH_NAME1, DISPATCH_NAME2, PAYEE_ALIAS


      MERGE INTO payments py
           USING (SELECT py.payment_id,
                         ahn.comp_code,
                         ahn.ivc_code,
                         ahn.A_CONCAT_NAME_STRING
                    FROM ANON_HOLDER_NAMES ahn,
                         PAYMENTS          py,
                         CASH_TRANSACTIONS ct,
                         CASH_ACCOUNTS     ca
                   WHERE     NVL (holder_seq, 1) <= 1
                         AND py.cash_transaction_id = ct.cash_transaction_id
                         AND ct.cash_account_id = ca.cash_account_id
                         AND ca.COMP_CODE = ahn.COMP_CODE
                         AND ca.IVC_CODE = ahn.IVC_CODE) aha
              ON (py.payment_id = aha.payment_id)
      WHEN MATCHED
      THEN
         UPDATE SET
            PAYEE_NAME1 =
               DECODE (PAYEE_NAME1,
                       NULL, PAYEE_NAME1,
                       SUBSTR (aha.A_CONCAT_NAME_STRING, 1, 35)),
            PAYEE_NAME2 =
               DECODE (PAYEE_NAME2,
                       NULL, PAYEE_NAME2,
                       SUBSTR (aha.A_CONCAT_NAME_STRING, 1, 35)),
            PAYEE_NAME3 =
               DECODE (PAYEE_NAME3,
                       NULL, PAYEE_NAME3,
                       SUBSTR (aha.A_CONCAT_NAME_STRING, 1, 35)),
            PAYEE_NAME4 =
               DECODE (PAYEE_NAME4,
                       NULL, PAYEE_NAME4,
                       SUBSTR (aha.A_CONCAT_NAME_STRING, 1, 35)),
            PAYEE_NAME5 =
               DECODE (PAYEE_NAME5,
                       NULL, PAYEE_NAME5,
                       SUBSTR (aha.A_CONCAT_NAME_STRING, 1, 35)),
            DISPATCH_NAME1 =
               DECODE (DISPATCH_NAME1,
                       NULL, DISPATCH_NAME1,
                       SUBSTR (aha.A_CONCAT_NAME_STRING, 1, 35)),
            DISPATCH_NAME2 =
               DECODE (DISPATCH_NAME2,
                       NULL, DISPATCH_NAME2,
                       SUBSTR (aha.A_CONCAT_NAME_STRING, 1, 35)),
            PAYEE_ALIAS =
               DECODE (PAYEE_ALIAS,
                       NULL, PAYEE_ALIAS,
                       aha.A_CONCAT_NAME_STRING)
                 WHERE    py.PAYEE_NAME1
                       || py.PAYEE_NAME2
                       || py.PAYEE_NAME3
                       || py.PAYEE_NAME4
                       || py.PAYEE_NAME5
                       || py.DISPATCH_NAME1
                       || py.DISPATCH_NAME2
                       || py.PAYEE_ALIAS
                          IS NOT NULL;



      COMMIT;


      MERGE INTO payments pay
           USING (SELECT bank_account_id, o_account_no, sortcode
                    FROM anon_bank_accounts) aba
              ON (pay.payer_sortcode || LTRIM (pay.payer_account_number, 0) =
                     aba.sortcode || LTRIM (aba.o_account_no, 0))
      WHEN MATCHED
      THEN
         UPDATE SET pay.payer_bank_account_id = aba.bank_account_id;


      MERGE INTO payments pay
           USING (SELECT o_account_no,
                         a_account_no,
                         sortcode,
                         bank_account_id
                    FROM anon_bank_accounts) aba
              ON (pay.payer_bank_account_id = aba.bank_account_id)
      WHEN MATCHED
      THEN
         UPDATE SET pay.payer_account_number = aba.a_account_no;

      COMMIT;

      --DR27801
      MERGE INTO payments p1
           USING (SELECT DENSE_RANK ()
                         OVER (PARTITION BY payer_account_number
                               ORDER BY cheque_no ASC)
                            AS new_cheque_no,
                         payer_account_number,
                         payment_id,
                         cheque_no
                    FROM payments
                   WHERE cheque_no IS NOT NULL) p2
              ON (p1.payment_id = p2.payment_id)
      WHEN MATCHED
      THEN
         UPDATE SET p1.cheque_no = p2.new_cheque_no;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;

   PROCEDURE anon_capitareference
   IS
   BEGIN
      UPDATE bank_accounts
         SET capita_reference =
                   sortcode
                || LPAD (SUBSTR (TO_CHAR (bank_account_id), 1, 8), 8, '0')
       WHERE capita_reference IS NOT NULL;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;

   PROCEDURE load_anon_bank_accounts
   IS
   BEGIN
      INSERT INTO anon_bank_accounts (BANK_ACCOUNT_ID,
                                      BANK_NAME,
                                      BRANCH_NAME,
                                      ALIAS_NAME,
                                      ACCOUNT_NAME,
                                      CHEQUEADDRESS_LINE1_TEXT,
                                      CHEQUEADDRESS_LINE2_TEXT,
                                      CHEQUEADDRESS_LINE3_TEXT,
                                      CHEQUEADDRESS_LINE4_TEXT,
                                      CHEQUEADDRESS_LINE5_TEXT,
                                      CHEQUEADDRESS_LINE6_TEXT,
                                      BIC,
                                      SORTCODE,
                                      O_ACCOUNT_NO,
                                      O_IBAN,
                                      O_INTERNATIONAL_ACCOUNT_NO,
                                      O_BUILDING_SOCIETY_ACCOUNT_NO,
                                      O_CAPITA_REFERENCE,
                                      CURRENCY_CODE,
                                      BANK_ACCOUNT_TYPE_CODE,
                                      REG_STATUS_TYPE_CODE,
                                      STATUS_TYPE_CODE,
                                      COUNTRY_CODE,
                                      SECOND_SORTCODE,
                                      O_BACS_CODE)
         SELECT ba.BANK_ACCOUNT_ID,
                ba.BANK_NAME,
                ba.BRANCH_NAME,
                ba.ALIAS_NAME,
                ba.ACCOUNT_NAME,
                ba.CHEQUEADDRESS_LINE1_TEXT,
                ba.CHEQUEADDRESS_LINE2_TEXT,
                ba.CHEQUEADDRESS_LINE3_TEXT,
                ba.CHEQUEADDRESS_LINE4_TEXT,
                ba.CHEQUEADDRESS_LINE5_TEXT,
                ba.CHEQUEADDRESS_LINE6_TEXT,
                ba.BIC,
                ba.SORTCODE,
                ba.ACCOUNT_NO               AS O_ACCOUNT_NO,
                ba.IBAN                     AS O_IBAN,
                ba.INTERNATIONAL_ACCOUNT_NO AS O_INTERNATIONAL_ACCOUNT_NO,
                ba.BUILDING_SOCIETY_ACCOUNT_NO
                   AS O_BUILDING_SOCIETY_ACCOUNT_NO,
                ba.CAPITA_REFERENCE         AS O_CAPITA_REFERENCE,
                ba.CURRENCY_CODE,
                ba.BANK_ACCOUNT_TYPE_CODE,
                ba.REG_STATUS_TYPE_CODE,
                ba.STATUS_TYPE_CODE,
                ba.COUNTRY_CODE,
                ba.SECOND_SORTCODE,
                ba.BACS_CODE                AS O_BACS_CODE
           FROM bank_accounts ba;
   END;
END;
/