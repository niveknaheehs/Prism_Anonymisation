CREATE OR REPLACE PACKAGE BODY anonymisation_utility
AS
   FUNCTION afn_randomise_digit (p_code VARCHAR2)
      RETURN VARCHAR2
   IS
      l_standard_string   VARCHAR2 (4000);
   BEGIN
      IF p_code IS NOT NULL
      THEN
         --Replace all Digits with a single random digit.
         l_standard_string :=
            REGEXP_REPLACE (
               p_code,
               '([[:digit:]])',
               SUBSTR (TO_CHAR (ROUND (DBMS_RANDOM.VALUE (0, 9))), 1, 1));
      --The resulting string contains the same number of Alpha and numeric characters in the same order.
      END IF;

      RETURN l_standard_string;
   END;

   FUNCTION afn_randomise_date (p_date DATE)
      RETURN DATE
   IS
      l_anon_date   DATE;
   BEGIN
      --Add or subtract +/- 30 Days to provided date
      IF p_date IS NOT NULL
      THEN
         l_anon_date := p_date + ROUND (DBMS_RANDOM.VALUE (-30, 30));
      END IF;

      RETURN l_anon_date;
   END;

   FUNCTION afn_randomise_textdigit (p_code VARCHAR2)
      RETURN VARCHAR2
   IS
      l_standard_string   VARCHAR2 (4000);
   BEGIN
      IF p_code IS NOT NULL
      THEN
         --Replace all Digits with a single random digit.
         l_standard_string :=
            REGEXP_REPLACE (
               p_code,
               '([[:digit:]])',
               SUBSTR (TO_CHAR (ROUND (DBMS_RANDOM.VALUE (0, 9))), 1, 1));
         --Replace all alpha with a single alpha character.
         l_standard_string :=
            REGEXP_REPLACE (l_standard_string,
                            '([[:alpha:]])',
                            DBMS_RANDOM.string ('U', 1));
      --The resulting string contains the same number of Alpha and numeric characters in the same order.
      END IF;

      RETURN l_standard_string;
   END;


   FUNCTION afn_gen_upd_date_stmt (p_owner           VARCHAR2,
                                   p_table_name      VARCHAR2,
                                   p_column_name     VARCHAR2,
                                   p_anonymise       VARCHAR2,
                                   p_schemaprefix    VARCHAR2)
      RETURN VARCHAR2
   IS
      l_stmt    VARCHAR2 (4000);
      l_owner   VARCHAR2 (60);
   BEGIN
      l_owner := p_schemaprefix || '_' || p_owner;
      l_stmt :=
            'update '
         || l_owner
         || '.'
         || p_table_name
         || ' '
         || ' set '
         || p_column_name
         || ' = '
         || p_anonymise
         || '.anonymisation_utility.AFN_RANDOMISE_DATE('
         || p_column_name
         || ') where '
         || p_column_name
         || ' is not null';

      RETURN l_stmt;
   END;


   FUNCTION afn_gen_upd_digittext_stmt (p_owner           VARCHAR2,
                                        p_table_name      VARCHAR2,
                                        p_column_name     VARCHAR2,
                                        p_anonymise       VARCHAR2,
                                        p_schemaprefix    VARCHAR2)
      RETURN VARCHAR2
   IS
      l_stmt    VARCHAR2 (4000);
      l_owner   VARCHAR2 (60);
   BEGIN
      l_owner := p_schemaprefix || '_' || p_owner;
      l_stmt :=
            'update '
         || l_owner
         || '.'
         || p_table_name
         || ' '
         || ' set '
         || p_column_name
         || ' = '
         || p_anonymise
         || '.anonymisation_utility.AFN_RANDOMISE_TEXTDIGIT('
         || p_column_name
         || ') where '
         || p_column_name
         || ' is not null';
      RETURN l_stmt;
   END;


   FUNCTION afn_gen_upd_reord_digit_stmt (p_owner            VARCHAR2,
                                          p_table_name       VARCHAR2,
                                          p_column_name      VARCHAR2,
                                          p_anonymise        VARCHAR2,
                                          p_schema_prefix    VARCHAR2)
      RETURN VARCHAR2
   IS
      l_stmt    VARCHAR2 (4000);
      l_owner   VARCHAR2 (60);
   BEGIN
      l_owner := p_schema_prefix || '_' || p_owner;

      l_stmt :=
            'update '
         || l_owner
         || '.'
         || p_table_name
         || ' ALIAS_D'
         || ' set '
         || p_column_name
         || ' = '
         || '(
--This is the Inner randomisation query
SELECT
'
         || p_column_name
         || '_NEW '
         || 'FROM
  (SELECT '
         || p_column_name
         || '_NEW, '
         || 'rownum AS rownum_a2
  FROM
    (SELECT '
         || p_anonymise
         || '.anonymisation_utility.afn_randomise_digit('
         || p_column_name
         || ') AS '
         || p_column_name
         || '_NEW,
      ROWNUM                                                             AS rownum_a
    FROM '
         || l_owner
         || '.'
         || p_table_name
         || '
    ORDER BY dbms_random.value()
    )
  ) ALIAS_A ,
  (SELECT
  old_rowid,'
         || p_column_name
         || ',
    rownum AS rownum_b2
  FROM
    (SELECT rowid as old_rowid, '
         || p_column_name
         || ',
      ROWNUM AS rownum_b
    FROM '
         || l_owner
         || '.'
         || p_table_name
         || '
    ORDER BY rowid asc
    )
  ) ALIAS_B
WHERE ALIAS_A.rownum_a2 = ALIAS_B.rownum_b2
---End Inner randomisation query
--and Join against outer table
and old_rowid = alias_d.rowid
)';

      RETURN l_stmt;
   END;


   --Anonymise Holder Forenames within Gender
   FUNCTION afn_holder_forenames (pi_gender VARCHAR2)
      RETURN VARCHAR2
   IS
      l_return   VARCHAR2 (4000);
      l_maxid    NUMBER;
      l_minid    NUMBER;
      l_rownum   NUMBER;
   BEGIN
      SELECT MAX (pk), MIN (pk)
        INTO l_maxid, l_minid
        FROM ref_holder_name_forenames
       WHERE gender = pi_gender;

      SELECT ROUND (DBMS_RANDOM.VALUE (l_minid, l_maxid))
        INTO l_rownum
        FROM DUAL;

      SELECT o_forename
        INTO l_return
        FROM (SELECT o_forename
                FROM ref_holder_name_forenames
               WHERE pk = l_rownum AND gender = pi_gender);

      RETURN l_return;
   END;


   FUNCTION afn_holder_surnames
      RETURN VARCHAR2
   IS
      l_return   VARCHAR2 (4000);
      l_maxid    NUMBER;
      l_minid    NUMBER;
      l_rownum   NUMBER;
   BEGIN
      SELECT MAX (pk), MIN (pk)
        INTO l_maxid, l_minid
        FROM REF_HOLDER_NAME_SURNAMES;

      SELECT ROUND (DBMS_RANDOM.VALUE (l_minid, l_maxid))
        INTO l_rownum
        FROM DUAL;

      SELECT o_surname
        INTO l_return
        FROM (SELECT O_SURNAME
                FROM REF_HOLDER_NAME_SURNAMES
               WHERE PK = l_rownum);

      RETURN l_return;
   END;


   PROCEDURE afn_holder_address (p_holder_address_id NUMBER)
   IS
      l_existing_country_code     VARCHAR2 (3);
      l_max_pk                    NUMBER;
      l_random_pk                 NUMBER;
      l_address_line1             VARCHAR2 (35 CHAR);
      l_address_line2             VARCHAR2 (35 CHAR);
      l_address_line3             VARCHAR2 (35 CHAR);
      l_address_line4             VARCHAR2 (35 CHAR);
      l_address_line5             VARCHAR2 (35 CHAR);
      l_address_line6             VARCHAR2 (35 CHAR);
      l_postcode_left             VARCHAR2 (4 CHAR);
      l_postcode_right            VARCHAR2 (3 CHAR);
      l_concate_postcode          VARCHAR2 (8 CHAR);
      l_o_concate_postcode        VARCHAR2 (8 CHAR);
      l_country_code              VARCHAR2 (2 CHAR);
      l_irish_distribution_code   VARCHAR2 (10 CHAR);
      l_concat_address_string     VARCHAR2 (500);
      l_o_concat_address_string   VARCHAR2 (500);
   BEGIN
      --Ensure Address anonymisation is country aware:
      SELECT NVL (o_country_code, 'U')
        INTO l_existing_country_code
        FROM anon_holder_addresses_gtt
       WHERE holder_address_id = p_holder_address_id;

      /*
      select nvl(O_COUNTRY_CODE,'U')
      FROM ANON_HOLDER_ADDRESSES
      WHERE HOLDER_ADDRESS_ID = 34210; -- 'HK'
      */
      --Initialise the max PK value to constrain randomisation
      SELECT MAX (pk)
        INTO l_max_pk
        FROM anon_holder_addresses_gtt
       WHERE NVL (o_country_code, 'U') = NVL (l_existing_country_code, 'U');

      /*
      select max(PK)
      FROM ANON_HOLDER_ADDRESSES
      WHERE nvl(O_COUNTRY_CODE,'U') = nvl(null,'U'); -- 12
      */
      --Initialise variables using the current record as a baseline:
      SELECT o_address_line1,
             o_address_line2,
             o_address_line3,
             o_address_line4,
             o_address_line5,
             o_address_line6,
             o_postcode_left,
             o_postcode_right,
             o_country_code
        INTO l_address_line1,
             l_address_line2,
             l_address_line3,
             l_address_line4,
             l_address_line5,
             l_address_line6,
             l_postcode_left,
             l_postcode_right,
             l_country_code
        FROM anon_holder_addresses_gtt
       WHERE holder_address_id = p_holder_address_id;

      --Generate Original Concatenated Address String
      IF l_postcode_left IS NOT NULL AND l_postcode_right IS NOT NULL
      THEN
         l_concate_postcode := l_postcode_left || ' ' || l_postcode_right;
      END IF;

      -- Generate the Original Concatenated Address String
      IF l_address_line1 IS NOT NULL
      THEN
         l_concat_address_string := l_concat_address_string || l_address_line1;
      END IF;

      IF l_address_line2 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line2;
      END IF;

      IF l_address_line3 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line3;
      END IF;

      IF l_address_line4 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line4;
      END IF;

      IF l_address_line5 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line5;
      END IF;

      IF l_address_line6 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line6;
      END IF;

      IF l_concate_postcode IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_concate_postcode;
      END IF;

      IF l_irish_distribution_code IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_irish_distribution_code;
      END IF;

      IF l_country_code IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_country_code;
      END IF;

      l_o_concat_address_string := l_concat_address_string;
      l_o_concate_postcode := l_concate_postcode;
      --Reinitialise the Concatenated Address Fields
      l_concat_address_string := NULL;
      l_concate_postcode := NULL;


      --For each variable in the Object anonymise the data fields
      --randomise_digit
      --dbms_output.put_line(l_ADDRESS_LINE1);
      IF l_address_line1 IS NOT NULL
      THEN
         l_random_pk :=
            NVL (ROUND (DBMS_RANDOM.VALUE (1, l_max_pk)), l_max_pk);

         SELECT afn_randomise_digit (o_address_line1)
           INTO l_address_line1
           FROM anon_holder_addresses_gtt
          WHERE     NVL (o_country_code, 'U') = l_existing_country_code
                AND pk = l_random_pk
                AND ROWNUM = 1;
      END IF;

      IF l_address_line2 IS NOT NULL
      THEN
         l_random_pk :=
            NVL (ROUND (DBMS_RANDOM.VALUE (1, l_max_pk)), l_max_pk);

         SELECT afn_randomise_digit (o_address_line2)
           INTO l_address_line2
           FROM anon_holder_addresses_gtt
          WHERE     NVL (o_country_code, 'U') = l_existing_country_code
                AND pk = l_random_pk
                AND ROWNUM = 1;
      END IF;

      IF l_address_line3 IS NOT NULL
      THEN
         l_random_pk :=
            NVL (ROUND (DBMS_RANDOM.VALUE (1, l_max_pk)), l_max_pk);

         SELECT afn_randomise_digit (o_address_line3)
           INTO l_address_line3
           FROM anon_holder_addresses_gtt
          WHERE     NVL (o_country_code, 'U') = l_existing_country_code
                AND pk = l_random_pk
                AND ROWNUM = 1;
      END IF;

      IF l_address_line4 IS NOT NULL
      THEN
         l_random_pk :=
            NVL (ROUND (DBMS_RANDOM.VALUE (1, l_max_pk)), l_max_pk);

         SELECT afn_randomise_digit (o_address_line4)
           INTO l_address_line4
           FROM anon_holder_addresses_gtt
          WHERE     NVL (o_country_code, 'U') = l_existing_country_code
                AND pk = l_random_pk
                AND ROWNUM = 1;
      END IF;

      IF l_address_line5 IS NOT NULL
      THEN
         l_random_pk :=
            NVL (ROUND (DBMS_RANDOM.VALUE (1, l_max_pk)), l_max_pk);

         SELECT afn_randomise_digit (o_address_line5)
           INTO l_address_line5
           FROM anon_holder_addresses_gtt
          WHERE     NVL (o_country_code, 'U') = l_existing_country_code
                AND pk = l_random_pk
                AND ROWNUM = 1;
      END IF;

      IF l_address_line6 IS NOT NULL
      THEN
         l_random_pk :=
            NVL (ROUND (DBMS_RANDOM.VALUE (1, l_max_pk)), l_max_pk);

         SELECT afn_randomise_digit (o_address_line6)
           INTO l_address_line6
           FROM anon_holder_addresses_gtt
          WHERE     NVL (o_country_code, 'U') = l_existing_country_code
                AND pk = l_random_pk
                AND ROWNUM = 1;
      END IF;

      --
      l_postcode_left := afn_randomise_digit (l_postcode_left);
      l_postcode_right := afn_randomise_digit (l_postcode_right);

      IF l_postcode_left IS NOT NULL AND l_postcode_right IS NOT NULL
      THEN
         l_concate_postcode := l_postcode_left || ' ' || l_postcode_right;
      END IF;

      --
      -- l_COUNTRY_CODE is unchanged in this release (so addresses are anonymised within Country)
      --
      IF l_irish_distribution_code IS NOT NULL
      THEN
         l_irish_distribution_code :=
            afn_randomise_digit (l_irish_distribution_code);
      END IF;

      --
      -- Generate the Anonymised Concatenated Address String
      IF l_address_line1 IS NOT NULL
      THEN
         l_concat_address_string := l_concat_address_string || l_address_line1;
      END IF;

      IF l_address_line2 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line2;
      END IF;

      IF l_address_line3 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line3;
      END IF;

      IF l_address_line4 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line4;
      END IF;

      IF l_address_line5 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line5;
      END IF;

      IF l_address_line6 IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_address_line6;
      END IF;

      IF l_concate_postcode IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_concate_postcode;
      END IF;

      IF l_irish_distribution_code IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_irish_distribution_code;
      END IF;

      IF l_country_code IS NOT NULL
      THEN
         l_concat_address_string :=
            l_concat_address_string || ',' || l_country_code;
      END IF;


      /*
      SELECT HOLDER_ADDRESS(
      --l_ADDRESS_LINE1,
      'HELLO', l_ADDRESS_LINE2, l_ADDRESS_LINE3, l_ADDRESS_LINE4, l_ADDRESS_LINE5, l_ADDRESS_LINE6, l_POSTCODE_LEFT, l_POSTCODE_RIGHT, l_CONCATE_POSTCODE, l_COUNTRY_CODE, l_IRISH_DISTRIBUTION_CODE, l_CONCAT_ADDRESS_STRING )
      INTO return_value
      FROM DUAL;
      */
      --
      UPDATE anon_holder_addresses_gtt
         SET a_address_line1 = l_address_line1,
             a_address_line2 = l_address_line2,
             a_address_line3 = l_address_line3,
             a_address_line4 = l_address_line4,
             a_address_line5 = l_address_line5,
             a_address_line6 = l_address_line6,
             a_postcode_left = l_postcode_left,
             a_postcode_right = l_postcode_right,
             a_concate_postcode = l_concate_postcode,
             a_irish_distribution_code = l_irish_distribution_code,
             a_concat_address_string = l_concat_address_string,
             o_concat_address_string = l_o_concat_address_string,
             o_concate_postcode = l_o_concate_postcode,
             a_country_code = l_country_code
       WHERE holder_address_id = p_holder_address_id;
   --Debugging Statements
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line ('Error Processing:');
         DBMS_OUTPUT.put_line ('Holder_Address_Id:' || p_holder_address_id);
         DBMS_OUTPUT.put_line (l_address_line1);
         DBMS_OUTPUT.put_line (l_address_line2);
         DBMS_OUTPUT.put_line (l_address_line3);
         DBMS_OUTPUT.put_line (l_address_line4);
         DBMS_OUTPUT.put_line (l_address_line5);
         DBMS_OUTPUT.put_line (l_address_line6);
         DBMS_OUTPUT.put_line (l_postcode_right);
         DBMS_OUTPUT.put_line (l_concate_postcode);
         DBMS_OUTPUT.put_line (l_irish_distribution_code);
         DBMS_OUTPUT.put_line (l_concat_address_string);
   END;

   PROCEDURE afn_national_insurance
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

      COMMIT;
   END;



   PROCEDURE run_afn_holder_address
   IS
      CURSOR lc_address
      IS
         SELECT holder_address_id
           FROM anon_holder_addresses
          WHERE a_address_line1 IS NULL;
   BEGIN
      FOR i IN lc_address
      LOOP
         afn_holder_address (i.holder_address_id);
      END LOOP;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;
END;
/