/* Formatted on 11/04/2016 14:29:59 (QP5 v5.185.11230.41888) */
CREATE OR REPLACE PROCEDURE export_database (
   pi_prefix      IN VARCHAR2,
   pi_compcodes   IN VARCHAR2,
   pi_debug       IN VARCHAR2 DEFAULT 'N')
   AUTHID CURRENT_USER
AS
   -- VERSION 13.0
   --Parameters IN
   -- pi_prefix - This is a PRISM Schema prefix e.g 'PSM'
   -- pi_compcodes - This is a comma separated list of company codes e.g. 'R030,SL01,C532'

   --This script is an extension of the Oracle documented script @
   --http://docs.oracle.com/cd/E11882_01/server.112/e22490/dp_api.htm#SUTIL977
   --Specifically the extensions:
   --Identifies schema to extract based on  prefix;
   --Generates a filename

   --NOTE: you must GRANT CREATE ANY TABLE on the schema owning this procedure.

   ind            NUMBER;                                        -- Loop index
   h1             NUMBER;                              -- Data Pump job handle
   percent_done   NUMBER;                        -- Percentage of job complete
   job_state      VARCHAR2 (30);                 -- To keep track of job state
   le             ku$_LogEntry;                  -- For WIP and error messages
   js             ku$_JobStatus;             -- The job status from get_status
   jd             ku$_JobDesc;          -- The job description from get_status
   sts            ku$_Status;      -- The status object returned by get_status

   --This cursor will transform the comma separated pi_compcodes list of companies into a recordset
   CURSOR lc_comp_codes (cpi_comp_codes VARCHAR2)
   IS
          SELECT REGEXP_SUBSTR (cpi_comp_codes,
                                '[^,]+',
                                1,
                                LEVEL)
                    AS comp_code
            FROM DUAL
      CONNECT BY REGEXP_SUBSTR (cpi_comp_codes,
                                '[^,]+',
                                1,
                                LEVEL)
                    IS NOT NULL;

   CURSOR lc_users
   IS
      SELECT username, ROWNUM
        FROM sys.all_users
       WHERE username LIKE pi_prefix || '%';

   --Note: All Company partitioned tables have a NULL_COMP_CODE partition, and
   -- so this can be used to identify the Company Partitioned tables.
   --Excluding schemas which are identified in the Exclusion list.
   CURSOR lc_comp_partitions
   IS
      SELECT DISTINCT table_owner, table_name
        FROM all_tab_partitions
       WHERE     table_owner LIKE pi_prefix || '%'
             AND table_owner NOT LIKE '%AUDIT'
             AND table_owner NOT LIKE '%INTEGRATION%'
             AND table_owner NOT LIKE '%CREST%'
             AND table_owner NOT LIKE '%CHECKING%'
             AND table_owner NOT LIKE '%PRISM_CAG_PARAMS%'
             AND table_owner NOT LIKE '%PRISM_CAG_DATA%'
             AND partition_name LIKE 'NULL_COMP_CODE';

   --Excluded tables. This cursor identifies tables whose data is to be explicitly excluded
   --Note: For anonymisation the exclusions below are acceptable. They are probably not acceptable for BAU support.
   CURSOR lc_anonymise_exclude_tabledata
   IS
      --Exclude Specific Schemas
      SELECT DISTINCT table_owner, table_name
        FROM all_tab_partitions
       WHERE    table_owner LIKE pi_prefix || '%INTEGRATION%'
             OR table_owner LIKE pi_prefix || '%CREST%'
             OR table_owner LIKE pi_prefix || '%CHECKING%'
             OR table_owner LIKE pi_prefix || '%PRISM_CAG_PARAMS%'
             OR table_owner LIKE pi_prefix || '%PRISM_CAG_DATA%'
      UNION
      --Exclude all Partitioned Audit tables
      SELECT DISTINCT table_owner, table_name
        FROM all_tab_partitions
       WHERE     table_owner LIKE pi_prefix || '_AUDIT'
             AND partition_name LIKE 'NULL_COMP_CODE'
      UNION
      --Exclude Tables large volume tables
      SELECT DISTINCT owner as table_owner, table_name
        FROM all_tables
       WHERE     owner LIKE pi_prefix || '%'
             AND table_name IN
                    ('OUTPUTFILES',
                     'AUDIT_EVENT_SUBEVENTS',
                     'LOGS',
                     'RPTINSTANCES',
                     'ERROR_LOGS',
                     'JOBS',
                     'IMPORT_STAGING_TABLE',
                     'JOB_DATA');


   --Note: All Event partitioned tables have a NULL_EVENT_CODE partition, and
   -- so this can be used to identify the Event Partitioned tables.
   CURSOR lc_event_partitions
   IS
      SELECT DISTINCT table_owner, table_name
        FROM all_tab_partitions
       WHERE     table_owner LIKE pi_prefix || '%'
             AND partition_name LIKE 'NULL_EVENT_CODE';


   l_schemas      VARCHAR2 (4000);
   l_partitions   VARCHAR2 (4000);
   l_filename     VARCHAR2 (255);
   l_dbversion    VARCHAR2 (20);
   l_count        NUMBER;
   l_stmt         VARCHAR2 (4000);
BEGIN
   --Obtain a list of schemas, and assign to the l_schemas variable
   FOR i IN lc_users
   LOOP
      IF (i.ROWNUM = 1)
      THEN
         l_schemas := '''' || i.username || '''';
      ELSE
         l_schemas := l_schemas || ',''' || i.username || '''';
      END IF;
   END LOOP;

   l_schemas := 'IN (' || l_schemas || ')';


   DBMS_OUTPUT.put_line ('#######SCHEMAS#########');
   DBMS_OUTPUT.put_line (l_schemas);

   --Generate a list of partitions to export (always including the NULL_COMP_CODE partition)
   FOR i IN lc_comp_codes (pi_compcodes)
   LOOP
      l_partitions := l_partitions || ',' || 'P_' || UPPER (i.comp_code);
   END LOOP;

   l_partitions := 'NULL_COMP_CODE' || l_partitions;

   DBMS_OUTPUT.put_line ('#######PARTITIONS#########');
   DBMS_OUTPUT.put_line (l_partitions);

   --Assign l_dbversion
   --Note this needs to be extended to call into the prism_utilties.what_db_release() function
   --l_dbversion := what_db_release();

   BEGIN
      l_stmt :=
            'select replace(db_version,''_'','''') from '
         || pi_prefix
         || '_PRISM_CORE.PRISM_DATA_BASE_VERSIONS
where created_date = (select Max(created_date) from '
         || pi_prefix
         || '_PRISM_CORE.PRISM_DATA_BASE_VERSIONS)';

      DBMS_OUTPUT.put_line ('#######VERSION_STMT#########');
      DBMS_OUTPUT.put_line (l_stmt);

      EXECUTE IMMEDIATE l_stmt INTO l_dbversion;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_dbversion := 'staticversion';
   END;

   --Generate filename variable
   l_filename :=
         TO_CHAR (SYSDATE, 'yyyymmdd')
      || '_'
      || pi_prefix
      || '_'
      || l_dbversion
      || '.dmp';


   -- Create a (user-named) Data Pump job to do a schema export.
   h1 :=
      DBMS_DATAPUMP.OPEN ('EXPORT',
                          'SCHEMA',
                          NULL,
                          'PRISM_DBA',
                          'LATEST');

   -- Specify a single dump file for the job (using the handle just returned)
   -- and a directory object, which must already be defined and accessible
   -- to the user running this procedure.
   -- Also add a log file.
   --      DBMS_DATAPUMP.ADD_FILE (h1, l_filename, 'DATA_PUMP_DIR');
   DBMS_DATAPUMP.add_file (
      handle      => h1,
      filename    => l_filename,
      reusefile   => 1,
      directory   => 'DATA_PUMP_DIR',
      filetype    => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE);

   DBMS_DATAPUMP.add_file (
      handle      => h1,
      filename    => l_filename || '.log',
      directory   => 'DATA_PUMP_DIR',
      filetype    => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);



   IF pi_debug = 'Y'
   THEN
      DBMS_DATAPUMP.set_debug (
         debug_flags    => TO_NUMBER ('1FF0300', 'XXXXXXXXXXXXX'),
         version_flag   => 1);

      DBMS_DATAPUMP.METADATA_FILTER (handle   => h1,
                                     name     => 'METRICS',
                                     VALUE    => 1);
   END IF;


   -- A metadata filter is used to specify the schema that will be exported.
   DBMS_DATAPUMP.METADATA_FILTER (h1, 'SCHEMA_EXPR', l_schemas);

   --Always Exclude Statistics from the Export
   DBMS_DATAPUMP.METADATA_FILTER (
      h1,
      'EXCLUDE_PATH_EXPR',
      'like''%/TABLE/INDEX/STATISTICS/INDEX_STATISTICS''');
   DBMS_DATAPUMP.METADATA_FILTER (
      h1,
      'EXCLUDE_PATH_EXPR',
      'like''%/TABLE/STATISTICS/TABLE_STATISTICS''');


   IF pi_debug = 'Y'
   THEN
      DBMS_OUTPUT.put_line ('#######EXPORTING METADATA ONLY#########');

      DBMS_DATAPUMP.DATA_FILTER (handle   => h1,
                                 name     => 'INCLUDE_ROWS',
                                 VALUE    => 0);               --METADATA_ONLY
   --Extract CONTENT=DATA_ONLY
   --DBMS_DATAPUMP.SET_PARAMETER (h1, 'INCLUDE_METADATA', 0);

   ELSE
      DBMS_OUTPUT.put_line ('#######EXPORTING TABLE PARTITIONS#########');
      l_count := 0;

      --Add Data filters to only consider certain company partitions
      FOR i IN lc_comp_partitions
      LOOP
         DBMS_DATAPUMP.data_filter (handle        => h1,
                                    name          => 'PARTITION_LIST',
                                    VALUE         => l_partitions,
                                    table_name    => i.table_name,
                                    schema_name   => i.table_owner);
         /*
             dbms_datapump.data_filter
             (
                 handle => h1,
                 name => 'PARTITION_EXPR',
                 value => q'[= 'Q22014']',
                 table_name => r_part_tabs.table_name
             );


         */
         l_count := l_count + 1;
      END LOOP;

      DBMS_OUTPUT.put_line (
            '#######IDENTIFIED '
         || l_count
         || ' COMPANY PARTITIONS TO EXPORT #########');


      l_count := 0;
      DBMS_OUTPUT.put_line ('#######EXCLUDING ANONYMISATION DATA#########');

      --Add Data filters to only consider certain Audit partitions
      FOR i IN lc_anonymise_exclude_tabledata
      LOOP
         DBMS_DATAPUMP.data_filter (handle        => h1,
                                    name          => 'INCLUDE_ROWS',
                                    VALUE         => 0,
                                    table_name    => i.table_name,
                                    schema_name   => i.table_owner);
         /*
             dbms_datapump.data_filter
             (
                 handle => h1,
                 name => 'PARTITION_EXPR',
                 value => q'[= 'Q22014']',
                 table_name => r_part_tabs.table_name
             );


         */
         l_count := l_count + 1;
      END LOOP;

      DBMS_OUTPUT.put_line (
            '#######IDENTIFIED '
         || l_count
         || ' PARTITIONED TABLES FOR ANONYMISATION EXCLUSION#########');
   --
   END IF;


   -- Start the job. An exception will be generated if something is not set up
   -- properly.
   DBMS_DATAPUMP.START_JOB (h1);

   -- The export job should now be running. In the following loop, the job
   -- is monitored until it completes. In the meantime, progress information is
   -- displayed.
   percent_done := 0;
   job_state := 'UNDEFINED';

   WHILE (job_state != 'COMPLETED') AND (job_state != 'STOPPED')
   LOOP
      DBMS_DATAPUMP.get_status (
         h1,
           DBMS_DATAPUMP.ku$_status_job_error
         + DBMS_DATAPUMP.ku$_status_job_status
         + DBMS_DATAPUMP.ku$_status_wip,
         -1,
         job_state,
         sts);

      js := sts.job_status;

      -- If the percentage done changed, display the new value.
      IF js.percent_done != percent_done
      THEN
         DBMS_OUTPUT.put_line (
            '*** Job percent done = ' || TO_CHAR (js.percent_done));

         percent_done := js.percent_done;
      END IF;



      -- If any work-in-progress (WIP) or error messages were received for the job,

      -- display them.



      IF (BITAND (sts.mask, DBMS_DATAPUMP.ku$_status_wip) != 0)
      THEN
         le := sts.wip;
      ELSE
         IF (BITAND (sts.mask, DBMS_DATAPUMP.ku$_status_job_error) != 0)
         THEN
            le := sts.error;
         ELSE
            le := NULL;
         END IF;
      END IF;



      IF le IS NOT NULL
      THEN
         ind := le.FIRST;



         WHILE ind IS NOT NULL
         LOOP
            DBMS_OUTPUT.put_line (le (ind).LogText);

            ind := le.NEXT (ind);
         END LOOP;
      END IF;
   END LOOP;



   -- Indicate that the job finished and detach from it.

   DBMS_OUTPUT.put_line ('Job has completed');

   DBMS_OUTPUT.put_line ('Final job state = ' || job_state);

   DBMS_DATAPUMP.detach (h1);
/*
--#############  ADDITIONAL DOCUMENTATION FOR DBMS_DATAPUMP ##############-----

CLUSTER
dbms_datapump.start_job(xxx,xxx,xxx,cluster_ok=>0,xxx,xx); = 'N'
dbms_datapump.start_job(xxx,xxx,xxx,cluster_ok=>1,xxx,xx); = 'Y'

COMPRESSION
dbms_datapump.set_parameter(handle => l_dp_handle, name => 'COMPRESSION', value => 'ALL');
dbms_datapump.set_parameter(handle => l_dp_handle, name => 'COMPRESSION', value => 'DATA_ONLY');
dbms_datapump.set_parameter(handle => l_dp_handle, name => 'COMPRESSION', value => 'METADATA_ONLY');

CONTENT
dbms_datapump.set_parameter(handle => h1, name => 'INCLUDE_METADATA', value => 1); ALL
dbms_datapump.set_parameter(handle => h1, name => 'INCLUDE_METADATA', value => 0); DATA_ONLY
DBMS_DATAPUMP.DATA_FILTER(handle => h1, name => 'INCLUDE_ROWS', value => 0); METADATA_ONLY

DATA_OPTIONS
dbms_datapump.set_parameter(handle => l_dp_handle, name => 'DATA_OPTIONS', value => DBMS_DATAPUMP.KU$_DATAOPT_XMLTYPE_CLOB);

ENCRYPTION
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION', value => 'ALL');
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION', value => 'DATA_ONLY');
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION', value => 'ENCRYPTED_COLUMNS_ONLY');
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION', value => 'METADATA_ONLY');
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION', value => 'NONE');

ENCRYPTION_ALGORITHM
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION_ALGORITHM', value => 'AES128');
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION_ALGORITHM', value => 'AES192');
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION_ALGORITHM', value => 'AES256');

ENCRYPTION_MODE
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION_MODE', value => 'DUAL');
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION_MODE', value => 'PASSWORD');
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION_MODE', value => 'TRANSPARENT');

ENCRYPTION_PASSWORD
dbms_datapump.set_parameter(handle => h1, name => 'ENCRYPTION_PASSWORD', value => 'secret password');

ESTIMATE
dbms_datapump.set_parameter(handle => h1, name => 'ESTIMATE', value => 'BLOCKS');

ESTIMATE_ONLY
dbms_datapump.set_parameter(handle => h1, name => 'ESTIMATE_ONLY', value => 1);

FLASHBACK_SCN
dbms_datapump.set_parameter(handle => h1, name => 'FLASHBACK_SCN', value => nnnnnnnn);
dbms_datapump.set_parameter(handle => h1, name => 'FLASHBACK_SCN', value => TIMESTAMP_TO_SCN( TO_TIMESTAMP( TO_CHAR( SYSDATE) )));

FLASHBACK_TIME
dbms_datapump.set_parameter(handle => h1, name => 'FLASHBACK_TIME', value =>  'TO_TIMESTAMP( TO_CHAR( SYSDATE) )');

PARALLEL
DBMS_DATAPUMP.set_parallel(handle=>h1,degree=n);

QUERY
dbms_datapump.data_filter(handle => h1, name => 'SUBQUERY', value => 'WHERE col1 = 1', table_name => null, schema_name => null);

REMAP_DATA
dbms_datapump.data_remap(handle => h1, name => 'COLUMN_FUNCTION', table_name => 'TAB1', column => 'COL1', function => 'PUMPY.TESTPACKAGE.SETTESTID', schema => 'PUMPY');

SOURCE_EDITION
dbms_datapump.set_parameter(handle => h1, name => 'SOURCE_EDITION', value => 'edition name');

ACCESS_METHOD
dbms_datapump.set_parameter(handle => h1, name => 'DATA_ACCESS_METHOD', value => 'AUTOMATIC');
dbms_datapump.set_parameter(handle => h1, name => 'DATA_ACCESS_METHOD', value => 'DIRECT_PATH');
dbms_datapump.set_parameter(handle => h1, name => 'DATA_ACCESS_METHOD', value => 'EXTERNAL_TABLE');

METRICS

DBMS_DATAPUMP.METADATA_FILTER(
HANDLE => HANDLR,
NAME => 'METRICS',
VALUE => 0
);

DBMS_DATAPUMP.METADATA_FILTER(
HANDLE => HANDLR,
NAME => 'METRICS',
VALUE => 1
);


TRACE
dbms_datapump.set_debug(debug_flags=>to_number('1FF0300','XXXXXXXXXXXXX'),version_flag=>1);

KEEP_MASTER
  dbms_datapump.set_parameter(handle => l_dp_handle, name => 'KEEP_MASTER', value => 1);
  dbms_datapump.set_parameter(handle => l_dp_handle, name => 'KEEP_MASTER', value => 0);
*/



END;