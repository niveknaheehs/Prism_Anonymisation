/* Formatted on 29/09/2016 19:40:29 (QP5 v5.185.11230.41888) */
SET SERVEROUTPUT ON;

--Check anonymisation schemas match BAU development constraints.
select * 
from all_constraints
where owner like 'BAU%'
and owner not like '%ACTIVE_BATCH%'
and constraint_name not like 'SYS%'
and constraint_name not in 
(select constraint_name
from all_constraints
where owner like 'FOROBS2%');

DECLARE
   l_tabname   VARCHAR (200);
   l_count     NUMBER;

   CURSOR lc_cursor
   IS
      SELECT    'select 
'''
             || owner
             || '-'
             || table_name
             || ''' as table_name,
count(*) from 
'
             || owner
             || '.'
             || table_name
             || '
where mod_timestamp < sysdate-1'
                AS l_stmt
        FROM all_tab_columns
       WHERE owner LIKE 'FOROBS2%' AND column_name LIKE 'MOD_TIMESTAMP'
       and table_name in (select table_name from forobs2_anonymise.privacy_catalog)
       ;
BEGIN
   FOR i IN lc_cursor
   LOOP
      EXECUTE IMMEDIATE (i.l_stmt) INTO l_tabname, l_count;

      IF l_count > 0
      THEN
         DBMS_OUTPUT.put_line ('table ' || l_tabname || ' count:' || l_count);
      END IF;
   END LOOP;
END;
/


DECLARE
   l_tabname   VARCHAR (200);
   l_count     NUMBER;

   CURSOR lc_cursor
   IS
      SELECT    'select 
'''
             || owner
             || '-'
             || table_name
             || ''' as table_name,
count(*) from 
'
             || owner
             || '.'
             || table_name
             || '
where mod_timestamp < sysdate-1'
                AS l_stmt
        FROM all_tab_columns
       WHERE owner LIKE 'FOROBS2%' AND table_name like 'A_\%' escape '\'
       ;
BEGIN
   FOR i IN lc_cursor
   LOOP
      EXECUTE IMMEDIATE (i.l_stmt) INTO l_tabname, l_count;

      IF l_count > 0
      THEN
         DBMS_OUTPUT.put_line ('table ' || l_tabname || ' count:' || l_count);
      END IF;
   END LOOP;
END;
/



set serveroutput on size unlimited;

DECLARE
   l_tabname   VARCHAR (200);
   l_count     NUMBER;

   CURSOR lc_cursor
   IS
      SELECT    'select 
'''
             || owner
             || '-'
             || table_name
             || ''' as table_name,
count(*) from 
'
             || owner
             || '.'
             || table_name
             || '
where mod_timestamp < sysdate-1'
                AS l_stmt
        FROM all_tab_columns
       WHERE owner LIKE 'FOROBS2%' AND column_name LIKE 'MOD_TIMESTAMP'
       and table_name not in (select table_name from forobs2_anonymise.privacy_catalog)
       and table_name not like '%TYPES'
       and owner not like '%TTPARM%'
       ;
BEGIN
   FOR i IN lc_cursor
   LOOP
      
      begin
      EXECUTE IMMEDIATE (i.l_stmt) INTO l_tabname, l_count;
        exception when others then
        dbms_output.put_line('Error Processing:'||i.l_stmt);
end;

      IF l_count > 0
      THEN
         DBMS_OUTPUT.put_line ('table ' || l_tabname || ' count:' || l_count);
      END IF;
   END LOOP;
END;
/
