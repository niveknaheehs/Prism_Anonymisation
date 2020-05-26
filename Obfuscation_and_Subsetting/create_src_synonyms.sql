REM creating src SYNONYMS
SET DEFINE ON;
set serveroutput on;

declare
   cursor c_src_synonyms
   is  
		select 'create or replace synonym src_'||table_name||' for &&PRISM_PREFIX._PRISM_UTILITIES.'||table_name||'@SRC_LINK' stmt
		  from user_tables@SRC_LINK
		 where table_name like 'DD\_%' escape '\'
			or table_name like 'MD\_%' escape '\'
			or table_name like 'SS\_%' escape '\'
		union
		select 'create or replace synonym src_'||view_name||' for &&PRISM_PREFIX._PRISM_UTILITIES.'||view_name||'@SRC_LINK' stmt
		  from user_views@SRC_LINK
		 where view_name like 'VW\_%' escape '\';
	  
begin
   for r in c_src_synonyms
   loop
      dbms_output.put_line('Creating SYNONYM '||r.stmt);  
      execute immediate r.stmt;
   end loop;
end;
/	  