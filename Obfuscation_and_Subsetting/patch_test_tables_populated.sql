whenever SQLERROR exit sql.sqlcode
whenever OSERROR  exit sql.sqlcode

--sqlplus cpenny/PT4_reader@PT4
set serveroutput on
set timing on
set feedback on
var v_tabs_pop   varchar2(32767);
var v_db_version varchar2(11);

declare
  cursor c_tab_pop
  is
    select owner, table_name 
      from all_tables 
     where owner like'PSM%' 
       and owner not like'%AUDIT';
     
  v_sql varchar2(4000); 
  v_pop_cnt number := 0;
  v_cnt number := 0; 

begin

  select x.db_version
    into :v_db_version
    from ( select db_version
             from PSM_PRISM_CORE.prism_data_base_versions
           order by created_date desc ) x
   where rownum = 1;	

  for r in c_tab_pop
  loop
     v_sql := 'select count(*) from ' || r.owner ||'.'||r.table_name;
     execute immediate v_sql into v_cnt;
     if v_cnt > 0 then
       v_pop_cnt := v_pop_cnt + 1;
       :v_tabs_pop := :v_tabs_pop || r.owner ||'.'||r.table_name||'.'||to_char(v_cnt)||';';
     end if;
     
  end loop;
  dbms_output.put_line('v_pop_cnt: ' || v_pop_cnt); 
  dbms_output.put_line('length(:v_tabs_pop): ' || length(:v_tabs_pop));  
  
end;
/
--ACCEPT OS_INSTANCE CHAR PROMPT 'Type INSTANCE name for Obfuscation and Subsetting install (e.g. MIGDEV):  ' HIDE 
--INSTANCE name for Obfuscation and Subsetting install (e.g. MIGDEV)' 

conn os1/os1@&&OS_INSTANCE
set serveroutput on
set timing on
set feedback on

declare
   t_strings     subsetting_control.string_list_4000; 
   i             integer := 1;
   v_32K_string  varchar2(32767); 
begin  
   v_32K_string := ';' || :v_tabs_pop;

   loop      
      t_strings(i) := substr( v_32K_string, instr(v_32K_string,';',1,i)+1, instr(v_32K_string,';',1,i+1)-instr(v_32K_string,';',1,i)-1 );    
      i := i+1;
      exit when instr(v_32K_string,';',1,i+1) = 0;        
   end loop;
      
   dbms_output.put_line('t_strings.count: ' || t_strings.count);
   
   delete SS_PT_TABS_POPULATED;
   dbms_output.put_line(to_char(sql%rowcount) || ' rows deleted from SS_PT_TABS_POPULATED'); 
   
   forall i in 1..t_strings.count
   insert into SS_PT_TABS_POPULATED (actual_owner, owner, table_name, row_count)
   values (substr(t_strings(i),1,instr(t_strings(i),'.',1,1)-1),
           substr(t_strings(i),5,instr(t_strings(i),'.',1,1)-5),
           substr(t_strings(i),instr(t_strings(i),'.',1,1)+1,instr(t_strings(i),'.',1,2)-instr(t_strings(i),'.',1,1)-1),
           substr(t_strings(i),instr(t_strings(i),'.',1,2)+1));
   
   dbms_output.put_line(to_char(sql%rowcount) || ' rows inserted into SS_PT_TABS_POPULATED'); 
		   
    update SS_PT_TABS_POPULATED
       set db_version = :v_db_version;
	   
    dbms_output.put_line('Updated SS_PT_TABS_POPULATED with db_version = '||:v_db_version); 
	   
end;
/
commit;
--select * from SS_PT_TABS_POPULATED order by row_count desc;
exit sql.sqlcode;