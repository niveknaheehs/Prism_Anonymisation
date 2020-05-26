--------------------------------------------------------
--  File created - Tuesday-July-17-2018   
--------------------------------------------------------
set define off;


--------------------------------------------------------
--  DDL for Function EXCLUDE
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION EXCLUDE (astring varchar2)  return varchar2 as

begin
  return astring;
end;

/
--------------------------------------------------------
--  DDL for Function GET_FORMATTED_SK
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION GET_FORMATTED_SK (pi_name in varchar2,pi_holder_type varchar2,pi_holder_designation varchar2)
    return varchar2 is
   l_count number;
   l_sortkey varchar2(4000);
   l_name varchar2(4000);
   l_holder_designation varchar2(4000);
   
BEGIN 

  l_name := pi_name;

  if pi_holder_type IN ('C','T') then 
    l_name := regexp_replace(l_name,'[.(),;]',' ');
  end if;  

  if pi_holder_type IN ('C', 'I','T') then 
    l_name := regexp_replace(l_name,'['',./#!$%^&*;:{}=_`~()<>?@|-]');
    l_name := regexp_replace(l_name, '[ÀÁÂÃÄÅÆ]','A');
    l_name := regexp_replace(l_name, '[Ç]','C');
    l_name := regexp_replace(l_name, '[ÈÉÊË]','E');
    l_name := regexp_replace(l_name, '[Ñ]','N');
    l_name := regexp_replace(l_name, '[ÒÓÔÕÖŒØ]','O'); 
    l_name := regexp_replace(l_name, '[ß]','S');
    l_name := regexp_replace(l_name, '[ÙÚÛÜ]','U'); 
    l_name := regexp_replace(l_name, '[Ÿ]','Y');
    l_name := regexp_replace(l_name, '[Í]','I');
  end if;
  
  l_count := regexp_count(pi_name,'[^ ]+');

  for i in 1 .. l_count loop 
    if i > 12 then
      exit;
    end if;
    l_sortkey := l_sortkey ||
      CASE 
            WHEN i=1 THEN SUBSTR(RPAD(regexp_substr(l_name,'[^ ]+',1,i),10,'*'),1,10) 
            WHEN i=2 THEN SUBSTR(RPAD(regexp_substr(l_name,'[^ ]+',1,i),4,'*'),1,4)
            WHEN i=3 THEN SUBSTR(RPAD(regexp_substr(l_name,'[^ ]+',1,i),3,'*'),1,3)
            ELSE  SUBSTR(regexp_substr(l_name,'[^ ]+',1,i),1,1) END;
          
  end loop;
  
  l_sortkey := substr(RPAD(l_sortkey,34,'*'),1,34);
  
  if pi_holder_designation is not null then
    l_holder_designation := regexp_replace(pi_holder_designation, '[[:punct:]]', '');
    l_holder_designation := regexp_replace(l_holder_designation , '[[:space:]]', '');
    l_sortkey := substr(l_sortkey,1,26) || lpad(l_holder_designation, 8, '*');
  end if;
  
  if l_sortkey is null then 
    l_sortkey := '**********************************';
  end if;
  
  return l_sortkey;
END;

/
--------------------------------------------------------
--  DDL for Function MAN
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION MAN (astring varchar2)  return varchar2 as

begin
  return astring;
end;

/
--------------------------------------------------------
--  DDL for Function TDC
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION TDC (astring varchar2) return varchar2 as
  nPointer NUMBER := 1;
  nPrevPointer NUMBER := 0;
  ret_str varchar2(4000);
begin

  nPointer  := 1;
  nPrevPointer  := 0;
  
  if astring is null then
    return null;
  end if;

  loop
  
    nPointer:= REGEXP_INSTR(astring,'([[:digit:]])',nPointer);
    
    if nPointer = 0 then
      ret_str := ret_str||substr(astring,nPrevPointer+1);
      exit;
    end if;
    if to_number(substr(astring,nPointer,1)) > 9 or to_number(substr(astring,nPointer,1)) < 0 then
      dbms_output.put_line(substr(astring,nPointer,1));
    end if;
    ret_str := ret_str ||substr(astring,nPrevPointer+1,nPointer-nPrevPointer-1)||to_char(rnd_digit_wo_resel(to_number(substr(astring,nPointer,1))));
    nPrevPointer := nPointer;
    nPointer := nPointer+1;
    
  end loop;
  return REGEXP_REPLACE (ret_str, '([[:alpha:]])', DBMS_RANDOM.string ('U', 1));
end;
/

--------------------------------------------------------
--  DDL for Function TDC2
--------------------------------------------------------
CREATE OR REPLACE FUNCTION TDC2 (p_string varchar2) return varchar2 as
  nPointer NUMBER := 1;
  nPrevPointer NUMBER := 0;
  ret_str  varchar2(4000);
  ret_str2 varchar2(4000);
begin

  nPointer  := 1;
  nPrevPointer  := 0;
  
  if p_string is null then
     ret_str2 := null;
  else
    loop
    
      nPointer:= REGEXP_INSTR(p_string,'([[:digit:]])',nPointer);
      
      if nPointer = 0 then
        ret_str := ret_str||substr(p_string,nPrevPointer+1);
        exit;
      end if;
      
      ret_str := ret_str ||substr(p_string,nPrevPointer+1,nPointer-nPrevPointer-1)||to_char(rnd_digit_wo_resel(to_number(substr(p_string,nPointer,1))));
      
      nPrevPointer := nPointer;
      nPointer := nPointer+1;
      
    end loop;

    for i IN 1..length(ret_str)
    loop
       ret_str2 := ret_str2 || REGEXP_REPLACE(SUBSTR(ret_str,i,1),'([[:alpha:]])',DBMS_RANDOM.string ('U', 1));
    end loop;
  end if;  
  
  return ret_str2;
end TDC2;
/

-----------------------------------------------------------------------------------------------------
-- Function to return random NON-ALPHANUMERIC Printable Character, not the same as the one passed in
-----------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gen_rnd_non_alphanum ( p_not_char IN VARCHAR DEFAULT NULL )
   return varchar2 
AS 
   v_non_alphanum VARCHAR2(1);
BEGIN

   WHILE (   v_non_alphanum IS NULL 
          OR regexp_instr(v_non_alphanum,'[[:alnum:]]+') > 0
          OR v_non_alphanum = SUBSTR(p_not_char,1,1)
   )       
   LOOP
      v_non_alphanum := dbms_random.string('P',1);
   END LOOP;
   
   RETURN v_non_alphanum;
END gen_rnd_non_alphanum;
/

--------------------------------------------------------
--  DDL for Function TDC2_UNIQUE    
--------------------------------------------------------

CREATE OR REPLACE FUNCTION TDC2_UNIQUE (p_str_for_unique_obfus in varchar2) return varchar2 as
   v_unique_candidate_str varchar2(4000); 
   v_candidate_str_len      integer;
   v_tdc2_candidate_str_len integer;   
begin
   --DBMS_OUTPUT.PUT_LINE('input: ' || p_str_for_unique_obfus);
   v_candidate_str_len := LENGTH(p_str_for_unique_obfus);
   v_unique_candidate_str := NVL(p_str_for_unique_obfus,dbms_random.string('P',1));
  
   begin
       
      v_unique_candidate_str := TDC2(v_unique_candidate_str);
      v_tdc2_candidate_str_len := LENGTH(v_unique_candidate_str);

      IF v_tdc2_candidate_str_len <>  v_candidate_str_len THEN 
         obfuscation_control.obfus_log('Length of input string: ' || p_str_for_unique_obfus || ' not equal to length of output: ' || v_unique_candidate_str,null,null,null,NULL,NULL,'TDC2_UNIQUE');    
         --DBMS_OUTPUT.PUT_LINE('input: ' || p_str_for_unique_obfus);
         --DBMS_OUTPUT.PUT_LINE('insert: ' || v_unique_candidate_str);
      END IF;
      
      v_unique_candidate_str := NVL(v_unique_candidate_str,dbms_random.string('P',1));
      insert into tbl_TDC2_UNIQUE (UNIQUE_COLUMN) VALUES (v_unique_candidate_str);
   
   exception
      when dup_val_on_index then
         obfuscation_control.obfus_log('DUP_VAL_ON_INDEX for: ' || v_unique_candidate_str || ', so retrying recursively for another value.',null,null,null,NULL,NULL,'TDC2_UNIQUE');  
         -- Non Alpha Numeric characters are not replaced by TDC2
         if regexp_instr(v_unique_candidate_str,'[^[:alnum:]]+') > 0 then
            v_unique_candidate_str := regexp_replace(v_unique_candidate_str,'([^[:alnum:]])',gen_rnd_non_alphanum(regexp_substr(v_unique_candidate_str,'([^[:alnum:]])',1,1,'x')),1,0,'x' );
         end if;
         v_unique_candidate_str := TDC2_UNIQUE(v_unique_candidate_str);
      
      when others then
         obfuscation_control.obfus_log('TDC2_UNIQUE error with: ' || v_unique_candidate_str,null,null,null,SQLCODE,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),'TDC2_UNIQUE');
         RAISE;
   end;

   return v_unique_candidate_str;
    
end TDC2_UNIQUE;
/

--------------------------------------------------------
--  DDL for Function OFE
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION OFE (astring varchar2)  return varchar2 as

begin
  return TDC(astring);
end;

/
--------------------------------------------------------
--  DDL for Function OFT
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION OFT (astring varchar2)  return varchar2 as

begin
  return TDC(astring);
end;

/
--------------------------------------------------------
--  DDL for Function OFW
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION OFW (astring varchar2)  return varchar2 as

begin
  return TDC(astring);
end;

/
--------------------------------------------------------
--  DDL for Function RANDOMISE_DATE_30
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION RANDOMISE_DATE_30 (p_date date) return date
   is
      l_anon_date   date;
   begin
      --Add or subtract +/- 30 Days to provided date
      
      if p_date is not null  then
        loop 
          l_anon_date := p_date + ROUND (DBMS_RANDOM.VALUE (-30, 30));
          if  l_anon_date <> p_date then
            return l_anon_date;
          end if;
        end loop;
      end if;
      return l_anon_date;
   end;

/
--------------------------------------------------------
--  DDL for Function RD30
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION RD30 (p_date date) return date
   is
      l_anon_date   date;
   begin
      --Add or subtract +/- 30 Days to provided date
      
      if p_date is not null  then
        loop 
          l_anon_date := p_date + ROUND (DBMS_RANDOM.VALUE (-30, 30));
          if  l_anon_date <> p_date then
            return l_anon_date;
          end if;
        end loop;
      end if;
      return l_anon_date;
   end;

/
--------------------------------------------------------
--  DDL for Function RN
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION RN (note varchar2)  return varchar2 as

  l_nkey number;
  l_words randomised_notes.rnd_words%type;

begin

  l_nkey :=  mod(abs(dbms_random.random), anonymisation_process.g_max_rnd_note ) +1;
  select rnd_words into l_words from randomised_notes
  where key_ns = l_nkey;
  
  return  l_words;
end;

/
--------------------------------------------------------
--  DDL for Function RND_DIGIT_WO_RESEL
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION RND_DIGIT_WO_RESEL (anumber number) return number as
  higher number;
  lower number;
  higher_rnd number;
  lower_rnd number;
begin

  higher := anumber + 1;
  if higher > 9 then higher := 9; end if;
  
  lower := anumber -1;
  if lower < 0 then lower := 0; end if;

  higher_rnd := mod(abs(dbms_random.random), 10-higher )+anumber+1;

  lower_rnd := mod(abs(dbms_random.random), anumber+1);

  if higher = 9 then
    return lower_rnd;
  end if;
  if lower = 0 then
   return higher_rnd;
  end if;
   
  return case when (mod(abs(dbms_random.random),10) - lower) > 0  then higher_rnd else lower_rnd end;
end;

/
--------------------------------------------------------
--  DDL for Function RND_NOTE
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION RND_NOTE return varchar2 as

  l_nkey number;
  l_words randomised_notes.rnd_words%type;

begin

  l_nkey :=  mod(abs(dbms_random.random), 100000 ) +1;
  select rnd_words into l_words from randomised_notes
  where key_ns = l_nkey;
  
  return  l_words;
end;
/

--------------------------------------------------------
--  DDL for Procedure DISABLE_R_CONS
--------------------------------------------------------

  CREATE OR REPLACE PROCEDURE DISABLE_R_CONS (p_table_owner varchar2 ,p_table_name varchar2) is

  cursor get_id_cons(p_table_owner varchar2,p_table_name varchar2) is select constraint_name from all_constraints where constraint_type in ('U','P')
  and owner = p_table_owner and table_name = p_table_name ;
  
  cursor get_ref_cons(p_cons_name varchar2 )  is select  owner,table_name,constraint_name from all_constraints where constraint_type in ('R')
  --and r_owner = p_table_owner 
  and 
  r_constraint_name = p_cons_name;
begin


  
  for get_id_consRec in get_id_cons(p_table_owner,p_table_name) loop
    for get_ref_consRec in get_ref_cons(get_id_consRec.constraint_name) loop
      dbms_output.put_line('alter table '||get_ref_consRec.owner||'.'||get_ref_consRec.table_name||' disable constraint  '||get_ref_consRec.constraint_name); 
      execute immediate 'alter table '||get_ref_consRec.owner||'.'||get_ref_consRec.table_name||' disable constraint  '||get_ref_consRec.constraint_name; 
    end loop;
  end loop;
  
    exception when others then 
          anonymisation_process.g_module := 'disable_r_cons';
          anonymisation_process.g_code := SQLCODE;
          anonymisation_process.g_errm := SUBSTR(SQLERRM, 1 , 64);
          anonymisation_process.obfus_log(p_table_owner ||' '||p_table_name ||' Unhandled Exception',anonymisation_process.g_code,anonymisation_process.g_errm,anonymisation_process.g_module);

  
end;

/
--------------------------------------------------------
--  DDL for Procedure ENABLE_R_CONS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE ENABLE_R_CONS (p_table_owner varchar2 ,p_table_name varchar2) is

  cursor get_id_cons(p_table_owner varchar2,p_table_name varchar2) is select constraint_name from all_constraints where constraint_type in ('U','P')
  and owner = p_table_owner and table_name = p_table_name ;
  
  cursor get_ref_cons(p_table_owner varchar2,p_cons_name varchar2 )  is select owner,table_name,constraint_name from all_constraints where constraint_type in ('R')
  and r_owner = p_table_owner and r_constraint_name = p_cons_name;
begin



  for get_id_consRec in get_id_cons(p_table_owner,p_table_name) loop
    for get_ref_consRec in get_ref_cons(p_table_owner,get_id_consRec.constraint_name) loop
--      dbms_output.put_line('alter table '||get_ref_consRec.table_name||'.'||p_table_name||' enable constraint  '||get_ref_consRec.constraint_name);       
      execute immediate 'alter table '||get_ref_consRec.owner||'.'||get_ref_consRec.table_name||' enable constraint  '||get_ref_consRec.constraint_name; 
    end loop;
  end loop;
  
  exception when others then 
      anonymisation_process.g_module := 'enable_r_cons';
      anonymisation_process.g_code := SQLCODE;
      anonymisation_process.g_errm := SUBSTR(SQLERRM, 1 , 64);
      anonymisation_process.obfus_log('Unhandled Exception',anonymisation_process.g_code,anonymisation_process.g_errm,anonymisation_process.g_module);

end;
/

--------------------------------------------------------
--  DDL for Procedure TRUNCATE_TABLE_NEW
--------------------------------------------------------
set define off;

CREATE OR REPLACE PROCEDURE truncate_table_new (p_table_owner varchar2 ,p_table_name varchar2) is

  cursor truncation_list (cp_table_owner varchar2 ,cp_table_name varchar2) is 
  select case when table_type = 'N' then 'truncate table '|| child_owner||'.'||child else 'alter table '|| child_owner||'.'||child||' truncate partition '||atp.partition_name end stmt
  from 
  (
    select distinct child_owner,child,alevel,table_type
    from
    (
      select  parent_owner,parent,child_owner,child, max(alevel)  over (partition by child_owner,child) alevel,case when apt.owner is null then 'N' else 'T' end table_type
      from 
      (
        (
        select parent_owner,parent,child_owner,child,level alevel 
        from (
            select  parent_table.owner parent_owner,parent_table.table_name parent, child_table.owner child_owner,child_table.table_name child
             from a_tables   parent_table
                  join  a_constraints  parent_constraint on parent_table.owner = parent_constraint.owner  and parent_table.table_name = parent_constraint.table_name and parent_constraint.constraint_type IN( 'P', 'U' )
                  join a_constraints child_constraint on child_constraint.r_constraint_name = parent_constraint.constraint_name and child_constraint.constraint_type   = 'R'
                  join a_tables      child_table on child_table.owner= child_constraint.owner and child_table.table_name = child_constraint.table_name and child_table.table_name != parent_table.table_name
              )
              start with parent = cp_table_name  and parent_owner = cp_table_owner
            connect by NOCYCLE  prior child = parent and child_owner = parent_owner
        ) 
        union
        select null parent_owner ,null parent ,cp_table_owner child_owner ,cp_table_name child,0 alevel from dual 
      ) res
      left outer join all_part_tables apt on  res.child_owner = apt.owner and res.child = apt.table_name
    ) res1
  ) res2
  left outer join all_tab_partitions atp on  res2.child_owner = atp.table_owner and res2.child = atp.table_name
  order by alevel desc;
 


begin
                 
  for truncation_list_rec in truncation_list(p_table_owner,p_table_name) loop
  begin
    execute immediate truncation_list_rec.stmt;
    exception when others then null;
  end;
          
  end loop;
  commit;
 
  exception when others then 
    anonymisation_process.g_module := 'truncate_table';
    anonymisation_process.g_code := SQLCODE;
    anonymisation_process.g_errm := SUBSTR(SQLERRM, 1 , 64);
    anonymisation_process.obfus_log('Unhandled Exception',anonymisation_process.g_code,anonymisation_process.g_errm,anonymisation_process.g_module);
            
end truncate_table_new;
/  