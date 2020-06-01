create table dd_tab_partitions as 
select table_owner , table_name,partition_name from dba_tab_partitions
where table_owner like 'PSM\_%' escape '\';

create index dtp_idx on dd_tab_partitions(table_owner , table_name);

create table dd_part_key_columns as 
select owner,name,object_type,column_name
from dba_part_key_columns
where owner like 'PSM\_%' escape '\';

create  table dd_ind_subpartitions as select index_owner,index_name,partition_name,subpartition_name from dba_ind_subpartitions
where index_owner like 'PSM\_%' escape '\';

create index dis_idx on dd_ind_subpartitions(index_owner,index_name,partition_name);




create or replace PACKAGE BODY ut is

  procedure drop_unused_cols (p_tgt_prefix in varchar2)
  is
     cursor c1 (cin_tgt_prefix in varchar2)
     is
        select t.owner, t.table_name, t.unused_col_count, sum(nvl(t.num_rows,0)) total_rows
          from unused_col_tabs_tmp t
          join per_trans_cols ptc on t.owner = cin_tgt_prefix ||'_'||ptc.owner and t.table_name = ptc.table_name
         where ptc.owner||'.'||ptc.table_name NOT IN ('DATA_EXTRACT.EXTR_STAGINGS','IMPORT.IMPORT_STAGING_TABLE')
           and ptc.transform <> 'NONE'
        group by t.owner, t.table_name, t.unused_col_count
        order by (t.unused_col_count * total_rows) asc;

     v_ddl varchar2(4000);
     v_archive_log_mode  varchar2(12);
     const_module        CONSTANT  varchar2(62) := 'ut.drop_unused_cols';
     v_code              number;
     v_errm              varchar2(4000);

  begin

     obfuscation_control.obfus_log('truncating table unused_col_tabs_tmp',null,null,p_tgt_prefix,null,null,const_module);
     execute immediate 'truncate table unused_col_tabs_tmp';

     obfuscation_control.obfus_log('inserting into unused_col_tabs_tmp for owner: ' || p_tgt_prefix,null,null,p_tgt_prefix,null,null,const_module);
     insert into unused_col_tabs_tmp
        select uct.owner, uct.table_name, uct.count unused_col_count, ats.num_rows, ats.blocks, ats.empty_blocks, ats.partition_name, ats.subpartition_name
          from all_unused_col_tabs uct
          join all_tab_statistics ats on uct.owner = ats.owner and uct.table_name = ats.table_name
         where uct.owner like p_tgt_prefix || '\_%' escape '\';

     obfuscation_control.obfus_log(to_char(sql%rowcount) || ' rows inserted into unused_col_tabs_tmp for owner: ' || p_tgt_prefix,null,null,p_tgt_prefix,null,null,const_module);
     commit;

     for r1 in c1(p_tgt_prefix) loop
        v_ddl := 'alter table ' || r1.owner ||'.'|| r1.table_name || ' drop unused columns';
        obfuscation_control.obfus_log('Executing: ' || v_ddl || ' for ' || to_char(r1.unused_col_count) || ' unused columns with ' || to_char(r1.total_rows) || ' total_rows',null,null,null,null,null,const_module);
        --dbms_output.put_line(v_ddl);
        execute immediate v_ddl;
        obfuscation_control.obfus_log('Finished dropping unused columns for: ' || v_ddl,null,null,p_tgt_prefix,null,null,const_module);
     end loop;

     obfuscation_control.obfus_log('Successfully completed dropping unused columns for: ' || p_tgt_prefix,null,null,p_tgt_prefix,null,null,const_module);

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        obfuscation_control.obfus_log('Unhandled Exception ',null,null,p_tgt_prefix,v_code,v_errm,const_module);
  end drop_unused_cols;


  FUNCTION EXCLUDE (astring varchar2)  return varchar2 as
  begin
     return astring;
  end;


  FUNCTION GET_FORMATTED_SK (pi_name in varchar2,pi_holder_type varchar2,pi_holder_designation varchar2)
    return varchar2 is
   l_count number;
   l_sortkey varchar2(4000);
   l_name varchar2(4000);
   l_holder_designation varchar2(4000);
   const_module  CONSTANT  varchar2(62) := 'ut.get_formatted_sk';

BEGIN

  begin

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

  EXCEPTION
    WHEN OTHERS THEN
      obfuscation_control.obfus_log('l_sortkey: ' || l_sortkey || ' l_name: ' || l_name  ||
                                    ' l_holder_designation: ' || l_holder_designation ||
                                    ' l_count: ' || l_count || ': ' || SUBSTR(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000)
                                    ,null,null,null,SQLCODE,substr(SQLERRM,1,4000),const_module);
  end;

  return l_sortkey;

END get_formatted_sk;


FUNCTION MAN (astring varchar2)  return varchar2 as

begin
  return astring;
end man;


FUNCTION TDC (astring varchar2) return varchar2 as
  nPointer NUMBER := 1;
  nPrevPointer NUMBER := 0;
  ret_str varchar2(4000);
  const_module        CONSTANT  varchar2(62) := 'ut.TDC';
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
    ret_str := ret_str ||substr(astring,nPrevPointer+1,nPointer-nPrevPointer-1)||to_char(ut.rnd_digit_wo_resel(to_number(substr(astring,nPointer,1))));
    nPrevPointer := nPointer;
    nPointer := nPointer+1;

  end loop;
  return REGEXP_REPLACE (ret_str, '([[:alpha:]])', DBMS_RANDOM.string ('U', 1));
end TDC;


FUNCTION TDC2 (p_string varchar2) return varchar2 as
  nPointer NUMBER := 1;
  nPrevPointer NUMBER := 0;
  ret_str  varchar2(4000);
  ret_str2 varchar2(4000);
  const_module        CONSTANT  varchar2(62) := 'ut.TDC2';
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


FUNCTION gen_rnd_non_alphanum ( p_not_char IN VARCHAR DEFAULT NULL )
   return varchar2
AS
   v_non_alphanum VARCHAR2(1);
   const_module        CONSTANT  varchar2(62) := 'ut.gen_rnd_non_alphanum';
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


FUNCTION TDC2_UNIQUE (p_str_for_unique_obfus in varchar2) return varchar2 as
   v_unique_candidate_str varchar2(4000);
   v_candidate_str_len      integer;
   v_tdc2_candidate_str_len integer;
   const_module        CONSTANT  varchar2(62) := 'ut.TDC2_UNIQUE';
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
            v_unique_candidate_str := regexp_replace(v_unique_candidate_str,'([^[:alnum:]])',ut.gen_rnd_non_alphanum(regexp_substr(v_unique_candidate_str,'([^[:alnum:]])',1,1,'x')),1,0,'x' );
         end if;
         v_unique_candidate_str := TDC2_UNIQUE(v_unique_candidate_str);

      when others then
         obfuscation_control.obfus_log('TDC2_UNIQUE error with: ' || v_unique_candidate_str,null,null,null,SQLCODE,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),'TDC2_UNIQUE');
         RAISE;
   end;

   return v_unique_candidate_str;

end TDC2_UNIQUE;


FUNCTION OFE (astring varchar2)  return varchar2 as

begin
  return TDC(astring);
end OFE;


FUNCTION OFT (astring varchar2)  return varchar2 as

begin
  return TDC(astring);
end OFT;


FUNCTION OFW (astring varchar2)  return varchar2 as

begin
  return TDC(astring);
end OFW;


FUNCTION RANDOMISE_DATE_30 (p_date date) return date
   is
      l_anon_date   date;
      const_module        CONSTANT  varchar2(62) := 'ut.RANDOMISE_DATE_30';
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
   end RANDOMISE_DATE_30;


 FUNCTION RD30 (p_date date) return date
   is
      l_anon_date   date;
      const_module        CONSTANT  varchar2(62) := 'ut.RD30';
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
   end RD30;


FUNCTION RN (note varchar2) return varchar2 as

  l_nkey number;
  l_words randomised_notes.rnd_words%type;

  const_module      CONSTANT  varchar2(62) := 'ut.RN';
  v_code            number;
  v_errm            varchar2(4000);

begin

  begin

    l_nkey :=  mod(abs(dbms_random.random), const.k_max_rnd_note) +1;

    select rnd_words
      into l_words
      from randomised_notes
     where key_ns = l_nkey;

  exception
    when no_data_found then
      l_words := 'Default Note';
      obfuscation_control.obfus_log('No Data Found: RND Note gen failure l_nkey: '||to_char(l_nkey),null,null,null,v_code,v_errm,const_module);
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      obfuscation_control.obfus_log('Unhandled Exception: RND Note gen failure l_nkey: '||to_char(l_nkey),null,null,null,v_code,v_errm,const_module);
      l_words :=  'Default Note l_nkey: '||to_char(l_nkey);
  end;

  return l_words;

end RN;


FUNCTION RND_DIGIT_WO_RESEL (anumber number) return number as
  higher number;
  lower number;
  higher_rnd number;
  lower_rnd number;
  const_module        CONSTANT  varchar2(62) := 'ut.RND_DIGIT_WO_RESEL';
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
end RND_DIGIT_WO_RESEL;


--FUNCTION RND_NOTE return varchar2 as
--
--  l_nkey number;
--  l_words randomised_notes.rnd_words%type;
--  const_module        CONSTANT  varchar2(62) := 'ut.RND_NOTE';
--begin
--
--  l_nkey :=  mod(abs(dbms_random.random), 100000 ) +1;
--  select rnd_words into l_words from randomised_notes
--  where key_ns = l_nkey;
--
--  return  l_words;
--end RND_NOTE;


--PROCEDURE DISABLE_R_CONS (p_table_owner varchar2 ,p_table_name varchar2) is
--
--  cursor get_id_cons(p_table_owner varchar2,p_table_name varchar2) is
--     select constraint_name
--       from all_constraints
--      where constraint_type in ('U','P')
--        and owner = p_table_owner and table_name = p_table_name ;
--
--  cursor get_ref_cons(p_cons_name varchar2 )  is
--     select owner,table_name,constraint_name
--       from all_constraints
--      where constraint_type in ('R')
--      --and r_owner = p_table_owner
--        and r_constraint_name = p_cons_name;
--
--  const_module        CONSTANT  varchar2(62) := 'ut.DISABLE_R_CONS';
--
--begin
--
--
--
--  for get_id_consRec in get_id_cons(p_table_owner,p_table_name) loop
--    for get_ref_consRec in get_ref_cons(get_id_consRec.constraint_name) loop
--      dbms_output.put_line('alter table '||get_ref_consRec.owner||'.'||get_ref_consRec.table_name||' disable constraint  '||get_ref_consRec.constraint_name);
--      execute immediate 'alter table '||get_ref_consRec.owner||'.'||get_ref_consRec.table_name||' disable constraint  '||get_ref_consRec.constraint_name;
--    end loop;
--  end loop;
--
--    exception when others then
--          anonymisation_process.g_code := SQLCODE;
--          anonymisation_process.g_errm := SUBSTR(SQLERRM, 1 , 64);
--          anonymisation_process.obfus_log(p_table_owner ||' '||p_table_name ||' Unhandled Exception',anonymisation_process.g_code,anonymisation_process.g_errm,const_module);
--
--end DISABLE_R_CONS;
--
--
--PROCEDURE ENABLE_R_CONS (p_table_owner varchar2 ,p_table_name varchar2) is
--
--  cursor get_id_cons(p_table_owner varchar2,p_table_name varchar2) is
--     select constraint_name
--       from all_constraints
--      where constraint_type in ('U','P')
--        and owner = p_table_owner and table_name = p_table_name ;
--
--  cursor get_ref_cons(p_table_owner varchar2,p_cons_name varchar2 )  is
--     select owner,table_name,constraint_name
--       from all_constraints
--      where constraint_type in ('R')
--        and r_owner = p_table_owner and r_constraint_name = p_cons_name;
--
--  const_module        CONSTANT  varchar2(62) := 'ut.ENABLE_R_CONS';
--
--begin
--
--  for get_id_consRec in get_id_cons(p_table_owner,p_table_name) loop
--    for get_ref_consRec in get_ref_cons(p_table_owner,get_id_consRec.constraint_name) loop
----      dbms_output.put_line('alter table '||get_ref_consRec.table_name||'.'||p_table_name||' enable constraint  '||get_ref_consRec.constraint_name);
--      execute immediate 'alter table '||get_ref_consRec.owner||'.'||get_ref_consRec.table_name||' enable constraint  '||get_ref_consRec.constraint_name;
--    end loop;
--  end loop;
--
--  exception when others then
--      anonymisation_process.g_module := 'enable_r_cons';
--      anonymisation_process.g_code := SQLCODE;
--      anonymisation_process.g_errm := SUBSTR(SQLERRM, 1 , 64);
--      anonymisation_process.obfus_log('Unhandled Exception',anonymisation_process.g_code,anonymisation_process.g_errm,const_module);
--
--end ENABLE_R_CONS;


function fn_date_mask (p_date in date)
  return date
is
  v_date DATE := TO_DATE('01/01/2099','DD/MM/YYYY');
begin
  return v_date;
end fn_date_mask;


function fn_char_mask ( p_string varchar2 )
  return varchar2
is
begin
  return RPAD('*',length(p_string),'*');
end fn_char_mask;


function fn_number_mask ( p_number number )
  return number
is
begin
  return RPAD(9,length(p_number),9);
end fn_number_mask;


PROCEDURE truncate_table_new (p_table_owner varchar2, p_table_name varchar2, p_temporary varchar2 DEFAULT 'N') is

  cursor truncation_list (cp_table_owner varchar2 ,cp_table_name varchar2) is
    select case when table_type = 'N'
                then 'truncate table '|| child_owner||'.'||child || decode(p_temporary,'N',' reuse storage',null)
                else 'alter table '|| child_owner||'.'||child||' truncate partition '||atp.partition_name
           end stmt
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

  const_module      CONSTANT  varchar2(62) := 'ut.truncate_table_new';

begin

  for truncation_list_rec in truncation_list(p_table_owner,p_table_name) loop
    begin
      --anonymisation_process.obfus_log('Executing: ' || truncation_list_rec.stmt,null,null,const_module);
      execute immediate truncation_list_rec.stmt;
    exception
      when others then
        anonymisation_process.g_code := SQLCODE;
        anonymisation_process.g_errm := SUBSTR(SQLERRM,1,4000);
        anonymisation_process.obfus_log('Truncaction Failed for p_table_owner ' || p_table_owner || ' p_table_name ' || p_table_name || ' stmt: ' || truncation_list_rec.stmt,anonymisation_process.g_code,anonymisation_process.g_errm,const_module);
        exit;
    end;
  end loop;

  exception when others then
    anonymisation_process.g_code := SQLCODE;
    anonymisation_process.g_errm := SUBSTR(SQLERRM,1,4000);
    anonymisation_process.obfus_log('Unhandled Exception',anonymisation_process.g_code,anonymisation_process.g_errm,const_module);

end truncate_table_new;


procedure rebuild_indexes (p_table_owner varchar2 ,p_table_name varchar2) is

  cursor c_part_indexes (cp_table_owner varchar2 ,cp_table_name varchar2)
  is
    select apk.owner index_owner, atc.table_name, apk.name index_name, atp.partition_name
     from dd_part_key_columns apk
     join dd_tab_columns atc on apk.owner = atc.owner and apk.column_name = atc.column_name
     join dd_tab_partitions atp on atp.table_owner = atc.owner and atp.table_name = atc.table_name
    where apk.owner = cp_table_owner
    and apk.name like '%'|| atc.table_name ||'%'
    and apk.object_type = 'INDEX'
    and atc.table_name = cp_table_name
    order by atc.table_name, apk.name, atp.partition_name;

  cursor c_subpart_indexes (cp_index_owner varchar2, cp_index_name varchar2, cp_partition_name varchar2)
  is
    select ais.index_owner, ais.index_name, ais.partition_name, ais.subpartition_name
     from dd_ind_subpartitions ais
    where ais.index_owner = cp_index_owner
      and ais.index_name = cp_index_name
      and ais.partition_name = cp_partition_name
    order by ais.subpartition_name;

  v_sConstraintName varchar2 (128);
  const_module      CONSTANT  varchar2(62) := 'ut.rebuild_indexes';
  v_code            number;
  v_errm            varchar2(4000);
  v_ddl             varchar2(4000);
  v_sub_part_count  integer := 0;

begin

  begin
    select ai.constraint_name
    into v_sConstraintName
    from all_constraints ai where owner = p_table_owner and table_name = p_table_name
    and constraint_type = 'P';

    v_ddl := 'alter index  '||p_table_owner||'.'||v_sConstraintName||' rebuild';
    obfuscation_control.obfus_log('Executing: ' || v_ddl,null,null,null,null,null,const_module);
    execute immediate v_ddl;

  exception
    when no_data_found then
       null;
  end;

  for r in c_part_indexes (p_table_owner,p_table_name)
  loop

    v_sub_part_count := 0;

    for i in c_subpart_indexes(r.index_owner, r.index_name, r.partition_name)
    loop
      v_ddl := 'alter index  '||i.index_owner||'.'||i.index_name||' rebuild subpartition ' || i.subpartition_name;
      obfuscation_control.obfus_log('Executing: ' || v_ddl,null,null,null,null,null,const_module);
      execute immediate v_ddl;
      v_sub_part_count := v_sub_part_count + 1;
    end loop;

    if v_sub_part_count = 0 then
      v_ddl := 'alter index  '||r.index_owner||'.'||r.index_name||' rebuild partition ' || r.partition_name;
      obfuscation_control.obfus_log('Executing: ' || v_ddl,null,null,null,null,null,const_module);
      execute immediate v_ddl;
    end if;
  end loop;

exception when others then
    v_code := SQLCODE;
    v_errm := SUBSTR(SQLERRM,1,4000);
    obfuscation_control.obfus_log('Unhandled Exception',null,null,null,v_code,v_errm,const_module);

end rebuild_indexes;


FUNCTION AN (p_account_name varchar2) return varchar2 as

  const_module      CONSTANT  varchar2(62) := 'ut.AN';
  v_code            number;
  v_errm            varchar2(4000);

  l_account_name VARCHAR2(108 CHAR);

begin

  begin

    l_account_name := case when p_account_name is not null
                           then substr (p_account_name,1,const.k_acc_name_length-length(const.k_acc_name_suffix)) || const.k_acc_name_suffix
                           else null end;

  exception
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      obfuscation_control.obfus_log('Unhandled Exception ',null,null,null,v_code,v_errm,const_module);
      l_account_name :=  null;
  end;

  return l_account_name;

end AN;

end ut;
/