create or replace PACKAGE BODY ut is

  procedure log(p_subsystem varchar2,p_log_msg VARCHAR2,p_code NUMBER,p_errm varchar2,p_module varchar2,p_stage_step_code varchar2 default null, p_stage_type varchar2 default null) is
   v_nLogID NUMBER;
  begin

    v_nLogID := log(p_subsystem, p_log_msg, p_code, p_errm, p_module, p_stage_step_code, p_stage_type );

  end log;


  function log(p_subsystem varchar2,p_log_msg VARCHAR2, p_code NUMBER,p_errm varchar2,p_module varchar2,p_stage_step_code varchar2 default null, p_stage_type varchar2 default null)
    return number
  is
    pragma autonomous_transaction;

    v_nLogID NUMBER;
  begin

    if p_subsystem = const.k_subsys_obfus
    then
      v_nLogID := obfuscation_log_seq.nextval;
      insert into obfuscation_log (log_id,stage_step_code,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module, mod_timestamp)
      values (v_nLogID, p_stage_step_code, p_log_msg, gp.src_prefix, gp.anon_version, gp.tgt_prefix, p_code, p_errm, p_module, systimestamp);
      
    elsif p_subsystem = const.k_subsys_subset
    then
       v_nLogID := ss_log_id_seq.nextval;
       insert into ss_log (log_id,stage_step_code,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module, mod_timestamp, ss_run_id, execution_id, stage_type)
       values (v_nLogID, p_stage_step_code, p_log_msg, gp.src_prefix, gp.anon_version, gp.tgt_prefix, p_code, p_errm, p_module, systimestamp, gp.ss_run_id, gp.ss_execution_id, p_stage_type);
    end if;

    commit;

    return v_nLogID;

  end log;


  procedure switch_subsystem_on_off ( p_subsystem       varchar2,
                                      p_on_off          varchar2,
                                      p_run_id          number   DEFAULT NULL,
                                      p_src_prefix      varchar2 DEFAULT NULL,
                                      p_tgt_prefix      varchar2 DEFAULT NULL,
                                      p_run_env         varchar2 DEFAULT NULL,
                                      p_anon_version    varchar2 DEFAULT NULL)
  is
    pragma autonomous_transaction;
    v_code        number;
    v_errm        varchar2(4000);
    v_on_off      varchar2(3);
    const_module  CONSTANT  varchar2(62) := 'ut.switch_subsystem_on_off';
  begin
    select on_off into v_on_off from subsystem_onoff_switch where subsystem = upper(p_subsystem);

    if upper(v_on_off) = upper(p_on_off)
    then
      if upper(p_subsystem) = const.k_subsys_obfus
      then
        ut.log(const.k_subsys_obfus,'obfuscation subsystem_onoff_switch already turned ' || upper(p_on_off),SQLCODE,SQLERRM,const_module);
      elsif upper(p_subsystem) = const.k_subsys_subset
      then
        ut.log(const.k_subsys_subset,'subsetting subsystem_onoff_switch already turned ' || upper(p_on_off),SQLCODE,SQLERRM,const_module);
      end if;
    else
      delete subsystem_onoff_switch where subsystem = upper(p_subsystem); -- only ever allow one row
      insert into subsystem_onoff_switch (subsystem, on_off, description)
      values (upper(p_subsystem), upper(p_on_off), 'OFF disables subsystem and if subsystem is switched OFF while running then subsystem is TERMINATED at completion of current stage. ON allows subsystem as normal.');
      commit;
      ut.log(upper(p_subsystem),'subsystem_onoff_switch turned ' || p_on_off,SQLCODE,SQLERRM,const_module);

      if (p_run_id is not null and p_src_prefix is not null and p_tgt_prefix is not null and p_run_env is not null and p_anon_version is not null)
      then
        if upper(p_subsystem) = const.k_subsys_obfus
        then
          obfuscation_control.update_obfus_control(p_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, upper(p_on_off));
        elsif upper(p_subsystem) = const.k_subsys_subset
        then
          subsetting_control.update_ss_ctrl(p_run_id, p_src_prefix, p_tgt_prefix, p_run_env, p_anon_version, upper(p_on_off));
        end if;
      else
        if upper(p_subsystem) = const.k_subsys_obfus
        then
          update obfus_control set obfus_status = upper(p_on_off), updated_time = SYSDATE where obfus_status IN ( 'ON', 'OFF', 'RUNNING');
        elsif upper(p_subsystem) = const.k_subsys_subset
        then
          update ss_ctrl set ss_status = upper(p_on_off) where ss_status IN ( 'ON', 'OFF', 'RUNNING');
        end if;

      end if;
    end if;

    commit;
  exception
    when excep.x_CHK_ONOFF_SWITCH_violated then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      rollback;
      ut.log(p_subsystem,const_module || ' error ',v_code,v_errm,const_module);
      RAISE_APPLICATION_ERROR(-20002, 'subsystem_onoff_switch must be either ''ON'' or ''OFF''.' || v_errm);
    when excep.x_parent_key_not_found then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      rollback;
      ut.log(p_subsystem,const_module || ' error ',v_code,v_errm,const_module);
      RAISE_APPLICATION_ERROR(-20002, 'subsystem_onoff_switch must be either ''ON'' or ''OFF''.' || v_errm);
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      rollback;
      ut.log(p_subsystem,const_module || ' error ',v_code,v_errm,const_module);
  end switch_subsystem_on_off;


  function get_stmt_seq ( p_subsystem        varchar2,
                          p_stage_step_code  varchar2)
     return integer
  is
     const_module  CONSTANT  varchar2(62) := 'ut.get_stmt_seq';  
     v_stmt_seq    integer;
  begin
     
     if upper(p_subsystem) = const.k_subsys_obfus
     then
        select max(stmt_seq)+1
          into v_stmt_seq
          from obfus_ctrl_stmts
         where obfus_run_id = gp.obfus_run_id
           and stage_step_code = p_stage_step_code;
             
     elsif upper(p_subsystem) = const.k_subsys_subset
     then   
        select max(stmt_seq)+1
          into v_stmt_seq
          from ss_ctrl_stmts
         where ss_run_id = gp.get_ss_run_id
           and stage_step_code = p_stage_step_code;
     end if;
       
     return v_stmt_seq;
        
  end get_stmt_seq;
     
  
  function get_ss_stage_type (p_stage_step_code  varchar2)
     return varchar2
  is
     const_module  CONSTANT  varchar2(62) := 'ut.get_ss_stage_type';   
     v_stage_type  ss_ctrl_stmts.stage_type%type;
  begin
     
     select stage_type
       into v_stage_type
       from ss_ctrl_stmts
      where ss_run_id = gp.get_ss_run_id
        and stage_step_code = p_stage_step_code
        and rownum = 1;
       
     return v_stage_type;
        
  end get_ss_stage_type;


  procedure switch_subsystem_off_after_stage_step ( p_subsystem         varchar2,
                                                    p_stage_step_code   varchar2 )
  is
     pragma autonomous_transaction;
      
     const_module  CONSTANT  varchar2(62) := 'ut.switch_subsystem_off_after_stage_step';
     
     v_code           number;
     v_errm           varchar2(4000);   
     v_new_stmt_seq   integer;
     v_ss_stage_type  ss_ctrl_stmts.stage_type%type;
     
  begin

     if upper(p_subsystem) = const.k_subsys_obfus
     then

        v_new_stmt_seq := get_stmt_seq(const.k_subsys_obfus,p_stage_step_code);

        insert into obfus_ctrl_stmts  (obfus_run_id,stage_step_code,dependent_ss_code,step_type,stmt_seq,owner,stmt)
          select gp.obfus_run_id,
                 p_stage_step_code,
                 null  dependent_ss_code,
                'P' step_type,
                v_new_stmt_seq,
                gp.get_run_env,
                'ut.switch_subsystem_on_off ( '||chr(39)||const.k_subsys_obfus||chr(39)||',''OFF'')' stmt
            from dual;     
     
        ut.log(const.k_subsys_obfus,'inserted '||sql%rowcount||' record into obfus_ctrl_stmts to switch '||const.k_subsys_obfus||' subsystem OFF after stage_step_code '||p_stage_step_code,null,null,const_module);
        
     elsif upper(p_subsystem) = const.k_subsys_subset
     then
    
        v_new_stmt_seq  := get_stmt_seq(const.k_subsys_subset,p_stage_step_code);     
        v_ss_stage_type := get_ss_stage_type(p_stage_step_code);
        
        insert into ss_ctrl_stmts  (ss_run_id,stage_step_code,dependent_ss_code,step_type,stage_type,stmt_seq,owner,stmt)
          select gp.ss_run_id,
                 p_stage_step_code,
                 null  dependent_ss_code,
                'P' step_type,
                v_ss_stage_type,
                v_new_stmt_seq,
                gp.get_run_env,
                'ut.switch_subsystem_on_off ( '||chr(39)||const.k_subsys_subset||chr(39)||',''OFF'')' stmt
            from dual;    

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' record into ss_ctrl_stmts to switch '||const.k_subsys_subset||' subsystem OFF after stage_step_code '||p_stage_step_code,null,null,const_module);
          
     end if;
 
     commit;
  
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        rollback;
        ut.log(const.k_subsys_subset,substr('Unexpected error : '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  end switch_subsystem_off_after_stage_step;


  function can_continue(p_subsystem varchar2)
    return boolean
  is
    v_code       number;
    v_errm       varchar2(4000);
    v_on_off     varchar2(3);
    bln_continue boolean;
    const_module  CONSTANT  varchar2(62) := 'ut.can_continue';
  begin

    begin
      select on_off into v_on_off from subsystem_onoff_switch where subsystem = p_subsystem;
      if upper(v_on_off) = 'OFF'
      then
        bln_continue := FALSE;
      else
        bln_continue := TRUE;
      end if;

    exception when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(p_subsystem, substr(const_module||': '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      bln_continue := FALSE;
    end;
    return bln_continue;
  end can_continue;

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

     ut.log(const.k_subsys_obfus,'truncating table unused_col_tabs_tmp',null,null,const_module);
     execute immediate 'truncate table unused_col_tabs_tmp';

     ut.log(const.k_subsys_obfus,'inserting into unused_col_tabs_tmp for owner: ' || p_tgt_prefix,null,null,const_module);
     insert into unused_col_tabs_tmp
        select uct.owner, uct.table_name, uct.count unused_col_count, ats.num_rows, ats.blocks, ats.empty_blocks, ats.partition_name, ats.subpartition_name
          from all_unused_col_tabs uct
          join all_tab_statistics ats on uct.owner = ats.owner and uct.table_name = ats.table_name
         where uct.owner like p_tgt_prefix || '\_%' escape '\';

     ut.log(const.k_subsys_obfus,to_char(sql%rowcount) || ' rows inserted into unused_col_tabs_tmp for owner: ' || p_tgt_prefix,null,null,const_module);
     commit;

     for r1 in c1(p_tgt_prefix) loop
        v_ddl := 'alter table ' || r1.owner ||'.'|| r1.table_name || ' drop unused columns';
        ut.log(const.k_subsys_obfus,'Executing: ' || v_ddl || ' for ' || to_char(r1.unused_col_count) || ' unused columns with ' || to_char(r1.total_rows) || ' total_rows',null,null,const_module);
        --dbms_output.put_line(v_ddl);
        execute immediate v_ddl;
        ut.log(const.k_subsys_obfus,'Finished dropping unused columns for: ' || v_ddl,null,null,const_module);
     end loop;

     ut.log(const.k_subsys_obfus,'Successfully completed dropping unused columns for: ' || p_tgt_prefix,null,null,const_module);

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_obfus,'Unhandled Exception ',v_code,v_errm,const_module);
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
        l_name := regexp_replace(l_name, '[???????]','A');
        l_name := regexp_replace(l_name, '[?]','C');
        l_name := regexp_replace(l_name, '[????]','E');
        l_name := regexp_replace(l_name, '[?]','N');
        l_name := regexp_replace(l_name, '[???????]','O');
        l_name := regexp_replace(l_name, '[?]','S');
        l_name := regexp_replace(l_name, '[????]','U');
        l_name := regexp_replace(l_name, '[?]','Y');
        l_name := regexp_replace(l_name, '[?]','I');
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
                ELSE  SUBSTR(regexp_substr(l_name,'[^ ]+',1,i),1,1)
          END;

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
        ut.log(const.k_subsys_obfus,'l_sortkey: ' || l_sortkey || ' l_name: ' || l_name  ||
                                      ' l_holder_designation: ' || l_holder_designation ||
                                      ' l_count: ' || l_count || ': ' || SUBSTR(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000)
                                      ,SQLCODE,substr(SQLERRM,1,4000),const_module);
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
           ut.log(const.k_subsys_obfus,'Length of input string: ' || p_str_for_unique_obfus || ' not equal to length of output: ' || v_unique_candidate_str,NULL,NULL,'TDC2_UNIQUE');
           --DBMS_OUTPUT.PUT_LINE('input: ' || p_str_for_unique_obfus);
           --DBMS_OUTPUT.PUT_LINE('insert: ' || v_unique_candidate_str);
        END IF;

        v_unique_candidate_str := NVL(v_unique_candidate_str,dbms_random.string('P',1));
        insert into tbl_TDC2_UNIQUE (UNIQUE_COLUMN) VALUES (v_unique_candidate_str);

     exception
        when dup_val_on_index then
           ut.log(const.k_subsys_obfus,'DUP_VAL_ON_INDEX for: ' || v_unique_candidate_str || ', so retrying recursively for another value.',NULL,NULL,'TDC2_UNIQUE');
           -- Non Alpha Numeric characters are not replaced by TDC2
           if regexp_instr(v_unique_candidate_str,'[^[:alnum:]]+') > 0 then
              v_unique_candidate_str := regexp_replace(v_unique_candidate_str,'([^[:alnum:]])',ut.gen_rnd_non_alphanum(regexp_substr(v_unique_candidate_str,'([^[:alnum:]])',1,1,'x')),1,0,'x' );
           end if;
           v_unique_candidate_str := TDC2_UNIQUE(v_unique_candidate_str);

        when others then
           ut.log(const.k_subsys_obfus,'TDC2_UNIQUE error with: ' || v_unique_candidate_str,SQLCODE,DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),'TDC2_UNIQUE');
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
        ut.log(const.k_subsys_obfus,'No Data Found: RND Note gen failure l_nkey: '||to_char(l_nkey),v_code,v_errm,const_module);
      when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_obfus,'Unhandled Exception: RND Note gen failure l_nkey: '||to_char(l_nkey),v_code,v_errm,const_module);
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
        --ut.log(const.k_subsys_obfus,'Executing: ' || truncation_list_rec.stmt,null,null,const_module);
        execute immediate truncation_list_rec.stmt;
      exception
        when others then
          anonymisation_process.g_code := SQLCODE;
          anonymisation_process.g_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_obfus,'Truncaction Failed for p_table_owner ' || p_table_owner || ' p_table_name ' || p_table_name || ' stmt: ' || truncation_list_rec.stmt,anonymisation_process.g_code,anonymisation_process.g_errm,const_module);
          exit;
      end;
    end loop;

    exception when others then
      anonymisation_process.g_code := SQLCODE;
      anonymisation_process.g_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_obfus,'Unhandled Exception',anonymisation_process.g_code,anonymisation_process.g_errm,const_module);

  end truncate_table_new;


  procedure rebuild_indexes (p_table_owner varchar2 ,p_table_name varchar2) is

    cursor c_part_indexes (cp_table_owner varchar2 ,cp_table_name varchar2)
    is
      select apk.owner index_owner, atc.table_name, apk.name index_name, atp.partition_name
       from all_part_key_columns apk
       join all_tab_columns atc on apk.owner = atc.owner and apk.column_name = atc.column_name
       join all_tab_partitions atp on atp.table_owner = atc.owner and atp.table_name = atc.table_name
      where apk.owner = cp_table_owner
      and apk.name like '%'|| atc.table_name ||'%'
      and apk.object_type = 'INDEX'
      and atc.table_name = cp_table_name
      order by atc.table_name, apk.name, atp.partition_name;

    cursor c_subpart_indexes (cp_index_owner varchar2, cp_index_name varchar2, cp_partition_name varchar2)
    is
      select ais.index_owner, ais.index_name, ais.partition_name, ais.subpartition_name
       from all_ind_subpartitions ais
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
      ut.log(const.k_subsys_obfus,'Executing: ' || v_ddl,null,null,const_module);
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
        ut.log(const.k_subsys_obfus,'Executing: ' || v_ddl,null,null,const_module);
        execute immediate v_ddl;
        v_sub_part_count := v_sub_part_count + 1;
      end loop;

      if v_sub_part_count = 0 then
        v_ddl := 'alter index  '||r.index_owner||'.'||r.index_name||' rebuild partition ' || r.partition_name;
        ut.log(const.k_subsys_obfus,'Executing: ' || v_ddl,null,null,const_module);
        execute immediate v_ddl;
      end if;
    end loop;

  exception when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_obfus,'Unhandled Exception',v_code,v_errm,const_module);

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
        ut.log(const.k_subsys_obfus,'Unhandled Exception ',v_code,v_errm,const_module);
        l_account_name :=  null;
    end;

    return l_account_name;

  end AN;

  procedure load_dd_stats (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.load_dd_stats';
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);

  begin

    if p_subsystem = const.k_subsys_obfus
    then
       v_prefix := gp.tgt_prefix;
    elsif p_subsystem = const.k_subsys_subset
    then
       v_prefix := gp.tgt_prefix;
    end if;

    ut.log(p_subsystem,'truncating tables dd_tab_columns, dd_tab_stats and dd_tab_col_stats',null,null,const_module);
    execute immediate 'truncate table dd_tab_columns';
    execute immediate 'truncate table dd_tab_stats';
    execute immediate 'truncate table dd_tab_col_stats';

    insert into dd_tab_columns( actual_owner,owner, table_name,column_name,data_type,data_type_mod,
                data_type_owner,data_length,data_precision,data_scale,nullable,column_id,default_length,num_distinct,
                density,num_nulls,num_buckets,last_analyzed,sample_size,character_set_name,
                char_col_decl_length,global_stats,user_stats,avg_col_len,char_length)
    select owner actual_owner,replace(owner,v_prefix||'_',null) owner, table_name,column_name,data_type,data_type_mod,
           data_type_owner,data_length,data_precision,data_scale,nullable,column_id,default_length,num_distinct,
           density,num_nulls,num_buckets,last_analyzed,sample_size,character_set_name,
           char_col_decl_length,global_stats,user_stats,avg_col_len,char_length
      from dba_tab_columns
     where owner like v_prefix||'\_%' escape '\';

    ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_tab_columns',null,null,const_module);


    insert into dd_tab_stats(owner,actual_owner,table_name,partition_name,partition_position,subpartition_name,
                subpartition_position,object_type,num_rows,blocks,empty_blocks,avg_space,chain_cnt,avg_row_len,
                avg_space_freelist_blocks,num_freelist_blocks,avg_cached_blocks,avg_cache_hit_ratio,im_imcu_count,
                im_block_count,im_stat_update_time,scan_rate,sample_size,last_analyzed,global_stats,user_stats,
                stattype_locked,stale_stats,scope)
     select  replace(owner,v_prefix||'_',null) owner,owner actual_owner,	table_name,partition_name,
                partition_position,subpartition_name,subpartition_position,object_type,num_rows,blocks,
                empty_blocks,avg_space,chain_cnt,avg_row_len,avg_space_freelist_blocks,num_freelist_blocks,
                avg_cached_blocks,avg_cache_hit_ratio,im_imcu_count,im_block_count,
                im_stat_update_time,scan_rate,sample_size,last_analyzed,global_stats,
                user_stats,stattype_locked,stale_stats,scope
      from dba_tab_statistics
     where owner like v_prefix||'\_%' escape '\';

    ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_tab_stats',null,null,const_module);

    insert into  dd_tab_col_stats( owner,  actual_owner,table_name,column_name,num_distinct,low_value,high_value,density ,
                 num_nulls,num_buckets,last_analyzed,sample_size,global_stats,user_stats,notes,avg_col_len,histogram ,scope )
    select replace(owner,v_prefix||'_',null)  owner,owner actual_owner,table_name,column_name,num_distinct,low_value,high_value,density,num_nulls,num_buckets,
           last_analyzed,sample_size,global_stats,user_stats,notes,avg_col_len,histogram,scope
      from dba_tab_col_statistics
     where owner like v_prefix||'\_%' escape '\';

    ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_tab_col_stats',null,null,const_module);

    ut.load_dd_tab_partitions(p_subsystem);

    commit;

  exception
    when others then
       v_code := SQLCODE;
       v_errm := SUBSTR(SQLERRM,1,4000);
       ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
       subsetting_control.update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_dd_loaded => 'FAILED');

  end load_dd_stats;

  procedure load_dd_constraints (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.load_dd_constraints';
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);

  begin

    if p_subsystem = const.k_subsys_obfus
    then
       v_prefix := gp.tgt_prefix;
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
    elsif p_subsystem = const.k_subsys_subset
    then
       v_prefix := gp.tgt_prefix;
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
    end if;

    ut.log(p_subsystem,'deleting  dd_constraints',null,null,const_module);
    delete dd_constraints;

    begin

      insert into dd_constraints (
         actual_owner,owner,constraint_name,constraint_type,table_name,
         actual_r_owner, r_owner,r_constraint_name,
         actual_index_owner,index_owner,index_name
      )
      select owner actual_owner, replace(owner,v_prefix||'_',null) owner, constraint_name,constraint_type,table_name,
             r_owner actual_r_owner, replace(r_owner,v_prefix||'_',null) r_owner, r_constraint_name,
             index_owner actual_index_owner, replace(index_owner,v_prefix||'_',null) index_owner,index_name
        from dba_constraints
       where owner like v_prefix||'\_%' escape '\';

      ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_constraints',null,null,const_module);

      commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

  end load_dd_constraints;


procedure load_dd_synonyms (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.load_dd_synonyms';
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);

  begin

    if p_subsystem = const.k_subsys_obfus
    then
       v_prefix := gp.tgt_prefix;
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
    elsif p_subsystem = const.k_subsys_subset
    then
       v_prefix := gp.tgt_prefix;
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
    end if;

    ut.log(p_subsystem,'deleting  dd_synonyms',null,null,const_module);
    delete dd_synonyms;

    begin

      insert into dd_synonyms (owner,actual_owner ,synonym_name ,table_owner ,table_name,db_link,origin_con_id )
      select replace(owner,v_prefix||'_',null) owner,owner actual_owner ,synonym_name ,table_owner ,table_name,db_link,origin_con_id
      from dba_synonyms
      where owner like v_prefix||'\_%' escape '\';


      ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_synonyms',null,null,const_module);

      commit;

      exception
         when others then
            v_code := SQLCODE;
            v_errm := SUBSTR(SQLERRM,1,4000);
            ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

  end load_dd_synonyms;


procedure load_dd_tab_partitions (p_subsystem varchar2)
is

   const_module   CONSTANT  varchar2(62) := 'ut.load_dd_tab_partitions';
   v_code         number;
   v_errm         varchar2(4000);

begin

    if p_subsystem = const.k_subsys_obfus
    then
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
    elsif p_subsystem = const.k_subsys_subset
    then
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
    end if;

    ut.log(p_subsystem,'deleting dd_tab_partitions',null,null,const_module);
    delete dd_tab_partitions;
    ut.log(p_subsystem,to_char(sql%rowcount) || ' rows deleted from dd_tab_partitions',null,null,const_module);

    begin

      insert into dd_tab_partitions (actual_table_owner,table_owner,table_name,partition_name,num_rows,last_analyzed)
        select table_owner actual_table_owner, replace(table_owner,gp.tgt_prefix||'_',null) table_owner, table_name,partition_name,num_rows,last_analyzed
          from dba_tab_partitions
         where table_owner like gp.tgt_prefix||'\_%' escape '\';


      ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_tab_partitions',null,null,const_module);

      commit;

    exception
         when others then
            v_code := SQLCODE;
            v_errm := SUBSTR(SQLERRM,1,4000);
            ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

    commit;
end load_dd_tab_partitions;


procedure load_dd (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.load_dd';
    v_code           number;
    v_errm           varchar2(4000);

  begin


      --load_dd_users (p_subsystem);
      load_dd_views (p_subsystem);  

      load_dd_tables(p_subsystem);

      load_dd_stats(p_subsystem);

      load_dd_constraints(p_subsystem);

      load_dd_synonyms(p_subsystem);

      load_dd_tab_privs(p_subsystem);

      load_dd_cons_columns(p_subsystem);

      load_dd_ind_columns(p_subsystem);
      
      load_dd_indexes(p_subsystem);

      load_dd_objects(p_subsystem);
      
      load_dd_part_tables(p_subsystem);

      if p_subsystem = const.k_subsys_obfus
      then
        obfuscation_control.update_obfus_control(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_dd_loaded => 'Y');
      elsif p_subsystem = const.k_subsys_subset
      then
        subsetting_control.update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_dd_loaded => 'Y');
      end if;

      commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);

          if p_subsystem = const.k_subsys_obfus
          then
            obfuscation_control.update_obfus_control(gp.obfus_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_dd_loaded => 'FAILED');
          elsif p_subsystem = const.k_subsys_subset
          then
            subsetting_control.update_ss_ctrl(gp.ss_run_id, gp.src_prefix, gp.tgt_prefix, gp.run_env, gp.anon_version, p_dd_loaded => 'FAILED');
          end if;

  end load_dd;
  
procedure load_dd_part_tables (p_subsystem varchar2)
is

   const_module   CONSTANT  varchar2(62) := 'ut.load_dd_part_tables';
   v_code         number;
   v_errm         varchar2(4000);
   v_prefix         varchar2(128);

begin

  if p_subsystem = const.k_subsys_obfus
  then
       v_prefix := gp.tgt_prefix;
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
  elsif p_subsystem = const.k_subsys_subset
  then
       v_prefix := gp.tgt_prefix;
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
  end if;


  delete  dd_part_tables;
  ut.log(p_subsystem,'deleted '||sql%rowcount||' rows from dd_part_tables',null,null,const_module);
  

  insert into dd_part_tables (actual_owner,owner,table_name,partitioning_type,subpartitioning_type)
        select owner actual_table_owner,  replace(owner,v_prefix||'_',null) owner, table_name,partitioning_type,subpartitioning_type
          from dba_part_tables
          where owner like v_prefix||'\_%' escape '\';
         
  ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_part_tables',null,null,const_module);

  commit;
end load_dd_part_tables;

   procedure load_dd_objects (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.dd_objects';
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);

  begin

    if p_subsystem = const.k_subsys_obfus
    then
       v_prefix := gp.tgt_prefix;
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
    elsif p_subsystem = const.k_subsys_subset
    then
       v_prefix := gp.tgt_prefix;
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
    end if;

    ut.log(p_subsystem,'deleting  dd_objects',null,null,const_module);
    delete dd_objects;

    begin

        insert into dd_objects (object_id, actual_owner, owner, object_type, object_name, LAST_DDL_TIME, STATUS, TEMPORARY)
        select object_id, owner actual_owner, replace(owner,v_prefix||'_',null) owner, object_type, object_name, LAST_DDL_TIME, STATUS, TEMPORARY
          from dba_objects
         where owner like v_prefix||'\_%' escape '\'
           and object_id is not null;

      ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_objects',null,null,const_module);


      commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

  end load_dd_objects;


procedure load_dd_ind_columns (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.load_dd_ind_columns';
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);

  begin

    if p_subsystem = const.k_subsys_obfus
    then
       v_prefix := gp.tgt_prefix;
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
    elsif p_subsystem = const.k_subsys_subset
    then
       v_prefix := gp.tgt_prefix;
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
    end if;

    ut.log(p_subsystem,'deleting  dd_ind_columns',null,null,const_module);
    delete dd_ind_columns;

    begin

      insert into dd_ind_columns (
         actual_index_owner,index_owner,index_name,
         actual_table_owner,table_owner,table_name,
         column_name,column_position,column_length,char_length
      )
      select index_owner actual_index_owner, replace(index_owner,v_prefix||'_',null) index_owner, index_name,
             table_owner actual_table_owner, replace(table_owner,v_prefix||'_',null) table_owner, table_name,
             column_name, column_position, column_length, char_length
        from dba_ind_columns
       where index_owner like v_prefix||'\_%' escape '\';

      ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_ind_columns',null,null,const_module);


      commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

  end load_dd_ind_columns;


  procedure load_dd_indexes (p_subsystem varchar2) as
  
      start_time date;
      end_time date;
  
      const_module     CONSTANT  varchar2(62) := 'ut.load_dd_indexes';
      v_code           number;
      v_errm           varchar2(4000);
      v_prefix         varchar2(128);
  
  begin
  
    begin
  
      if p_subsystem = const.k_subsys_obfus
      then
         v_prefix := gp.tgt_prefix;
         gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
      elsif p_subsystem = const.k_subsys_subset
      then
         v_prefix := gp.tgt_prefix;
         gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
      end if;
  
      ut.log(p_subsystem,'deleting dd_indexes',null,null,const_module);
      delete dd_indexes;
      ut.log(p_subsystem,'deleted '||sql%rowcount||' rows from dd_indexes',null,null,const_module);    
  
     insert into dd_indexes (
        actual_owner,owner,index_name,
        index_type,actual_table_owner,table_owner,
        table_name,table_type
     )
     select owner actual_owner, replace(owner,v_prefix||'_',null) owner, index_name,
            index_type,table_owner,replace(table_owner,v_prefix||'_',null),
            table_name,table_type
       from dba_indexes
      where owner like v_prefix||'\_%' escape '\';
  
     ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_indexes',null,null,const_module);
  
     commit;
  
    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;
  
  end load_dd_indexes;


procedure load_dd_cons_columns (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.dd_cons_columns';
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);

  begin

    if p_subsystem = const.k_subsys_obfus
    then
       v_prefix := gp.tgt_prefix;
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
    elsif p_subsystem = const.k_subsys_subset
    then
       v_prefix := gp.tgt_prefix;
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
    end if;

    ut.log(p_subsystem,'deleting  dd_cons_columns',null,null,const_module);
    delete dd_cons_columns;

    begin

      insert into dd_cons_columns (
         actual_owner,owner,constraint_name,table_name,column_name,position
      )
      select owner actual_owner, replace(owner,v_prefix||'_',null) owner, constraint_name, table_name, column_name, position
        from dba_cons_columns
       where owner like v_prefix||'\_%' escape '\';

      ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_cons_columns',null,null,const_module);

      commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

  end load_dd_cons_columns;


 procedure load_dd_tables (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.load_dd_tables';
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);

  begin

    if p_subsystem = const.k_subsys_obfus
    then
       v_prefix := gp.tgt_prefix;
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
    elsif p_subsystem = const.k_subsys_subset
    then
       v_prefix := gp.tgt_prefix;
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
    end if;

    delete dd_tables;
    ut.log(p_subsystem,to_char(sql%rowcount) || ' rows deleted from dd_tables',null,null,const_module);
    
    begin

        insert into dd_tables (actual_owner, owner, table_name,tablespace_name,temporary,num_rows,iot_type,iot_name)
        select owner actual_owner, replace(owner,v_prefix||'_',null) owner, table_name,tablespace_name,temporary,num_rows,iot_type,iot_name
          from dba_tables
         where owner like v_prefix||'\_%' escape '\';

      ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_tables',null,null,const_module);

      commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

  end load_dd_tables;


  procedure load_dd_views (p_subsystem varchar2) as
  
     const_module   CONSTANT  varchar2(62) := 'ut.load_dd_views';
     v_code         number;
     v_errm         varchar2(4000);
  
  begin
  
     delete  dd_views;
     ut.log(p_subsystem,to_char(sql%rowcount) || ' rows deleted from dd_views',null,null,const_module);
  
     insert into dd_views (actual_owner, owner, view_name)
        select owner actual_owner, replace(owner,gp.tgt_prefix||'_',null) owner, view_name
          from dba_views
         where owner like gp.tgt_prefix||'\_%' escape '\';
  
     ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_views',null,null,const_module);
  
     commit;
  
  end load_dd_views; 

procedure load_dd_tab_privs (p_subsystem varchar2) as

    start_time date;
    end_time date;

    const_module     CONSTANT  varchar2(62) := 'ut.load_dd_tab_privs';
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);

  begin

    if p_subsystem = const.k_subsys_obfus
    then
       v_prefix := gp.tgt_prefix;
       gp.obfus_run_id := obfuscation_control.fn_existing_obfus_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.obfus_run_id);
    elsif p_subsystem = const.k_subsys_subset
    then
       v_prefix := gp.tgt_prefix;
       gp.ss_run_id := subsetting_control.fn_existing_ss_run_id(gp.src_prefix,gp.run_env,gp.tgt_prefix,gp.ss_run_id);
    end if;

    ut.log(p_subsystem,'deleting  dd_tab_privs',null,null,const_module);
    delete dd_tab_privs;

    begin

        insert into dd_tab_privs (grantee, actual_owner, owner, table_name, grantor, privilege, grantable, hierarchy, common, type, inherited)
        select grantee, owner actual_owner, replace(owner,v_prefix||'_',null) as owner, table_name, grantor, privilege, grantable, hierarchy, common, type, inherited
          from dba_tab_privs
         where grantee like v_prefix||'\_%' escape '\';

      ut.log(p_subsystem,to_char(sql%rowcount) || ' rows inserted into dd_tab_privs',null,null,const_module);

      commit;


    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
    end;

  end load_dd_tab_privs;



  procedure drop_table_if_exists (p_subsystem varchar2, p_table_name varchar2)
  is
    const_module     CONSTANT  varchar2(62) := 'ut.drop_table_if_exists';
    v_code           number;
    v_errm           varchar2(4000);
    v_ddl            varchar2(1000);
  begin

    v_ddl := 'drop table ' || p_table_name || ' purge';
    ut.log(p_subsystem,'Executing: ' || v_ddl,null,null,const_module);
    execute immediate v_ddl;

  exception
    when excep.x_table_not_exist then
      raise;
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  end drop_table_if_exists;


  procedure execute_stmt (p_subsystem        varchar2,
                          p_stmt             varchar2, 
                          p_stage_type       varchar2, 
                          p_stage_step_code  varchar2, 
                          p_stmt_seq         number )
  is
     v_code              number;
     v_errm              varchar2(4000);
     const_module        CONSTANT  varchar2(62) := 'ut.execute_stmt';
     vLogID              number;
     vStartTS            timestamp;
     v_run_id            number;
     v_execution_id      number; 
     
  begin
  
     vStartTS := systimestamp;
     if p_subsystem = const.k_subsys_subset then
        v_run_id       := to_number(gp.get_global_var('SS_RUN_ID'));
        v_execution_id := to_number(gp.get_global_var('SS_EXECUTION_ID'));
     elsif p_subsystem = const.k_subsys_obfus then
        v_run_id       := to_number(gp.get_global_var('OBFUS_RUN_ID'));
        v_execution_id := to_number(gp.get_global_var('OBFUS_EXECUTION_ID'));
     end if;     
             
     ut.log(p_subsystem,'executing: '||p_stmt,null,null,const_module,p_stage_step_code,p_stage_type);
     execute immediate p_stmt;
     commit;

     if p_subsystem = const.k_subsys_subset then
        subsetting_control.merge_ss_ctrl_exec_result(v_run_id,p_stage_step_code,p_stmt_seq,v_execution_id,null,systimestamp,const.k_COMPLETED,null);
     elsif p_subsystem = const.k_subsys_obfus then
        obfuscation_control.merge_obfus_ctrl_exec_result(v_run_id,p_stage_step_code,p_stmt_seq,v_execution_id,null,systimestamp,const.k_COMPLETED,null);
     end if;     
     
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        vLogID := ut.log(p_subsystem,substr('Error: p_stmt: '||p_stmt ||': '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),SQLCODE,SQLERRM,const_module,p_stage_step_code,p_stage_type);
        if p_subsystem = const.k_subsys_subset then
           subsetting_control.merge_ss_ctrl_exec_result(v_run_id,p_stage_step_code,p_stmt_seq,v_execution_id,null,systimestamp,const.k_FAILED,vLogID);
        elsif p_subsystem = const.k_subsys_obfus then
           obfuscation_control.merge_obfus_ctrl_exec_result(v_run_id,p_stage_step_code,p_stmt_seq,v_execution_id,null,systimestamp,const.k_FAILED,vLogID);
        end if;
        
        rollback;
  end execute_stmt;
  

  procedure sleep ( p_subsystem      varchar2,
                    p_sleep_seconds  number )
  is
     const_module   CONSTANT  varchar2(62) := 'ut.sleep';
     v_sql          varchar2(4000);
     v_code         number;
     v_errm         varchar2(4000);
  begin
     dbms_lock.sleep(p_sleep_seconds);
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        ut.log(p_subsystem,'sleep error:'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),v_code,v_errm,const_module);
  end sleep;


  procedure wait_for_ss_jobs ( p_job_name_like varchar2 default null )
  is
     const_module   CONSTANT  varchar2(62) := 'ut.wait_for_ss_jobs';
     v_sql          varchar2(4000);
     v_code         number;
     v_errm         varchar2(4000);
     --v_nCount       number;     
  begin
     while ut.get_subsystem_running_job_cnt(const.k_subsys_subset, p_job_name_like) > 0
     loop
        --v_nCount := v_nCount + 1;
        ut.log(const.k_subsys_subset,'Subsetting jobs still running ... sleeping: '||const.k_sleep_seconds,null,null,const_module);
        ut.sleep(const.k_subsys_subset,const.k_sleep_seconds);   
     end loop;       
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        ut.log(const.k_subsys_subset,substr('Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  end wait_for_ss_jobs;
  

  function get_subsystem_running_job_cnt (p_subsystem    varchar2, p_job_name_like varchar2 default null)
    return number
  is
     const_module   CONSTANT  varchar2(62) := 'ut.get_subsystem_running_job_cnt';
     v_code         number;
     v_errm         varchar2(4000);
     v_running_job_cnt number;
  begin
     begin
        if p_subsystem = const.k_subsys_subset
        then
        
          select count(*)
            into v_running_job_cnt
            from all_scheduler_jobs
           where state = 'RUNNING'
             and job_name like nvl(p_job_name_like,'SS_')||'%'
             and job_name not like const.k_overrun_job_prefix ||'%'
             and job_name <> const.k_SS_JOB_MONITOR;

        elsif p_subsystem = const.k_subsys_obfus
        then
             
          select count(*)
            into v_running_job_cnt
            from all_scheduler_jobs
           where state = 'RUNNING'
            -- and job_name like'OB_%' ?
             and job_name not like const.k_overrun_job_prefix ||'%'
             and job_name <> const.k_OBFUS_JOB_MONITOR;
             
        end if;
             
     exception
        when others then
           ut.log(p_subsystem,'get_subsystem_running_job_cnt error:'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),v_code,v_errm,const_module);
     end;

     return v_running_job_cnt;

  end get_subsystem_running_job_cnt;


  function get_all_running_job_cnt (p_subsystem    varchar2)
    return number
  is
     const_module   CONSTANT  varchar2(62) := 'ut.get_all_running_job_cnt';
     v_code         number;
     v_errm         varchar2(4000);
     v_running_job_cnt number;
  begin
  
     begin
        select count(*)
          into v_running_job_cnt
          from all_scheduler_jobs
         where state = 'RUNNING';             
     exception
        when others then
           ut.log(p_subsystem,'get_all_running_job_cnt error:'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),v_code,v_errm,const_module);
     end;

     return v_running_job_cnt;

  end get_all_running_job_cnt;


  function get_incomplete_job_cnt (p_subsystem varchar2, p_run_id number, p_start_date date)
    return number
  is
     const_module   CONSTANT  varchar2(62) := 'ut.get_incomplete_job_cnt';
     v_code                number;
     v_errm                varchar2(4000);
     v_incomplete_job_cnt  number;

  begin
     begin

        select count(*)
          into v_incomplete_job_cnt
          from job_execution
         where subsystem = p_subsystem
           and completed_yn = 'N'
           and run_id = p_run_id
           and execution_id = gp.ss_execution_id;

     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM,1,4000);
           ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     end;

     return v_incomplete_job_cnt;

  end get_incomplete_job_cnt;


  function get_job_errors ( p_subsystem varchar2, p_job_name varchar2 )
    return varchar2
  is
     const_module   CONSTANT  varchar2(62) := 'ut.get_job_errors';
     v_code                number;
     v_errm                varchar2(4000);
     v_job_status          all_scheduler_job_run_details.status%type;
     v_errors              all_scheduler_job_run_details.errors%type;

  begin
     begin

        select errors, status
          into v_errors, v_job_status
          from (
                select errors, status
                  from all_scheduler_job_run_details
                 where job_name = upper(p_job_name)
                   and status <> 'SUCCEEDED'
                 order by log_date DESC
        )
        where rownum = 1;

        ut.log(p_subsystem,substr('Job '||p_job_name||' is '||v_job_status||' with errors: '||v_errors,1,4000),null,null,const_module);

     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           ut.log(p_subsystem,substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
     end;

     return v_errors;

  end get_job_errors;


 function get_job_status ( p_subsystem    varchar2,
                           p_job_name     varchar2 )
    return varchar2
  is
     const_module   CONSTANT  varchar2(62) := 'ut.get_job_status';
     v_code                number;
     v_errm                varchar2(4000);
     v_job_start_date      all_scheduler_jobs.start_date%type;
     v_job_name            all_scheduler_jobs.job_name%type;
     v_job_status          all_scheduler_jobs.state%type;
  begin
     begin
        
        select job_name,status
          into v_job_name,v_job_status
          from (
            select nvl(asj.job_name,jrd.job_name) job_name, nvl(asj.state,jrd.status) status
              from all_scheduler_job_run_details jrd
              join ss_ctrl ctl on ctl.ss_run_id = gp.ss_run_id 
               and jrd.log_date > ctl.run_start_time 
                 full outer join all_scheduler_jobs asj on asj.job_name = jrd.job_name
             where ( jrd.job_name = p_job_name or asj.job_name = p_job_name )
             order by log_date desc
        )
        where rownum = 1;

        --ut.log(p_subsystem,'Job '||v_job_name||' is currently '||v_job_status,null,null,const_module);  

     exception
        when no_data_found then
           v_job_status := null;
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     end;

     return v_job_status;

  end get_job_status;


  procedure run_job ( p_subsystem  varchar2, p_job_name   varchar2 )                
  is

     const_module   CONSTANT  varchar2(62) := 'ut.run_job';
     
     v_code                   number;
     v_errm                   varchar2(4000);
    
  begin

     ut.log(p_subsystem,'Running job: ' || p_job_name,null,null,const_module);        

     begin
        DBMS_SCHEDULER.RUN_JOB(upper(p_job_name));
     exception
        when excep.x_unknown_job
        then                  
           null;     
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           ut.log(p_subsystem,substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);        
           raise;
     end;    
     
  end run_job;
  

  procedure monitor_jobs (p_subsystem varchar2, p_run_id number, p_execution_id number, p_start_date date)
  is
     
     const_module   CONSTANT  varchar2(62) := 'ut.monitor_jobs';
       
     cursor c_job_monitor ( cp_subsystem varchar2, cp_run_id number, cp_start_date in date )
     is
       select jex.job_name, nvl(jrd.status,asj.state) status, jrd.log_date --, jrd.run_duration, jrd.errors
         from job_execution jex
         left outer join all_scheduler_jobs asj            on jex.job_name = asj.job_name
         left outer join all_scheduler_job_run_details jrd on jex.job_name = jrd.job_name
        where subsystem = cp_subsystem
          and run_id = cp_run_id
          and actual_start_date > cp_start_date
          and jex.completed_yn = 'N'
       order by jrd.log_date DESC; 
 
     v_code                number;
     v_errm                varchar2(4000);
   --  v_run_id              number;  
     v_incomplete_job_cnt  number;
     v_running_job_cnt     number;
     
  begin  
     
     begin
--        if p_subsystem = const.k_subsys_subset
--        then
--           v_run_id := gp.ss_run_id;
--        elsif p_subsystem = const.k_subsys_obfus
--        then
--           v_run_id := gp.obfus_run_id;
--        end if;

        for r in c_job_monitor(p_subsystem, p_run_id, p_start_date)
        loop
           merge_job_execution(p_subsystem, p_run_id, r.job_name, p_execution_id);           
        end loop;  
      
        v_incomplete_job_cnt := ut.get_incomplete_job_cnt(const.k_subsys_subset,p_run_id,p_start_date);
       -- ut.log(p_subsystem,to_char(v_incomplete_job_cnt)||' SS jobs not completed',null,null,const_module);
         
        v_running_job_cnt := ut.get_subsystem_running_job_cnt(const.k_subsys_subset);
       -- ut.log(p_subsystem,to_char(v_running_job_cnt)||' SS jobs currently running',null,null,const_module);  
     
     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM,1,4000);
           ut.log(p_subsystem,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     end;     
     
  end monitor_jobs;
  

  procedure create_job ( p_subsystem        varchar2,
                         p_job_name         varchar2,
                         p_job_action       varchar2,
                         p_job_type         varchar2  default 'PLSQL_BLOCK',
                         p_repeat_interval  varchar2  default null,
                         p_comments         varchar2  default null )
  is

     v_code                   number;
     v_errm                   varchar2(4000);
     v_job_name               varchar2(50);
     v_job_action             varchar2(4000);
     v_event_condition        varchar2(4000);
     v_queue_spec             varchar2(1000);
     v_event_queue_subscriber varchar2(100);
     v_job_status             varchar2(100);
     v_nCount                 number;     
     
     bln_enabled              boolean := TRUE;
     const_module   CONSTANT  varchar2(62) := 'ut.create_job';

  begin
    -- ut.log(p_subsystem, 'p_job_name: ' || upper(p_job_name) || ' p_job_action: ' || p_job_action || ' p_job_type: ' || p_job_type || ' p_comments: ' || p_comments,null,null,const_module);

     begin
        DBMS_SCHEDULER.DROP_JOB(job_name => upper(p_job_name));
     exception
        when excep.x_unknown_job then
           null;
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           ut.log(p_subsystem, substr('p_job_name: ' || upper(p_job_name) || ' p_job_action: ' || p_job_action || ' Errors: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     end;

     ut.sleep(p_subsystem,const.k_sleep_seconds); -- prevent contention / dbms_scheduler lock up
     
     if gp.bln_stop_job_overrun
     then bln_enabled := FALSE;
     else bln_enabled := TRUE;
     end if;
     
     while ut.get_all_running_job_cnt(p_subsystem) > gp.get_parallel_job_limit
     loop
        v_nCount := v_nCount + 1;
        if mod(v_nCount,100) = 0
        then
          ut.log(const.k_subsys_subset,'Sleeping: '||const.k_sleep_seconds||': running job count exceeds maximum of '||to_char(gp.get_parallel_job_limit),null,null,const_module);
        end if;
        ut.sleep(const.k_subsys_subset,const.k_sleep_seconds);   
     end loop;  
     
     begin
        DBMS_SCHEDULER.CREATE_JOB (
                      job_name           =>  upper(p_job_name),
                      job_type           =>  p_job_type,
                      job_action         =>  p_job_action,
                      repeat_interval    =>  p_repeat_interval,
                      start_date         =>  SYSDATE,
                      enabled            =>  bln_enabled,
                      comments           =>  p_comments
                   );         
     exception
        when others then
           raise;
     end;

     if bln_enabled then
        ut.sleep(p_subsystem,const.k_sleep_seconds);
        v_job_status := ut.get_job_status(const.k_subsys_subset,upper(p_job_name));
        if v_job_status = 'SCHEDULED' then -- queuing to run
           ut.log(const.k_subsys_subset,p_job_name||' is waiting to run ...',null,null,const_module);
        end if;
     end if;
     
     if gp.bln_stop_job_overrun
     then
        -- set max run time
        dbms_scheduler.set_attribute (name => upper(p_job_name), attribute => 'max_run_duration', value => numtodsinterval(const.k_max_run_duration_mins, 'MINUTE'));
        -- set all events to be raised (for debugging)
        dbms_scheduler.set_attribute(name => upper(p_job_name), attribute => 'raise_events', value => DBMS_SCHEDULER.job_all_events);
        -- start the job
        dbms_scheduler.enable(name => upper(p_job_name));
        ut.sleep(p_subsystem,const.k_sleep_seconds);
        v_job_status := ut.get_job_status(const.k_subsys_subset,upper(p_job_name));
        if v_job_status = 'SCHEDULED' then
           ut.log(const.k_subsys_subset,p_job_name||' is waiting to run ...',null,null,const_module);
        end if;  
  
        v_job_action := 'BEGIN DBMS_SCHEDULER.STOP_JOB(job_name => '||chr(39)||upper(p_job_name)||chr(39)||'); END;';
        v_event_condition := 'tab.user_data.object_name = '||chr(39)||upper(p_job_name)||chr(39)||' and tab.user_data.event_type = ' ||chr(39)||'JOB_OVER_MAX_DUR'||chr(39);
        v_event_queue_subscriber := gp.run_env || '_obfus_jobs_agent';
        v_queue_spec := 'sys.scheduler$_event_queue,' || v_event_queue_subscriber;
        v_job_name := const.k_overrun_job_prefix || upper(p_job_name);
  
        begin
           DBMS_SCHEDULER.DROP_JOB(job_name => v_job_name);
        exception
           when excep.x_unknown_job then
              null;
           when others then
              v_code := SQLCODE;
              v_errm := SUBSTR(SQLERRM, 1 , 4000);
              ut.log(p_subsystem, substr('v_job_name: ' || v_job_name || ' v_job_action: ' || v_job_action || ' Errors: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        end;
  
        ut.sleep(p_subsystem,const.k_sleep_seconds);  -- prevent contention / dbms_scheduler lock up
  
        begin
           dbms_scheduler.create_job( job_name        =>  v_job_name,
                                      job_type        => 'PLSQL_BLOCK',
                                      job_action      =>  v_job_action,
                                      event_condition =>  v_event_condition,
                                      queue_spec      =>  v_queue_spec,
                                      enabled         =>  true);
                                
           ut.sleep(p_subsystem,const.k_sleep_seconds);                            
           v_job_status := ut.get_job_status(const.k_subsys_subset,upper(v_job_name));
           if v_job_status = 'SCHEDULED' then
              ut.log(const.k_subsys_subset,v_job_name||' is waiting to run ...',null,null,const_module);
          end if;                                       
          
        exception
           when others then
              v_code := SQLCODE;
              v_errm := SUBSTR(SQLERRM,1,4000);          
              ut.log(p_subsystem, substr('p_job_name: ' || upper(p_job_name) || ' p_job_action: ' || p_job_action || ' p_job_type: ' || p_job_type || ' p_comments: ' || p_comments || ' Errors: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        end;
     end if; -- gp.bln_stop_job_overrun

  end create_job;


  function create_job( p_subsystem        varchar2,
                       p_job_name         varchar2,
                       p_job_action       varchar2,
                       p_job_type         varchar2  default 'PLSQL_BLOCK',
                       p_repeat_interval  varchar2  default null,
                       p_comments         varchar2  default null )
     return varchar2
  is
     v_code                   number;
     v_errm                   varchar2(4000);
     v_job_name               varchar2(32);

     const_module   CONSTANT  varchar2(62) := 'ut.create_job';

  begin  
  
     v_job_name := create_job (p_subsystem, upper(p_job_name), p_job_action, p_job_type, p_repeat_interval, p_comments);
  
     return v_job_name;

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);       
        ut.log(p_subsystem, 'Function wrapper for create_job: '||substr('p_job_name: ' || upper(p_job_name) || ' p_job_action: ' || p_job_action || ' p_job_type: ' || p_job_type || ' p_comments: ' || p_comments || ' Errors: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);  
  end create_job;
  
  
  procedure create_ss_job ( p_job_name                varchar2,
                            p_job_action              varchar2,
                            p_start_stage_step_code   varchar2  default null,
                            p_end_stage_step_code     varchar2  default null, 
                            p_stmt_seq                number    default 1,
                            p_job_type                varchar2  default 'PLSQL_BLOCK',
                            p_repeat_interval         varchar2  default null,
                            p_comments                varchar2  default null )
  is  
     v_code                   number;
     v_errm                   varchar2(4000);
     v_job_name               varchar2(32);

     const_module   CONSTANT  varchar2(62) := 'ut.create_ss_job';

  begin  
  
     --ut.log(const.k_subsys_subset,'creating job: '||upper(p_job_name),null,null,const_module);   
  
     ut.create_job (const.k_subsys_subset, upper(p_job_name), p_job_action, p_job_type, p_repeat_interval, p_comments);
     ut.sleep(const.k_subsys_subset,const.k_sleep_seconds);
     merge_job_execution(const.k_subsys_subset, gp.get_ss_run_id, p_job_name, gp.get_ss_execution_id, p_start_stage_step_code, p_end_stage_step_code, p_stmt_seq);

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);       
        ut.log(const.k_subsys_subset, substr('Unexpected Errors: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module,p_start_stage_step_code);  
  
  end create_ss_job;
     

  procedure drop_overrun_prevention_jobs(p_subsystem    varchar2) is

    cursor c_overrun_jobs ( cp_subsystem varchar2, cp_run_id number )
    is
      select job_name
        from job_execution
       where subsystem = cp_subsystem
         and run_id = cp_run_id
         and execution_id = gp.ss_execution_id
         and completed_yn = 'Y'
      order by end_timestamp;

    const_module   CONSTANT  varchar2(62) := 'ut.drop_overrun_prevention_jobs';
   
    v_code                   number;
    v_errm                   varchar2(4000);

    v_run_id number;
    v_overrun_job_name       varchar(128);

  begin

    if p_subsystem = const.k_subsys_subset
    then
       v_run_id := gp.ss_run_id;
    elsif p_subsystem = const.k_subsys_obfus
    then
       v_run_id := gp.obfus_run_id;
    end if;

    for r in c_overrun_jobs(p_subsystem,v_run_id)
    loop
       v_overrun_job_name := const.k_overrun_job_prefix || r.job_name;
       ut.log(const.k_subsys_obfus,'Dropping overrun job ' || v_overrun_job_name,null,null,const_module);
       DBMS_SCHEDULER.DROP_JOB (job_name => v_overrun_job_name);
       
       begin
          begin
             DBMS_SCHEDULER.STOP_JOB(job_name => v_overrun_job_name);
          exception
             when excep.x_job_not_running
             then
                DBMS_SCHEDULER.DROP_JOB(job_name => v_overrun_job_name);
          end;
       exception
          when others then
             v_code := SQLCODE;
             v_errm := SUBSTR(SQLERRM,1,4000);
             ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
       end;       
       
    end loop;

  end drop_overrun_prevention_jobs;


  function get_latest_log_id(p_subsystem varchar2, p_stage_step_code varchar2)
     return number
  is
     const_module        CONSTANT  varchar2(62) := 'ut.get_latest_log_id';
     v_log_id            ss_log.log_id%type := 0;
     v_code              number;
     v_errm              varchar2(4000);

  begin

     begin
     
        select log_id
          into v_log_id
          from (
                select log_id
                  from ss_log
                 where stage_step_code = p_stage_step_code
                   and ss_run_id = gp.ss_run_id
                   and execution_id = gp.ss_execution_id 
                order by log_id desc 
        )
        where rownum = 1;  
               
     exception
        when others then
           v_log_id := 0;
     end;
     
     return v_log_id;
  end get_latest_log_id;

  
  
  procedure merge_job_execution( p_subsystem               varchar2,
                                 p_run_id                  number,
                                 p_job_name                varchar2,
                                 p_execution_id            number,
                                 p_start_stage_step_code   varchar2  default null,
                                 p_end_stage_step_code     varchar2  default null,
                                 p_stmt_seq                number    default 1 )
  is
      pragma autonomous_transaction;
      const_module  CONSTANT  varchar2(62) := 'ut.merge_job_execution';
  
      v_code                   number;
      v_errm                   varchar2(4000);  
      
     -- v_job_actual_owner    ss_job_execution.job_actual_owner%type;
     --v_table_owner               dd_tables.owner%type;
     --v_table_actual_owner               dd_tables.actual_owner%type;     
      v_job_name            all_scheduler_jobs.job_name%type;
      v_job_start_date      all_scheduler_jobs.start_date%type;
      v_job_status          all_scheduler_jobs.state%type;
      v_job_run_duration    all_scheduler_job_run_details.run_duration%type;  
      v_completed_yn        job_execution.completed_yn%type;   
      vLogID                ss_log.log_id%type;
           
  begin

     begin
       
        select job_name,job_start_date,status,completed_yn,run_duration --job_actual_owner,
          into v_job_name,v_job_start_date,v_job_status,v_completed_yn,v_job_run_duration --,v_job_actual_owner
          from (
                select nvl(asj.owner,jrd.owner) job_actual_owner,
                       nvl(asj.job_name,jrd.job_name) job_name,
                       nvl(asj.start_date,jrd.actual_start_date) job_start_date,
                       nvl(asj.state,jrd.status) status, 
                       case when nvl(asj.state,jrd.status) in ('RUNNING','SCHEDULED') 
                            then 'N'
                            when nvl(asj.state,jrd.status) is null
                            then 'N'
                            else 'Y'  -- SUCCEEDED, FAILED, STOPPED
                       end completed_yn,
                       jrd.run_duration 
                  from all_scheduler_job_run_details jrd
                     full outer join all_scheduler_jobs asj on asj.job_name = jrd.job_name
                 where ( jrd.job_name = upper(p_job_name) or asj.job_name = upper(p_job_name) )
                  order by nvl(asj.start_date,jrd.actual_start_date) desc  )
         where rownum = 1; --latest matching job by name

      exception
         when no_data_found
         then --x_unknown_job,-27475
            v_code := SQLCODE;
            v_errm := SUBSTR(SQLERRM, 1 , 4000);
            ut.log(const.k_subsys_subset,substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);           
      end;

      merge into job_execution x
      using (select 1 from dual) y
         on (     x.subsystem = p_subsystem
              and x.run_id    = p_run_id
              and x.job_name  = upper(p_job_name)
              and x.execution_id = p_execution_id
          )
          when matched
          then
              update
                 set x.status                = v_job_status,
                     x.completed_yn          = v_completed_yn,
                     x.start_stage_step_code = nvl(p_start_stage_step_code,x.start_stage_step_code),
                     x.end_stage_step_code   = nvl(p_end_stage_step_code,x.end_stage_step_code),
                     x.start_timestamp       = v_job_start_date,
                     x.run_duration          = v_job_run_duration
                     --x.end_timestamp         = nvl(x.end_timestamp,p_end_timestamp)                    
          when not matched
          then
              insert (subsystem,run_id,job_name,start_stage_step_code,end_stage_step_code,stmt_seq,execution_id,start_timestamp,run_duration,status,completed_yn)
              values (p_subsystem,p_run_id,upper(p_job_name),p_start_stage_step_code,p_end_stage_step_code,p_stmt_seq,p_execution_id,v_job_start_date,v_job_run_duration,v_job_status,v_completed_yn);
      commit;
      
  end merge_job_execution;
  

  procedure gather_partition_stats(p_part_list varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'ut.gather_partition_stats';

     cursor c_schemas
     is
        select dest_schema
          from ss_schema_list
        order by cre_seq;
  begin

     for r in c_schemas
     loop
        ut.log(const.k_subsys_subset,'Calling gathering partition_stats for dest_schema: '||r.dest_schema||' with p_part_list: '||p_part_list,null,null,const_module);
        ut.gather_partition_stats(r.dest_schema, p_part_list);
     end loop;

     load_dd_stats(const.k_subsys_subset);

  end gather_partition_stats;


  procedure gather_partition_stats(p_owner varchar2, p_part_list varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'ut.gather_partition_stats';

     cursor c_tab_partitions(cp_owner varchar2, cp_part_list varchar2)
     is
        with ptn as(SELECT REGEXP_SUBSTR (cp_part_list,
                                      '[^,]+',
                                      1,
                                      LEVEL)
                          AS partition_name
                  FROM dual
            CONNECT BY REGEXP_SUBSTR (cp_part_list,
                                      '[^,]+',
                                      1,
                                      LEVEL)
                          IS NOT NULL)
        select tp.table_owner, tp.table_name, tp.partition_name
          from md_ddl md
          join dba_tab_partitions tp on md.actual_owner = tp.table_owner and md.object_name = tp.table_name
           join ptn on ptn.partition_name = tp.partition_name
          where md.actual_owner = cp_owner
            and md.object_type = 'TABLE'
            and md.partitioning_type is not null;
  begin

     for r in c_tab_partitions(p_owner, p_part_list||','||'NULL_COMP_CODE')
     loop
        ut.log(const.k_subsys_subset,'gathering partition_stats for: '||r.table_owner||'.'||r.table_name||'.'||r.partition_name,null,null,const_module);
        dbms_stats.gather_table_stats(OWNNAME=>r.table_owner,TABNAME=>r.table_name, PARTNAME=>r.partition_name, CASCADE=> TRUE);
     end loop;

  end gather_partition_stats;

  PROCEDURE drop_r_cons (p_subsystem            varchar2,
                         p_actual_r_cons_owner  varchar2  default null,
                         p_r_cons_name          varchar2  default null) is

    cursor get_ref_cons(p_table_owner varchar2,p_cons_name varchar2)  is
       select c.actual_owner,c.table_name,c.constraint_name
         from dd_constraints c
        where c.constraint_type = 'R'
          and c.actual_owner = nvl(p_actual_r_cons_owner,c.actual_owner)
          and c.actual_owner like gp.tgt_prefix ||'\_'||'%' escape '\'
          and c.constraint_name = nvl(p_cons_name,c.constraint_name)
         --ORA-14650: operation not supported for reference-partitioned tables
          and c.constraint_name not in ( select ref_ptn_constraint_name 
                                           from md_ddl 
                                          where object_type = 'PARTITION'
                                            and ref_ptn_constraint_name is not null );

     const_module        CONSTANT  varchar2(62) := 'ut.drop_r_cons';
     v_code              number;
     v_errm              varchar2(4000);
     v_ddl               varchar2(4000);
     v_r_cons_count      number := 0;
  begin

    -- dd_constraints must be refreshed
    ut.load_dd_constraints(p_subsystem);

    for r in get_ref_cons(p_actual_r_cons_owner,p_r_cons_name) loop
       begin
          v_ddl := 'alter table '||r.actual_owner||'.'||r.table_name||' drop constraint '||r.constraint_name;
          ut.log(p_subsystem,'Executing: ' || v_ddl,null,null,const_module);
          execute immediate v_ddl;
          v_r_cons_count := v_r_cons_count +1;
       exception 
          when excep.x_not_supported_for_ref_part_tabs
          then
             null;
          when others then
             v_code := SQLCODE;
             v_errm := SUBSTR(SQLERRM,1,4000);
             ut.log(p_subsystem,'Error executing: ' || v_ddl,v_code,v_errm,const_module);
       end;
    end loop;

    ut.log(p_subsystem,'Dropped '||v_r_cons_count||' REF CONSTRAINTS',null,null,const_module);
  end drop_r_cons;
  
  
procedure ins_add_partition_log(
      p_actual_owner             varchar2,
      p_table_name               varchar2,
      p_exec_order               varchar2,
      p_success_add_order        varchar2 default null,
      p_partition_fk             varchar2 default null,
      p_parent_owner             varchar2 default null,
      p_parent_table             varchar2 default null,
      p_cross_schema_relation    varchar2 default null,
      p_errm                     varchar2 default null,
      p_err_code                 number   default null
  )
  is
      const_module        CONSTANT  varchar2(62) := 'ut.ins_add_partition_log';
  begin
    insert into ss_add_partition_log (
        ss_run_id,
        actual_owner,
        owner,
        table_name,
        exec_order,
        success_add_order,
        partition_fk,
        parent_owner,
        parent_table,
        cross_schema_relation,
        errm,
        err_code
      )
      values
      (
        gp.ss_run_id,
        p_actual_owner,
        replace(p_actual_owner,gp.tgt_prefix||'_',null),
        p_table_name,
        p_exec_order,
        p_success_add_order,
        p_partition_fk,
        p_parent_owner,
        p_parent_table,
        p_cross_schema_relation,
        p_errm,
        p_err_code
      );
  end ins_add_partition_log;
  
  
 procedure ins_add_partition_recursive(p_owner varchar2, p_table_name varchar2, p_part_ddl varchar2)
 is

     const_module        CONSTANT  varchar2(62) := 'ut.ins_add_partition_recursive';

     cursor c_parent_tables (cp_owner varchar2, cp_table_name varchar2)
     is
        select c2.owner,c2.table_name,smp.partition_fk fk,c2.constraint_name pk,smp.part_ddl
          from ss_missing_parts smp 
          join dd_constraints c2 on c2.constraint_name  = smp.partition_fk and  c2.constraint_type = 'P'
          where smp.owner = cp_owner
          and smp.table_name = cp_table_name;
           

         v_ddl       varchar2(1000);
         v_code      number;
         v_errm      varchar2(4000);
    
         v_owner        varchar2(4000);
         v_table_name   varchar2(4000);
         v_fk           varchar2(4000);
         v_pk           varchar2(4000);
         v_part_ddl     varchar2(4000);

  begin

     ut.log(const.k_subsys_subset,'executing '||p_part_ddl,null,null,const_module);

     begin
       execute immediate p_part_ddl;
       ut.ins_add_partition_log (     p_actual_owner          => p_owner,
                                      p_table_name            => p_table_name,
                                      p_exec_order            => ss_exec_ins_part_seq.nextval,
                                      p_success_add_order     => ss_success_ins_part_order_seq.nextval );
     exception
       when excep.x_table_not_exist then
         ut.log(const.k_subsys_subset,p_owner||'.'||p_table_name||' does not exist.',null,null,const_module);

       when excep.x_parent_not_part then       
         if c_parent_tables%ISOPEN then close c_parent_tables; end if;
         open c_parent_tables(p_owner,p_table_name);

         loop

           fetch c_parent_tables into v_owner,v_table_name,v_fk,v_pk,v_part_ddl;
           exit when c_parent_tables%NOTFOUND;
           if p_owner <> v_owner then  -- Special log to indicate fK crossing schemas
             ut.log(const.k_subsys_subset,'Cross schema fK relationship found: '||p_owner||'.'||p_table_name||' fK '||v_fK||' REFERENCES '||v_owner||'.'||v_table_name||' pK '||v_pK,null,null,const_module);

             ut.ins_add_partition_log ( p_actual_owner              => p_owner,
                                            p_table_name            => p_table_name,
                                            p_exec_order            => ss_cre_part_order_seq.nextval,
                                            p_partition_fk          => v_fK,
                                            p_parent_owner          => v_owner,
                                            p_parent_table          => v_table_name,
                                            p_cross_schema_relation => p_owner||'.'||p_table_name||' '||v_fK||' REFERENCES '||v_owner||'.'||v_table_name||' '||v_pK,
                                            p_errm                  => SQLERRM,
                                            p_err_code              => SQLCODE );
                                 
           end if;

           ut.log(const.k_subsys_subset,'calling ins_add_partition_recursive with '||v_owner||','||v_table_name,null,null,const_module);
           ut.ins_add_partition_recursive(v_owner,v_table_name,v_part_ddl);

         end loop;
         if c_parent_tables%ISOPEN then close c_parent_tables; end if;

       when others then
          v_errm := SUBSTR(SQLERRM,1,4000);
          v_code := SQLCODE;
          ut.log(const.k_subsys_subset,'Error converting to partition table partition '||p_owner||'.'||p_table_name,v_code,v_errm,const_module);
          ut.ins_add_partition_log ( p_actual_owner          => p_owner,
                                         p_table_name            => p_table_name,
                                         p_exec_order            => ss_exec_drop_order_seq.nextval,
                                         p_errm                  => SQLERRM,
                                         p_err_code              => SQLCODE );
       end;
       commit;
  end ins_add_partition_recursive;
  
  procedure ins_add_partition( p_subsystem in varchar2 default 'SUBSETTING' ) is
     
     const_module        CONSTANT  varchar2(62) := 'ut.ins_add_partition';

     cursor c_ref_part_tabs
     is
        select smp.owner, smp.table_name,smp.part_ddl
        from ss_missing_parts smp
        order by obj_cre_seq;

     v_code              number;
     v_errm              varchar2(4000);
     v_failure_count     number(6) := 0;

  begin

    for r in c_ref_part_tabs
    loop
       begin
          ut.ins_add_partition_recursive(r.owner,r.table_name,r.part_ddl);
       exception
          when others then
             v_errm := SUBSTR(SQLERRM,1,4000);
             v_code := SQLCODE;
             v_failure_count := v_failure_count+1;

             ut.log(const.k_subsys_subset,'Error v_failure_count: '||v_failure_count||' continuing with next iteration',v_code,v_errm,const_module);
             continue;
       end;
    end loop;
   end ins_add_partition;
   
   procedure ins_add_partition_main( p_subsystem in varchar2 default 'SUBSETTING' ) is

     const_module        CONSTANT  varchar2(62) := 'ut.ins_add_partition_main';

  begin
     delete ss_add_partition_log where ss_run_id = gp.ss_run_id;
     ut.log(const.k_subsys_obfus,to_char(sql%rowcount) || ' rows deleted from ss_add_partition_log for ss_run_id: ' || gp.ss_run_id,null,null,const_module);

     ut.reset_sequence(const.k_subsys_subset,'ss_exec_ins_part_seq');
     ut.reset_sequence(const.k_subsys_subset,'ss_success_ins_part_order_seq');

     for x in 1..const.k_num_seperate_recursive_paths
     loop
        ut.log(const.k_subsys_subset,'iteration '||to_char(x)||' of ut.ins_add_partition starting.',null,null,const_module);
        ut.ins_add_partition;
     end loop;

  null;

  end ins_add_partition_main;


  PROCEDURE ins_drop_ref_part_tab_log(
      p_actual_owner             varchar2,
      p_table_name               varchar2,
      p_exec_order               varchar2,
      p_success_drop_order       varchar2 default null,
      p_cross_schema_fk          varchar2 default null,
      p_parent_owner             varchar2 default null,
      p_parent_table             varchar2 default null,
      p_cross_schema_relation    varchar2 default null,
      p_errm                     varchar2 default null,
      p_err_code                 number   default null
  )
  IS
      const_module        CONSTANT  varchar2(62) := 'ut.ins_drop_ref_part_tab_log';
  BEGIN
    insert into ss_drop_ref_part_tab_log (
        ss_run_id,
        actual_owner,
        owner,
        table_name,
        exec_order,
        success_drop_order,
        cross_schema_fk,
        parent_owner,
        parent_table,
        cross_schema_relation,
        errm,
        err_code
      )
      values
      (
        gp.ss_run_id,
        p_actual_owner,
        replace(p_actual_owner,gp.tgt_prefix||'_',null),
        p_table_name,
        p_exec_order,
        p_success_drop_order,
        p_cross_schema_fk,
        p_parent_owner,
        p_parent_table,
        p_cross_schema_relation,
        p_errm,
        p_err_code
      );
  END ins_drop_ref_part_tab_log;


  procedure drop_ref_part_tab_recursive (p_owner varchar2, p_table_name varchar2)
  is

     const_module        CONSTANT  varchar2(62) := 'ut.drop_ref_part_tab_recursive';

     cursor c_parent_tables (cp_owner varchar2, cp_table_name varchar2)
     is
        select c2.owner,c2.table_name,c1.constraint_name fk,c2.constraint_name pk
          from dba_constraints c1 join dba_constraints c2 on c1.r_constraint_name = c2.constraint_name
         where c1.owner = cp_owner
           and c1.owner like gp.tgt_prefix ||'\_'||'%' escape '\'
           and c2.owner like gp.tgt_prefix ||'\_'||'%' escape '\'
           and c1.constraint_type = 'R'
           and c2.constraint_type = 'P'
           and c1.table_name = cp_table_name;

     v_ddl       varchar2(1000);
     v_code      number;
     v_errm      varchar2(4000);

     v_owner                varchar2(128);
     v_table_name           varchar2(128);
     v_previous_table_name  varchar2(128);
     v_fk                   varchar2(128);
     v_pk                   varchar2(128);

  begin

     v_ddl := 'DROP TABLE '||p_owner||'.'||p_table_name;
     ut.log(const.k_subsys_subset,'executing '||v_ddl,null,null,const_module);

     begin
       execute immediate v_ddl;
       ut.ins_drop_ref_part_tab_log ( p_actual_owner          => p_owner,
                                      p_table_name            => p_table_name,
                                      p_exec_order            => ss_exec_drop_order_seq.nextval,
                                      p_success_drop_order    => ss_success_drop_order_seq.nextval );
     exception
       when excep.x_table_not_exist then
         ut.log(const.k_subsys_subset,p_owner||'.'||p_table_name||' does not exist.',null,null,const_module);

       when excep.x_uks_in_tab_ref_fk then
         if c_parent_tables%ISOPEN then close c_parent_tables; end if;
         open c_parent_tables(p_owner,p_table_name);

         loop

           fetch c_parent_tables into v_owner,v_table_name,v_fk,v_pk;
           exit when c_parent_tables%NOTFOUND OR v_table_name = v_previous_table_name;
           
           if p_owner <> v_owner then  -- Special log to indicate fK crossing schemas
             ut.log(const.k_subsys_subset,'Cross schema fK relationship found: '||p_owner||'.'||p_table_name||' fK '||v_fK||' REFERENCES '||v_owner||'.'||v_table_name||' pK '||v_pK,null,null,const_module);

             ut.ins_drop_ref_part_tab_log ( p_actual_owner          => p_owner,
                                            p_table_name            => p_table_name,
                                            p_exec_order            => ss_exec_drop_order_seq.nextval,
                                            p_cross_schema_fk       => v_fK,
                                            p_parent_owner          => v_owner,
                                            p_parent_table          => v_table_name,
                                            p_cross_schema_relation => p_owner||'.'||p_table_name||' '||v_fK||' REFERENCES '||v_owner||'.'||v_table_name||' '||v_pK,
                                            p_errm                  => SQLERRM,
                                            p_err_code              => SQLCODE );
           end if;

           ut.log(const.k_subsys_subset,'recursively calling drop_ref_part_tab_recursive with '||v_owner||','||v_table_name,null,null,const_module);
           v_previous_table_name := v_table_name;
           ut.drop_ref_part_tab_recursive(v_owner,v_table_name);

         end loop;
         if c_parent_tables%ISOPEN then close c_parent_tables; end if;

       when others then
          v_errm := SUBSTR(SQLERRM,1,4000);
          v_code := SQLCODE;
          ut.log(const.k_subsys_subset,'Error dropping table '||p_owner||'.'||p_table_name,v_code,v_errm,const_module);
          ut.ins_drop_ref_part_tab_log ( p_actual_owner          => p_owner,
                                         p_table_name            => p_table_name,
                                         p_exec_order            => ss_exec_drop_order_seq.nextval,
                                         p_errm                  => SQLERRM,
                                         p_err_code              => SQLCODE );
       end;
       commit;
  end drop_ref_part_tab_recursive;


  procedure drop_ref_part_tabs( p_subsystem in varchar2 default 'SUBSETTING' )
  is

     const_module        CONSTANT  varchar2(62) := 'ut.drop_ref_part_tabs';

     cursor c_ref_part_tabs
     is
        select sl.dest_schema, md.object_name
          from md_ddl md
          join ss_schema_list sl on replace(md.actual_owner,gp.src_prefix,gp.tgt_prefix) = sl.dest_schema
          join dba_tables dt on dt.owner = sl.dest_schema and dt.table_name = md.object_name
         where md.object_type = 'TABLE'
           and md.partitioning_type = 'REFERENCE'
           and sl.ss_run_id = gp.get_ss_run_id
       order by actual_owner, md.relational_level desc nulls last;

     v_code              number;
     v_errm              varchar2(4000);
     v_failure_count     number(6) := 0;

  begin

    for r in c_ref_part_tabs
    loop
       begin
          ut.drop_ref_part_tab_recursive(r.dest_schema,r.object_name);
       exception
          when others then
             v_errm := SUBSTR(SQLERRM,1,4000);
             v_code := SQLCODE;
             v_failure_count := v_failure_count+1;

             ut.log(const.k_subsys_subset,'Error v_failure_count: '||v_failure_count||' continuing with next iteration',v_code,v_errm,const_module);
             continue;
       end;
    end loop;

  end drop_ref_part_tabs;


  procedure drop_ref_part_tabs_main( p_subsystem in varchar2 default 'SUBSETTING' )
  is
     const_module        CONSTANT  varchar2(62) := 'ut.drop_ref_part_tabs_main';

  begin
     delete ss_drop_ref_part_tab_log where ss_run_id = gp.ss_run_id;
     ut.log(const.k_subsys_obfus,to_char(sql%rowcount) || ' rows deleted from ss_drop_ref_part_tab_log for ss_run_id: ' || gp.ss_run_id,null,null,const_module);

     ut.reset_sequence(const.k_subsys_subset,'ss_success_drop_order_seq');
     ut.reset_sequence(const.k_subsys_subset,'ss_exec_drop_order_seq');
     ut.drop_r_cons(p_subsystem);

     for x in 1..const.k_num_seperate_recursive_paths
     loop
        ut.log(const.k_subsys_subset,'iteration '||to_char(x)||' of ut.drop_ref_part_tabs starting.',null,null,const_module);
        ut.drop_ref_part_tabs;
     end loop;

  end drop_ref_part_tabs_main;


  PROCEDURE disable_enable_r_cons (p_subsystem            varchar2,
                                   p_disable_enable       varchar2,
                                   p_actual_r_cons_owner  varchar2  default null,
                                   p_r_cons_name          varchar2  default null) is

    cursor get_ref_cons(p_table_owner varchar2,p_cons_name varchar2)  is
	   select actual_owner,table_name,constraint_name
         from dd_constraints
        where constraint_type = 'R'
          and actual_owner = nvl(p_actual_r_cons_owner,actual_owner)
          and constraint_name = nvl(p_cons_name,constraint_name)
          and constraint_name not in ( select ref_ptn_constraint_name 
                                         from md_ddl 
                                        where object_type = 'PARTITION'
                                          and ref_ptn_constraint_name is not null );

     const_module        CONSTANT  varchar2(62) := 'ut.disable_enable_r_cons';
     v_code              number;
     v_errm              varchar2(4000);
     v_ddl               varchar2(4000);
     v_r_cons_count      number := 0;
  begin

    -- dd_constraints must be refreshed
    ut.load_dd_constraints(p_subsystem);

    for r in get_ref_cons(p_actual_r_cons_owner,p_r_cons_name) loop
       begin
          v_ddl := 'alter table '||r.actual_owner||'.'||r.table_name||' '||p_disable_enable||' constraint '||r.constraint_name;
          ut.log(p_subsystem,'Executing: ' || v_ddl,null,null,const_module);
          execute immediate v_ddl;
          v_r_cons_count := v_r_cons_count +1;
       exception when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(p_subsystem,'Error executing: ' || v_ddl,v_code,v_errm,const_module);
       end;
    end loop;

    ut.log(p_subsystem,p_disable_enable||'D '||v_r_cons_count||' REF CONSTRAINTS',null,null,const_module);
  end disable_enable_r_cons;

  procedure disable_r_cons(p_subsystem varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'ut.disable_r_cons';
  begin
     disable_enable_r_cons(p_subsystem,'DISABLE');
  end disable_r_cons;

  procedure enable_r_cons(p_subsystem  varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'ut.enable_r_cons';
  begin
     disable_enable_r_cons(p_subsystem,'ENABLE');
  end enable_r_cons;

  procedure disable_enable_triggers (p_subsystem       varchar2,
                                     p_disable_enable  varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'ut.disable_enable_triggers';
     v_code              number;
     v_errm              varchar2(4000);
     v_ddl               varchar2(4000);
     v_trigger_count     number := 0;

     cursor c_get_triggers
     is
       select owner,trigger_name
         from all_triggers
        where table_owner like gp.tgt_prefix ||'\_'||'%' escape '\';

  begin

      ut.log(p_subsystem,'opening get_triggers cursor for '||gp.tgt_prefix,null,null,const_module);
      for get_triggers_rec in c_get_triggers
      loop
        begin
           v_ddl := 'alter trigger  '||get_triggers_rec.owner||'.'||get_triggers_rec.trigger_name||' '||p_disable_enable;
           ut.log(p_subsystem,'Executing: '||v_ddl,null,null,const_module);
           execute immediate v_ddl;
           v_trigger_count := v_trigger_count + 1;
        exception when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM,1,4000);
           ut.log(p_subsystem,'Error executing: ' || v_ddl,v_code,v_errm,const_module);
           raise;
        end;
      end loop;

      ut.log(p_subsystem,p_disable_enable||'D '||v_trigger_count||' triggers',null,null,const_module);

  end disable_enable_triggers;

  procedure disable_triggers(p_subsystem  varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'ut.disable_triggers';
  begin
     disable_enable_triggers(p_subsystem, 'DISABLE');
  end disable_triggers;

  procedure enable_triggers(p_subsystem  varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'ut.enable_triggers';
  begin
     disable_enable_triggers(p_subsystem, 'ENABLE');
  end enable_triggers;

  procedure recompile(p_subsystem  varchar2)
  is
    v_code        number;
    v_errm        varchar2(4000);
    const_module  CONSTANT  varchar2(62) := 'ut.recompile';
  begin
    ut.log(const.k_subsys_subset,'executing SYS.UTL_RECOMP.RECOMP_PARALLEL',null,null,const_module);
    SYS.UTL_RECOMP.RECOMP_PARALLEL(const.k_max_parallel_jobs,null,null);
    ut.load_dd_objects(const.k_subsys_subset);   
    ss_reporting.invalid_objects_delta_report;        
    ss_reporting.invalid_objects_report;
  exception
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(p_subsystem,substr('Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      raise;
  end recompile;


  procedure reset_sequence (p_subsystem varchar2, p_seq_name varchar2, p_reset_to number default 1)
  as
    const_module        CONSTANT  varchar2(62) := 'ut.reset_sequence';
    v_nextval           number;
    v_sql_stmt          varchar2(4000);
    v_seq_name          varchar2(30);
    v_code              number;
    v_errm              varchar2(4000);

  begin
     v_seq_name := p_seq_name;
     begin
        v_sql_stmt := 'select ' || v_seq_name || '.NEXTVAL from dual';
        execute immediate v_sql_stmt into v_nextval;

        if v_nextval > 1 then
          v_sql_stmt := 'ALTER SEQUENCE ' || v_seq_name || ' INCREMENT BY ' || -(v_nextval-1);
          execute immediate v_sql_stmt;

          v_sql_stmt := 'select ' || v_seq_name || '.NEXTVAL from dual';
          execute immediate v_sql_stmt into v_nextval;
        end if;

        v_sql_stmt := 'ALTER SEQUENCE ' || v_seq_name || ' INCREMENT BY 1';
        execute immediate v_sql_stmt;

     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM,1,4000);
           ut.log(p_subsystem,substr('p_seq_name: '||p_seq_name||' Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     end;
  end reset_sequence;

  function get_tgt_psm_session_count(p_subsystem varchar2, p_tgt_prefix varchar2)
     return number
  is
     cursor c_sessions(cp_tgt_prefix varchar2)
     is
        select sid,serial#,username,status,osuser,machine
          from v$session
         where schemaname like cp_tgt_prefix||'\_%' escape '\';

    const_module        CONSTANT  varchar2(62) := 'ut.get_tgt_psm_session_count';
    v_session_count     number := 0;
    v_code              number;
    v_errm              varchar2(4000);

  begin
    for r in c_sessions(p_tgt_prefix)
    loop
       ut.log(p_subsystem,r.status||' '||r.username||' session found for osuser: '||r.osuser|| ' on machine '||r.machine||' with sid: '||r.sid||' serial#: '||r.serial#,null,null,const_module);
       v_session_count := v_session_count + 1;
    end loop;
    return v_session_count;
  end get_tgt_psm_session_count;


  procedure validate_db_link
  is
     const_module  CONSTANT  varchar2(62) := 'ut.validate_db_link';
      
     v_code           number;
     v_errm           varchar2(4000);
  
     v_valid_db_link     number(1,0);
     v_db_link_username  varchar2(128);
     v_db_link_ddl       varchar2(1000);
     
  begin
  
     gp.set_src_psm_password;
     gp.set_src_psm_hostname;
     gp.set_src_psm_servicename;
  
     ut.log(const.k_subsys_subset,'validating db_link: '||gp.get_ss_db_link||' to '||gp.get_src_prefix||'_PRISM_UTILITIES',null,null,const_module);
  
     begin
        select 1
          into v_valid_db_link
          from dba_db_links 
         where db_link = gp.get_ss_db_link
           and username = gp.get_src_prefix||'_PRISM_UTILITIES'
           and owner = 'PUBLIC'; 
           
        ut.log(const.k_subsys_subset,'valid db_link '||gp.get_ss_db_link||' found for '||gp.get_src_prefix||'_PRISM_UTILITIES',null,null,const_module);
     
     exception
        when no_data_found then
  
           -- if exists for different source
           begin
              select username
                into v_db_link_username
                from dba_db_links 
               where db_link = gp.get_ss_db_link;
           
              if v_db_link_username is not null then
                 ut.log(const.k_subsys_subset,'Dropping db_link '||gp.get_ss_db_link||' for '||v_db_link_username,null,null,const_module);
                 execute immediate 'DROP PUBLIC DATABASE LINK ' ||gp.get_ss_db_link;
              end if;
              
           exception
              when others then
                 v_code := SQLCODE;
                 v_errm := SUBSTR(SQLERRM,1,4000);
                 ut.log(const.k_subsys_subset,'DB Link may not exist to drop',v_code,v_errm,const_module);
           end;
           
           v_db_link_ddl := 'CREATE PUBLIC DATABASE LINK ' ||gp.get_ss_db_link ||
                            ' CONNECT TO '||gp.get_src_prefix||'_PRISM_UTILITIES IDENTIFIED BY '||gp.get_src_psm_password||
                            ' USING '||chr(39)||gp.get_src_psm_hostname||':1521/'||gp.get_src_psm_servicename||chr(39);  
  
           ut.log(const.k_subsys_subset,'validating db_link not found so re-creating with ddl: '||v_db_link_ddl,null,null,const_module);
           execute immediate v_db_link_ddl;
     end;
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,'Unexpected error ',v_code,v_errm,const_module);
  end validate_db_link;


  procedure create_src_synonyms
  is
     const_module  CONSTANT   varchar2(62) := 'ut.create_src_synonyms';
     
     v_code              number;
     v_errm              varchar2(4000); 
    
     cursor c_src_synonyms
     is    
        select 'create or replace synonym SRC_'||table_name||' for '||gp.get_src_prefix||'_PRISM_UTILITIES.'||table_name||'@SRC_LINK' stmt
          from dd_tables@SRC_LINK dd
         where ( table_name like 'DD\_%' escape '\'
              or table_name like 'MD\_%' escape '\'
              or table_name like 'SS\_%' escape '\' )
        union
        select 'create or replace synonym SRC_'||view_name||' for '||gp.get_src_prefix||'_PRISM_UTILITIES.'||view_name||'@SRC_LINK' stmt
          from dd_views@SRC_LINK dd
         where view_name like 'VW\_%' escape '\';    
    
    
  begin
     for r in c_src_synonyms
     loop  
        ut.log(const.k_subsys_subset,'Creating SYNONYM '||r.stmt,null,null,const_module);
        execute immediate r.stmt;
     end loop;
  end create_src_synonyms;


  procedure run_remote_md_utilities_job ( p_job_name        varchar2,
                                          p_job_action      varchar2,
                                          p_repeat_interval varchar2  default null,
                                          p_start_date      timestamp default null )
  is                                           
     const_module  CONSTANT   varchar2(62) := 'ut.run_remote_md_utilities_job';
     
     v_code              number;
     v_errm              varchar2(4000);     
     
     v_job_status             varchar2(30);
     v_job_errors             varchar2(4000);
     v_seconds_to_wait        integer;

  begin
     
     ut.log(const.k_subsys_subset,'Invoking remote dbms_scheduler job '||p_job_name||' p_job_action: '||p_job_action||' across db_link '||gp.ss_db_link,null,null,const_module);
       
     metadata_utilities.create_job@SRC_LINK( p_job_name,p_job_action,null,p_start_date );

     if ( p_start_date is not null and p_start_date > SYSDATE ) 
     then
       v_seconds_to_wait := round(to_number((p_start_date - SYSDATE)*24*60*60));
       ut.sleep(const.k_subsys_subset,v_seconds_to_wait);
     end if;
     
     loop
     
        v_job_status := metadata_utilities.get_job_status@SRC_LINK(p_job_name);
        
        case v_job_status
           when 'RUNNING'
           then 
		      ut.log(const.k_subsys_subset,'job '||p_job_name||' is running: see src_util_log for progress.',null,null,const_module);
			  ut.sleep(const.k_subsys_subset,30);
			  
           when 'SUCCEEDED'
           then      
              ut.log(const.k_subsys_subset,'job '||p_job_name||' has SUCCEEDED',null,null,const_module);
              exit;
			  
           when 'FAILED'
           then
              v_job_errors := metadata_utilities.get_job_errors@SRC_LINK(p_job_name);
              ut.log(const.k_subsys_subset,substr('job '||p_job_name||' has failed with errors: '||v_job_errors,1,4000),null,null,const_module); 
              raise excep.x_job_failure;
			  
           when 'STOPPED'
           then
              ut.log(const.k_subsys_subset,'gen_metadata.load_metadata job '||p_job_name||' has stopped.',null,null,const_module); 
              raise excep.x_job_stopped;  

           when 'SCHEDULED'
           then
              ut.log(const.k_subsys_subset,'job '||p_job_name||' created scheduled, so waiting to run...',null,null,const_module); 
              continue; 
			          
           else
              raise excep.x_unknown_job_status;
        end case;
     end loop;
     
  exception
     when excep.x_job_failure
     then
        raise_application_error( -20007, v_job_errors);
        
     when excep.x_job_stopped
     then
        raise_application_error( -20011, p_job_name);
        
     when excep.x_unknown_job_status
     then
        raise_application_error( -20012, v_job_status);     
        
     when others then
        raise;
        
  end run_remote_md_utilities_job;  
  

  procedure gen_src_metadata( p_comp_list       varchar2,  
                              p_job_start_time  timestamp default null)
  is
     const_module  CONSTANT   varchar2(62) := 'ut.gen_src_metadata';
     
     v_code              number;
     v_errm              varchar2(4000);     
     
     v_job_name               varchar2(32);
     v_job_action             varchar2(4000);

  begin
     
     ut.log(const.k_subsys_subset,'Invoking remote dbms_scheduler job to generate source metadata for companies '||p_comp_list||' across db_link '||gp.ss_db_link,null,null,const_module);

     v_job_name   := substr('GEN_METADATA_'||replace(p_comp_list,',','_'),1,32); 

     v_job_action := 'BEGIN  gen_metadata.load_metadata(TRUE,'||chr(39)||p_comp_list||chr(39)||');  END;';                   
        
     ut.run_remote_md_utilities_job( v_job_name,v_job_action,null,p_job_start_time );   
    
     create_src_synonyms;
     
  exception   
        
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,substr('Unexpected Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        
  end gen_src_metadata;   


  function fn_get_part_list return varchar2
  is
     const_module  CONSTANT   varchar2(62) := 'ut.fn_get_part_list';
     
     v_code              number;
     v_errm              varchar2(4000);   
     v_part_list         varchar2(32000);      
     
  begin
  
     begin
        select replace(listagg (partition_name,',') within group (order by 1),'NULL_COMP_CODE,',null) comp_list 
          into v_part_list
          from ss_companies 
         where ss_run_id = gp.ss_run_id;
     exception
        when no_data_found then
           v_part_list := null;
     end;
     
     return v_part_list;
     
  end fn_get_part_list;


  procedure build_all_load_views
  is
     const_module  CONSTANT   varchar2(62) := 'ut.build_all_load_views';
     
     v_code              number;
     v_errm              varchar2(4000);     
     v_part_list         varchar2(32000);

  begin
     
     ut.log(const.k_subsys_subset,'Invoking gen_metadata.build_all_load_views@SRC_LINK('||v_part_list||')',null,null,const_module);

     v_part_list := fn_get_part_list;

     gen_metadata.build_all_load_views@SRC_LINK(v_part_list);

  exception   
        
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,substr('Unexpected Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        
  end build_all_load_views;  


  function ins_md_ddl (p_actual_owner varchar2, p_object_type varchar2, p_object_name varchar2 )
     return number
  is
     const_module  CONSTANT   varchar2(62) := 'ut.ins_md_ddl';
     
     v_code              number;
     v_errm              varchar2(4000);       
  
     v_md_ddl_id         md_ddl.md_ddl_id%type;
  
  begin
  
      delete md_ddl where actual_owner = p_actual_owner and object_type = p_object_type and object_name = p_object_name;  
      ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' rows from md_ddl for '||p_actual_owner||' object_type '||p_object_type||': '||p_object_name,null,null,const_module);  
  
      insert into md_ddl (md_ddl_id,
                          actual_owner,
                          owner,
                          object_type,
                          object_name,
                          base_object_name,
                          object_ddl,
                          object_ddl_length,
                          object_xml,
                          object_cre_seq,
                          relational_level,
                          partitioning_type,
                          subpartitioning_type,
                          ref_ptn_constraint_name,
                          view_name,
                          has_large_object,
                          created_ts,
                          modified_ts,
                          dp_yn)
                   select md_ddl_id,
                          actual_owner,
                          owner,
                          object_type,
                          object_name,
                          base_object_name,
                          object_ddl,
                          object_ddl_length,
                          object_xml,
                          object_cre_seq,
                          relational_level,                      
                          partitioning_type,
                          subpartitioning_type,
                          ref_ptn_constraint_name,
                          view_name,
                          has_large_object,
                          created_ts,
                          modified_ts,
                          dp_yn
                     from src_md_ddl
                    where actual_owner = p_actual_owner
                      and object_type  = p_object_type 
                      and object_name  = p_object_name;
                    

     ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' rows into md_ddl from src_md_ddl for '||p_actual_owner||' object_type '||p_object_type||': '||p_object_name,null,null,const_module);
     commit;

     begin
       select md_ddl_id 
         into v_md_ddl_id
         from md_ddl
        where actual_owner = p_actual_owner
          and object_type  = p_object_type 
          and object_name  = p_object_name;   
     exception
        when too_many_rows then
           v_md_ddl_id := -1;
     end;   

     return v_md_ddl_id;
     
  end ins_md_ddl;
  

  function fn_build_regexp_match_str (p_prefix in varchar2)
    return varchar2
  is
    const_module        CONSTANT  varchar2(62) := 'ut.fn_build_regexp_match_str';   
  
    v_char           varchar2(128);
    v_previous_char  varchar2(128);
    v_regexp_str     varchar2(128);
    v_instances      number(2,0) := 0;
  begin
  
    for r in 1..length(p_prefix)
    loop
       v_char := substr(p_prefix,r,1); 
       --dbms_output.put_line('v_char:' ||v_char);
    
       if v_previous_char is not null
       then
          if (v_char = v_previous_char) 
          then
             v_instances := v_instances + 1;
          else   
             v_instances := v_instances + 1;
             v_regexp_str := v_regexp_str || v_previous_char || '{'||v_instances||'}';
             v_instances := 0;
          end if;   
       end if;
          
       v_previous_char := v_char;
       --dbms_output.put_line('v_previous_char:' ||v_previous_char);
    end loop;
    v_instances := v_instances + 1;
    --dbms_output.put_line('reg:' ||v_regexp_str);
    return v_regexp_str || v_char || '{'||v_instances||'}';
  end fn_build_regexp_match_str;  
  

  procedure replace_md_ddl_actual_owner (p_src_prefix varchar2, p_tgt_prefix varchar2)
  as
     const_module        CONSTANT  varchar2(62) := 'ut.replace_md_ddl_actual_owner';    
  
     v_src_prefix_expr        varchar2(128);
     v_tgt_prefix_expr        varchar2(128);   
     v_src_prefix_instances   number(38,0); 
     v_tgt_prefix_instances   number(38,0);    
     v_code                   number;
     v_errm                   varchar2(4000);     
     
  begin
  
     v_src_prefix_expr := fn_build_regexp_match_str (p_src_prefix);
     ut.log(const.k_subsys_subset,'regexp_match_str for '||p_src_prefix||' is '||v_src_prefix_expr,null,null,const_module);     
  
     v_tgt_prefix_expr := fn_build_regexp_match_str (p_tgt_prefix);
     ut.log(const.k_subsys_subset,'regexp_match_str for '||p_tgt_prefix||' is '||v_tgt_prefix_expr,null,null,const_module);  
    
  -- md_ddl_parts   
     select SUM(REGEXP_COUNT(object_ddl, v_src_prefix_expr,1,'i')) prefix_instances
       into v_src_prefix_instances
       from md_ddl_parts;   
       
     ut.log(const.k_subsys_subset,to_char(v_src_prefix_instances) ||' instances of '||p_src_prefix||' found in md_ddl_parts',null,null,const_module);     
     
     update md_ddl_parts 
        set object_ddl = REPLACE(object_ddl,p_src_prefix,p_tgt_prefix);
     ut.log(const.k_subsys_subset,p_src_prefix||' replaced with '||p_tgt_prefix||' in '||to_char(sql%rowcount)||' md_ddl_parts records',null,null,const_module);     
    
     select SUM(REGEXP_COUNT(object_ddl, v_tgt_prefix_expr,1,'i')) prefix_instances
       into v_tgt_prefix_instances
       from md_ddl_parts;   
       
     ut.log(const.k_subsys_subset,to_char(v_tgt_prefix_instances) ||' instances of '||p_tgt_prefix||' found in md_ddl_parts',null,null,const_module);     
   
     select SUM(REGEXP_COUNT(object_ddl, v_src_prefix_expr,1,'i')) prefix_instances
       into v_src_prefix_instances
       from md_ddl_parts;   
       
     ut.log(const.k_subsys_subset,to_char(v_src_prefix_instances) ||' instances of '||p_src_prefix||' found in md_ddl_parts',null,null,const_module);       
   
  
  -- md_ddl 
     select SUM(REGEXP_COUNT(object_ddl, v_src_prefix_expr,1,'i')) prefix_instances
       into v_src_prefix_instances
       from md_ddl;   
       
     ut.log(const.k_subsys_subset,to_char(v_src_prefix_instances) ||' instances of '||p_src_prefix||' found in md_ddl',null,null,const_module);     
     
     update md_ddl 
        set object_ddl = REPLACE(object_ddl,p_src_prefix,p_tgt_prefix),
            actual_owner = REPLACE(actual_owner,p_src_prefix,p_tgt_prefix);
            
     ut.log(const.k_subsys_subset,p_src_prefix||' replaced with '||p_tgt_prefix||' in '||to_char(sql%rowcount)||' md_ddl records',null,null,const_module);     
    
     select SUM(REGEXP_COUNT(object_ddl, v_tgt_prefix_expr,1,'i')) prefix_instances
       into v_tgt_prefix_instances
       from md_ddl;   
       
     ut.log(const.k_subsys_subset,to_char(v_tgt_prefix_instances) ||' instances of '||p_tgt_prefix||' found in md_ddl',null,null,const_module);     
   
     select SUM(REGEXP_COUNT(object_ddl, v_src_prefix_expr,1,'i')) prefix_instances
       into v_src_prefix_instances
       from md_ddl;   
       
     ut.log(const.k_subsys_subset,to_char(v_src_prefix_instances) ||' instances of '||p_src_prefix||' found in md_ddl_parts',null,null,const_module);       
     
     commit;
     
  exception
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,substr('Unexpected Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      rollback;       
     
  end replace_md_ddl_actual_owner;
  
 
  procedure restore_session_to_run_env
  as
     const_module        CONSTANT  varchar2(62) := 'ut.restore_session_to_run_env';    
  begin 
     execute immediate 'alter session set current_schema='||gp.get_run_env;
     ut.log(const.k_subsys_subset,'restored current_schema to '||gp.get_run_env,null,null,const_module);                 
  end restore_session_to_run_env;
  
  


  procedure load_table_exclusions
  as
     const_module             CONSTANT  varchar2(62) := 'ut.load_table_exclusions'; 
     v_code                   number;
     v_errm                   varchar2(4000);  
  begin 

    
      -- Load all tables that are excluded by schema
      merge into ss_load_excl_config  slec
      using (select dt.owner,dt.table_name 
            from ss_schema_load_excl_config ssle 
            join dd_tables dt on dt.owner = ssle.owner ) res
      on ( res.owner = slec.owner and res.table_name = slec.table_name)
      when not matched then
          insert (owner,table_name) values (res.owner,res.table_name);
          
      -- Load tables that are individually excluded
      merge into ss_load_excl_config  slec
      using (select dt.owner,dt.table_name 
            from ss_schema_load_excl_config ssle 
            join dd_tables dt on dt.owner = ssle.owner ) res
      on ( res.owner = slec.owner and res.table_name = slec.table_name)
      when not matched then
          insert (owner,table_name) values (res.owner,res.table_name);
          
          
    -- Remove any tables that are implicitally included , this overides exclsuions above
    
    delete from ss_load_excl_config 
    where (owner,table_name) in (select owner,table_name
                                 from ss_load_incl_config);
                                 
    commit;
    
    exception
    when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      ut.log(const.k_subsys_subset,'Unexpected Error: ',v_code,v_errm,const_module);
      rollback;  
  end load_table_exclusions;
  
end ut;

/