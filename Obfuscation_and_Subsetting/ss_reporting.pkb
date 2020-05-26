create or replace PACKAGE BODY ss_reporting is

  procedure gen_obj_count_diff_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_obj_count_diff_report';

    cursor c_obj_count_diffs
    is
       with
         src as (
          SELECT actual_owner,owner,tab,tab_part,idx,idx_part,vws,trg,fnc,prc,pks,pkb,seq,syn,type,type_body,dblnk
            from ss_object_counts
           where src_or_tgt = 'SRC' ),
         tgt as (
          SELECT actual_owner,owner,tab,tab_part,idx,idx_part,vws,trg,fnc,prc,pks,pkb,seq,syn,type,type_body,dblnk
            from ss_object_counts
          where src_or_tgt = 'TGT' )
            select src.actual_owner, 'TABLE' object_type, src.tab src_count, tgt.tab tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.tab <> tgt.tab
            union
            select src.actual_owner, 'PARTITION' object_type, src.tab_part src_count, tgt.tab_part tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.tab_part <> tgt.tab_part
            union
            select src.actual_owner, 'INDEX' object_type, src.idx src_count, tgt.idx tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.idx <> tgt.idx
            union
            select src.actual_owner, 'INDEX PARTITION' object_type, src.idx_part src_count, tgt.idx_part tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.idx_part <> tgt.idx_part
            union
            select src.actual_owner, 'VIEW' object_type, src.vws src_count, tgt.vws tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.vws <> tgt.vws
            union
            select src.actual_owner, 'TRIGGER' object_type, src.trg src_count, tgt.trg tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.trg <> tgt.trg
            union
            select src.actual_owner, 'FUNCTION' object_type, src.fnc src_count, tgt.fnc tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.fnc <> tgt.fnc
            union
            select src.actual_owner, 'PROCEDURE' object_type, src.prc src_count, tgt.prc tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.prc <> tgt.prc
            union
            select src.actual_owner, 'PACKAGE' object_type, src.pks src_count, tgt.pks tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.pks <> tgt.pks
            union
            select src.actual_owner, 'PACKAGE BODY' object_type, src.pkb src_count, tgt.pkb tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.pkb <> tgt.pkb
            union
            select src.actual_owner, 'SEQUENCE' object_type, src.seq src_count, tgt.seq tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.seq <> tgt.seq
            union
            select src.actual_owner, 'SYNONYM' object_type, src.syn src_count, tgt.syn tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.syn <> tgt.syn
            union
            select src.actual_owner, 'TYPE' object_type, src.type src_count, tgt.type tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.type <> tgt.type
            union
            select src.actual_owner, 'TYPE BODY' object_type, src.type_body src_count, tgt.type_body tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.type_body <> tgt.type_body
            union
            select src.actual_owner, 'DATABASE LINK' object_type, src.dblnk src_count, tgt.dblnk tgt_count
              from src join tgt on src.owner = tgt.owner
             where src.dblnk <> tgt.dblnk;


  begin

     ss_reporting.gen_obj_counts('SRC');
     ss_reporting.gen_obj_counts('TGT');

     begin

        delete ss_object_count_diff_report;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_object_count_diff_report',null,null,const_module);

        for r in c_obj_count_diffs
        loop
           insert into ss_object_count_diff_report (actual_owner, object_type, src_count, tgt_count)
           values (r.actual_owner ,r.object_type ,r.src_count ,r.tgt_count );
        end loop;

        commit;

     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM,1,4000);
           ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
           RAISE;
     end;
  end gen_obj_count_diff_report;


  procedure gen_obj_counts(p_src_or_tgt varchar2)
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);
    v_prefix         varchar2(128);
    v_dd_objects     varchar2(128);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_obj_counts';

    x_invalid_parameter EXCEPTION;
    PRAGMA exception_init (x_invalid_parameter, -20009);

  begin
     begin
        ut.load_dd_objects(const.k_subsys_subset);       
     
        delete ss_object_counts where upper(src_or_tgt) = upper(p_src_or_tgt);
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_object_counts for '||p_src_or_tgt,null,null,const_module);

        if upper(p_src_or_tgt) = 'SRC'
        then
           v_prefix     := gp.src_prefix;
           v_dd_objects := p_src_or_tgt||'_dd_objects';
        elsif upper(p_src_or_tgt) = 'TGT'
        then
           v_prefix     := gp.tgt_prefix;
           v_dd_objects := 'dd_objects';
        else
           raise_application_error(-20009,'Invalid input parameter p_src_or_tgt: '||p_src_or_tgt);
        end if;

        v_sql := 'insert into ss_object_counts (src_or_tgt,actual_owner,owner,tab,tab_part,idx,idx_part,vws,trg,fnc,prc,pks,pkb,seq,syn,type,type_body,dblnk)
               select '||chr(39)||upper(p_src_or_tgt)||chr(39)||' src_or_tgt, actual_owner, owner,
               sum(tbl) tab,
               sum(tbp) tab_part,
               sum(idx) idx,
               sum(inp) idx_part,
               sum(vws) vws,
               sum(trg) trg,
               sum(fnc) fnc,
               sum(prc) prc,
               sum(pks) pks,
               sum(pkb) pkb,
               sum(seq) seq,
               sum(syn) syn,
               sum(tys) type,
               sum(tyb) type_body,
               sum(dbl) Dblnk
         from ( select actual_owner, owner,
                case
                  when object_type = ''TABLE'' then 1 else 0
                end  tbl,
                case
                  when object_type = ''TABLE PARTITION'' then 1 else 0
                end  tbp,
                case
                  when object_type = ''INDEX'' then 1 else 0
                end  idx,
                case
                  when object_type = ''INDEX PARTITION'' then 1 else 0
                end  inp,
                case
                  when object_type = ''VIEW''  then 1 else 0
                end  vws,
                case
                  when object_type = ''TRIGGER'' then 1 else 0
                end  trg,
                case
                  when object_type = ''FUNCTION'' then 1 else 0
                end  fnc,
                case
                  when object_type = ''PROCEDURE'' then 1 else 0
                end  prc,
                case
                  when object_type = ''PACKAGE'' then 1 else 0
                end  pks,
                case
                  when object_type = ''PACKAGE BODY'' then 1 else 0
                end  pkb,
                case
                  when object_type = ''SEQUENCE'' then 1 else 0
                end  seq,
                case
                  when object_type = ''SYNONYM'' then 1 else 0
                end  syn,
                case
                  when object_type = ''TYPE'' then 1 else 0
                end  tys,
                case
                  when object_type = ''TYPE BODY'' then 1 else 0
                end  tyb,
                case
                  when object_type = ''DATABASE LINK'' then 1 else 0
                end  dbl
                from ' || v_dd_objects ||
              ' where actual_owner like '||chr(39)||v_prefix||'_%'||chr(39)||'
        )
        GROUP BY actual_owner,owner
        ORDER BY owner';

        execute immediate v_sql;
       -- dbms_output.put_line(v_sql);

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_object_counts '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;
/*
        with diff
        as (
          SELECT owner,TAB,TAB_PART,IDX,IDX_PART,VWS,TRG,FNC,PRC,PKS,PKB,SEQ,SYN,TYPE,TYPE_BODY,DBLNK
            from ss_object_counts
          where src_or_tgt = 'SRC'
          MINUS
          SELECT owner,TAB,TAB_PART,IDX,IDX_PART,VWS,TRG,FNC,PRC,PKS,PKB,SEQ,SYN,TYPE,TYPE_BODY,DBLNK
            from ss_object_counts
          where src_or_tgt = 'TGT' )
          SELECT src.*
            from ss_object_counts src
            join diff on src.owner = diff.owner
          order by src.owner, src.src_or_tgt;
*/
        ---
    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end gen_obj_counts;


  procedure gen_missing_object_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_missing_object_report';

  begin
     begin
        ut.load_dd_objects(const.k_subsys_subset);     
     
        delete ss_missing_objects;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_objects.',null,null,const_module);

        insert into ss_missing_objects(owner, object_type, object_name)
           select owner, object_type, object_name from src_dd_objects
            minus
           select owner, object_type, object_name from dd_objects;

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_objects '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end gen_missing_object_report;

  procedure gen_missing_table_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

      const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_missing_table_report';

  begin
     begin
        ut.load_dd_tables(const.k_subsys_subset);     
     
        delete ss_missing_tables;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_tables.',null,null,const_module);

        insert into ss_missing_tables (owner, table_name)
           select owner, table_name from src_dd_tables
           minus
           select owner, table_name from dd_tables;

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_tables '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end gen_missing_table_report;


  procedure gen_missing_constraints_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_missing_constraints_report';

  begin
     begin
     
        ut.load_dd_constraints(const.k_subsys_subset); 
        ut.load_dd_cons_columns(const.k_subsys_subset); 
     
        delete ss_missing_constraints;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_constraints.',null,null,const_module);

        insert into ss_missing_constraints(owner, r_owner, constraint_name, r_constraint_name, constraint_type, table_name)
           select owner, r_owner, constraint_name, r_constraint_name, constraint_type, table_name 
             from src_dd_constraints
            where constraint_name not like'SYS%' 
              and table_name not like'BIN$%' -- recycebin
           minus
           select owner, r_owner, constraint_name, r_constraint_name, constraint_type, table_name 
             from dd_constraints
            where constraint_name not like'SYS%' 
              and table_name not like'BIN$%';

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_constraints, excluding SYS constraints and BIN$ tables '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        delete ss_missing_sys_constraints;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_sys_constraints.',null,null,const_module);

        insert into ss_missing_sys_constraints(OWNER,SRC_ACTUAL_OWNER,TGT_ACTUAL_OWNER,TABLE_NAME,CONSTRAINT_TYPE,SRC_CONSTRAINT_NAME, column_name)
           select sc.owner, sc.actual_owner src_actual_owner, gp.get_tgt_prefix ||'_'|| sc.owner tgt_actual_owner, sc.table_name, sc.constraint_type, sc.constraint_name  src_constraint_name, scc.column_name
             from src_dd_constraints  sc
             join src_dd_cons_columns scc on sc.owner = scc.owner and sc.table_name = scc.table_name and sc.constraint_name = scc.constraint_name
             left outer join dd_constraints tc on sc.owner = tc.owner and sc.table_name = tc.table_name and sc.constraint_type = tc.constraint_type
             left outer join dd_cons_columns cc on cc.owner = scc.owner and cc.table_name = scc.table_name and cc.column_name = scc.column_name
            where sc.constraint_name like'SYS%'
              and tc.constraint_name is null;              
              
        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_sys_constraints for SYS constraints '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end gen_missing_constraints_report;


  procedure gen_missing_indexes_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_missing_indexes_report';

  begin
     begin
     
        ut.load_dd_indexes(const.k_subsys_subset);     
        ut.load_dd_ind_columns(const.k_subsys_subset);   
     
        delete ss_missing_indexes;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_indexes.',null,null,const_module);

        insert into ss_missing_indexes(index_owner, index_name, table_owner, table_name, index_type, column_names)
           select sic.index_owner, sic.index_name, sic.table_owner, sic.table_name, si.index_type, listagg(sic.column_name,',') WITHIN GROUP (ORDER BY sic.column_name) column_names
             from src_dd_ind_columns sic
             join src_dd_indexes si on sic.table_owner = si.table_owner and sic.index_owner = si.owner and sic.table_name = si.table_name and sic.index_name = si.index_name 
            where si.index_name not like'SYS%'
              and si.index_name not like'BIN$%' -- recycebin
           group by sic.index_owner, sic.index_name, sic.table_owner, sic.table_name, si.index_type 
           minus
           select tic.index_owner, tic.index_name, tic.table_owner, tic.table_name, ti.index_type, listagg(tic.column_name,',') WITHIN GROUP (ORDER BY tic.column_name) column_names
             from dd_ind_columns tic
             join dd_indexes ti on tic.table_owner = ti.table_owner and tic.index_owner = ti.owner and tic.table_name = ti.table_name and tic.index_name = ti.index_name
            where ti.index_name not like'SYS%'
              and ti.index_name not like'BIN$%'
           group by tic.index_owner, tic.index_name, tic.table_owner, tic.table_name, ti.index_type;

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_indexes, excluding SYS and BIN$ indexes '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        delete ss_missing_sys_indexes;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_sys_indexes.',null,null,const_module);

        insert into ss_missing_sys_indexes(owner,src_actual_owner,tgt_actual_owner,table_name,index_type,src_index_name, tgt_index_name, column_name)
           select si.owner, si.actual_owner src_actual_owner, ti.actual_owner tgt_actual_owner, si.table_name, si.index_type, si.index_name  src_index_name, ti.index_name  tgt_index_name, sic.column_name
             from src_dd_indexes  si
             join src_dd_ind_columns sic on si.owner = sic.index_owner and si.table_name = sic.table_name and si.index_name = sic.index_name
             left outer join dd_indexes ti on ti.owner = si.owner and ti.table_name = si.table_name and ti.index_type = si.index_type
             left outer join dd_ind_columns tic on tic.table_owner = sic.table_owner and tic.index_owner = sic.index_owner and tic.table_name = sic.table_name and tic.column_name = sic.column_name
            where si.index_name like'SYS%'
              and ti.index_name is null;           
              
        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_sys_indexes for SYS indexes '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end gen_missing_indexes_report;


  procedure gen_missing_syn_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_missing_syn_report';

  begin
     begin
     
        ut.load_dd_synonyms(const.k_subsys_subset);    
     
        delete ss_missing_synonyms;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_synonyms.',null,null,const_module);

        insert into ss_missing_synonyms(owner, synonym_name,table_owner,table_name)
           select owner, synonym_name, replace(table_owner,gp.src_prefix,gp.tgt_prefix), table_name  from src_dd_synonyms
           minus
           select owner, synonym_name, table_owner, table_name  from dd_synonyms;

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_synonyms '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end gen_missing_syn_report;

  procedure gen_ptn_count_diff_report
  is

     const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_ptn_count_diff_report';

     cursor c_type_subset_counts
     is
        select gp.get_src_prefix||'_'||sc.owner src_actual_owner, gp.get_tgt_prefix||'_'||sc.owner tgt_actual_owner,sc.owner,sc.table_name,co.partition_name ptn
          from ss_config sc
          join ss_companies co on co.ss_run_id = sc.run_id
          join src_dd_tab_partitions tp on sc.owner = tp.table_owner and sc.table_name = tp.table_name and co.partition_name = tp.partition_name
         where sc.ss_type = 'TYPE_SS_SUBSET'
           and sc.partitioned_yn = 'Y'
           and sc.run_id = gp.get_ss_run_id
           and sc.owner <> 'AUDIT'
        order by sc.owner,sc.table_name,co.partition_name; 
  
     v_tgt_stmt    varchar2(4000);
     v_tgt_count   number;    
     v_code        number;
     v_errm        varchar2(4000);  

  begin

     --if changing from count(*) ut.load_dd_tab_partitions(const.k_subsys_subset); 

     delete ss_partition_count_diffs;
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_partition_count_diffs.',null,null,const_module);

     for r in c_type_subset_counts
     loop
        begin
                  
           v_tgt_stmt := 'SELECT count(*) FROM '||r.tgt_actual_owner||'.'||r.table_name||' partition(' || r.ptn ||')'; 
           execute immediate v_tgt_stmt into v_tgt_count;
           
           insert into ss_partition_count_diffs(owner,src_actual_owner,tgt_actual_owner,table_name,partition_name,tgt_num_rows)  
              select r.owner,r.src_actual_owner, r.tgt_actual_owner,r.table_name,r.ptn,v_tgt_count
                from dual;
            
           commit;
 
          /****************************************************************************************************************
          *  Changed, so table partition stats are generated in source utilities schema as part of Metadata generation
          *   as this runs too slowly for large company subsets
          * --metadata_utilities.gather_table_partition_stats@SRC_LINK(r.src_actual_owner,r.table_name,r.ptn); 
          ****************************************************************************************************************/
        
        exception
           when others then
              v_code := SQLCODE;
              v_errm := SUBSTR(SQLERRM,1,4000);
              ut.log(const.k_subsys_subset,'Error: v_tgt_stmt: '||v_tgt_stmt,v_code,v_errm,const_module);       
        end;
     end loop;

     metadata_utilities.load_dd_tab_partitions@SRC_LINK(gp.get_src_prefix);
 
     merge into ss_partition_count_diffs x
     using (select actual_table_owner,table_name,partition_name,num_rows 
              from src_dd_tab_partitions ) y  
        on (     y.actual_table_owner = x.src_actual_owner
             and y.table_name         = x.table_name
             and y.partition_name     = x.partition_name )
     when matched then
        update set x.src_num_rows = y.num_rows;
 
     ut.log(const.k_subsys_subset,'updated '||sql%rowcount||' ss_partition_count_diffs records with source partition num_rows',null,null,const_module); 
            
     commit;

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        RAISE;
  end gen_ptn_count_diff_report;


  procedure gen_load_all_count_diff_report
  is

     const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_load_all_count_diff_report';
  
     cursor c_type_all_counts
     is
        select gp.get_tgt_prefix||'_'||owner tgt_actual_owner,
               gp.get_src_prefix||'_'||owner src_actual_owner,
               owner,table_name
          from ss_config
         where ss_type = 'TYPE_ALL'; 
       
     v_stmt    varchar2(4000);
     v_count   number;    
     v_code    number;
     v_errm    varchar2(4000);   

  begin

     -- if changing from count(*)  ut.load_dd_stats(const.k_subsys_subset); 

     delete ss_load_all_count_diffs;
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_load_all_count_diffs.',null,null,const_module);

     for r in c_type_all_counts
     loop

        begin
           v_stmt := 'SELECT count(*) FROM '||r.tgt_actual_owner||'.'||r.table_name; 
           --dbms_output.put_line(v_stmt);
           execute immediate v_stmt into v_count;
           insert into ss_load_all_count_diffs (owner,src_actual_owner,tgt_actual_owner,table_name,tgt_num_rows)  
              select r.owner,r.src_actual_owner,r.tgt_actual_owner,r.table_name,v_count
                from dual;
           commit;
           
           metadata_utilities.gather_table_stats@SRC_LINK(r.src_actual_owner,r.table_name);
        exception
           when others then
              v_code := SQLCODE;
              v_errm := SUBSTR(SQLERRM,1,4000);
              ut.log(const.k_subsys_subset,'Error: stmt'||v_stmt,v_code,v_errm,const_module);       
        end;

     end loop;
     
     metadata_utilities.load_dd_tab_stats@SRC_LINK(gp.get_src_prefix);
     
     merge into ss_load_all_count_diffs x
     using (select actual_owner,owner,table_name,num_rows 
              from src_dd_tab_stats ) y  
        on (     y.actual_owner   = gp.get_src_prefix||'_'||x.owner
             and y.owner          = x.owner
             and y.table_name     = x.table_name )
      when matched then
         update set x.src_num_rows = y.num_rows;
 
     ut.log(const.k_subsys_subset,'updated '||sql%rowcount||' ss_load_all_count_diffs records with src_dd_tab_stats.num_rows',null,null,const_module);  
     
     commit;

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        RAISE;
  end gen_load_all_count_diff_report;


  procedure gen_load_other_report
  is

     const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_load_other_report';
  
     -- THIS CURSOR / REPORT IS NOT EXPECTED TO RETURN ANY DATA - Negative Test 
     cursor c_other_counts
     is
        select owner,  
               gp.get_tgt_prefix||'_'||owner  tgt_actual_owner,
               gp.get_src_prefix||'_'||owner  src_actual_owner,
               table_name
          from src_dd_tables
         where owner <> 'AUDIT' 
           and table_name not like'%_TEMP'
           and (owner,table_name) not in (select owner, table_name from ss_config);         
       
     v_stmt    varchar2(4000);
     v_count   number;    
     v_code    number;
     v_errm    varchar2(4000);   

  begin

     delete ss_other_counts;
     ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_other_counts.',null,null,const_module);

     for r in c_other_counts
     loop

        begin
           v_stmt := 'SELECT count(*) FROM '||r.tgt_actual_owner||'.'||r.table_name; 
           --dbms_output.put_line(v_stmt);
           execute immediate v_stmt into v_count;
           insert into ss_other_counts (owner,src_actual_owner,tgt_actual_owner,table_name,tgt_num_rows)  
              select r.owner,r.src_actual_owner,r.tgt_actual_owner,r.table_name,v_count
                from dual;
           commit;
           
           metadata_utilities.gather_table_stats@SRC_LINK(r.src_actual_owner,r.table_name);
        exception
           when others then
              v_code := SQLCODE;
              v_errm := SUBSTR(SQLERRM,1,4000);
              ut.log(const.k_subsys_subset,'Error: stmt: '||v_stmt,v_code,v_errm,const_module);       
        end;

     end loop;
     
     metadata_utilities.load_dd_tab_stats@SRC_LINK(gp.get_src_prefix);
     ut.log(const.k_subsys_subset,'Completed generation of src_dd_tab_stats',null,null,const_module);   
     
     update ss_other_counts oc
        set src_num_rows = ( select num_rows
                               from src_dd_tab_stats ts
                              where ts.actual_owner = oc.src_actual_owner
                                and ts.owner        = oc.owner
                                and ts.table_name   = oc.table_name
                                and ts.partition_name is null )
      where exists  ( select 1
                        from src_dd_tab_stats ts
                       where ts.actual_owner = oc.src_actual_owner
                         and ts.owner        = oc.owner
                         and ts.table_name   = oc.table_name
                         and ts.partition_name is null );
 
     ut.log(const.k_subsys_subset,'updated '||sql%rowcount||' ss_other_counts records with src_dd_tab_stats.num_rows',null,null,const_module);  
     
     commit;

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        RAISE;
  end gen_load_other_report;


  procedure gen_tab_col_diff_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_tab_col_diff_report';

  begin
     begin
     
        ut.load_dd_stats(const.k_subsys_subset);  
        
        delete ss_tab_col_diffs;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_tab_col_diffs.',null,null,const_module);

        insert into ss_tab_col_diffs(owner,src_actual_owner,tgt_actual_owner,table_name,column_name,src_data_type,tgt_data_type,
                                    src_data_length,tgt_data_length,src_data_precision,tgt_data_precision,src_data_scale,tgt_data_scale,
                                    src_nullable,tgt_nullable,src_column_id,tgt_column_id,src_character_set_name,tgt_character_set_name)
            select src.owner owner, src.actual_owner src_actual_owner, tgt.actual_owner tgt_actual_owner,src.table_name,src.column_name,
                  src.data_type src_data_type, tgt.data_type tgt_data_type,
                  nvl(src.data_length,-1) src_data_length, nvl(tgt.data_length,-1) tgt_data_length,
                  nvl(src.data_precision,-1) src_data_precision,nvl(tgt.data_precision,-1) tgt_data_precision,
                  nvl(src.data_scale,-1) src_data_scale, nvl(src.data_scale,-1) tgt_data_scale,
                  src.nullable src_nullable, tgt.nullable tgt_nullable,
                  src.column_id src_column_id, tgt.column_id tgt_column_id,
                  src.character_set_name src_character_set_name, tgt.character_set_name tgt_character_set_name
             from src_dd_tab_columns src
             left outer join src_dd_objects sdo on src.owner = sdo.owner
                                               and src.table_name = sdo.object_name
                                               and sdo.object_type = 'VIEW'
             left outer join dd_tab_columns tgt on src.owner = tgt.owner
                                               and src.table_name = tgt.table_name
                                               and src.column_name = tgt.column_name
            where src.actual_owner like gp.get_src_prefix ||'\_%' escape '\' 
              and sdo.object_name is null
              and tgt.actual_owner is not null
              and (   tgt.column_name is null
                   or nvl(src.data_type,'NONE')          <> nvl(tgt.data_type,'NONE')
                   or nvl(src.data_length,-1)            <> nvl(tgt.data_length,-1)
                   or nvl(src.data_precision,-1)         <> nvl(tgt.data_precision,-1)
                   or nvl(src.data_scale,-1)             <> nvl(src.data_scale,-1)
                   or nvl(src.nullable,'X')              <> nvl(tgt.nullable,'X')
                   or nvl(src.column_id,-1)              <> nvl(tgt.column_id,-1)
                   or nvl(src.character_set_name,'NONE') <> nvl(tgt.character_set_name,'NONE')
                  )
              order by src.owner, src.table_name, src.column_name;

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_tab_col_diffs '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end gen_tab_col_diff_report;


  procedure missing_privs_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.missing_privs_report';

  begin
     begin
        ut.load_dd_tab_privs(const.k_subsys_subset);

        delete ss_missing_privs;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_privs.',null,null,const_module);

        insert into ss_missing_privs(grantee,owner,table_name,grantor,privilege,grantable,type)
           select replace(grantee,gp.src_prefix,gp.tgt_prefix),owner,table_name,replace(grantor,gp.src_prefix,gp.tgt_prefix),privilege,grantable,type
             from src_dd_tab_privs
             minus
           select grantee,owner,table_name,grantor,privilege,grantable,type
             from dd_tab_privs;

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_privs',null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end missing_privs_report;


  procedure invalid_objects_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.invalid_objects_report';

  begin
     begin
     
        ut.load_dd_objects(const.k_subsys_subset);
        
        delete ss_invalid_objects;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_invalid_objects.',null,null,const_module);

        insert into ss_invalid_objects(actual_owner, owner, object_type, object_name, last_ddl_time)
           select actual_owner, owner, object_type, object_name, last_ddl_time from dd_objects where status = 'INVALID';

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_invalid_objects '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end invalid_objects_report;
  

  procedure invalid_objects_delta_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.invalid_objects_delta_report';

  begin
     begin
     
        ut.load_dd_objects(const.k_subsys_subset);
     
        delete ss_invalid_objects_delta;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_invalid_objects_delta.',null,null,const_module);

        insert into ss_invalid_objects_delta(actual_owner, owner, object_type, object_name)
           select actual_owner, owner, object_type, object_name from dd_objects where status = 'INVALID'
           minus
           select actual_owner, owner, object_type, object_name from src_dd_objects where status = 'INVALID';

        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_invalid_objects_delta '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end invalid_objects_delta_report;  
  
  
   procedure gen_missing_parts_report
  is

    v_sql            varchar2(4000);
    v_code           number;
    v_errm           varchar2(4000);

    const_module     CONSTANT  varchar2(62) := 'ss_reporting.gen_missing_parts_report';

  begin
     begin

        ut.load_dd_part_tables(const.k_subsys_subset);
     
     
        delete ss_missing_parts;
        ut.log(const.k_subsys_subset,'deleted '||sql%rowcount||' records from ss_missing_parts.',null,null,const_module);

        insert into ss_missing_parts(owner,table_name,partitioning_type,subpartitioning_type)
          select owner,table_name,partitioning_type,subpartitioning_type 
            from src_dd_part_tables
          minus
          select owner,table_name,partitioning_type,subpartitioning_type
            from dd_part_tables;


        ut.log(const.k_subsys_subset,'inserted '||sql%rowcount||' records into ss_missing_parts',null,null,const_module);

        update ss_missing_parts mp
           set mp.part_ddl = ( select replace(to_char(md.object_ddl),gp.src_prefix,gp.tgt_prefix)
                                 from md_ddl md
                                where object_type = 'PARTITION'
                                  and md.owner = mp.owner
                                  and md.base_object_name = mp.table_name
                                  and md.base_object_name is not null
                                  and md.owner is not null )
         where exists ( select 1
                          from md_ddl md
                         where object_type = 'PARTITION'
                           and md.owner = mp.owner
                           and md.base_object_name = mp.table_name
                           and md.base_object_name is not null
                           and md.owner is not null ); 

        ut.log(const.k_subsys_subset,'updated '||sql%rowcount||' records in ss_missing_parts with part_ddl',null,null,const_module);

        commit;

    exception
       when others then
          v_code := SQLCODE;
          v_errm := SUBSTR(SQLERRM,1,4000);
          ut.log(const.k_subsys_subset,substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
          RAISE;
    end;
  end gen_missing_parts_report;


end ss_reporting;
/