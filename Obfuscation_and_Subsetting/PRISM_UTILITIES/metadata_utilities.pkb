create or replace PACKAGE BODY metadata_utilities
AS

function get_db_version return varchar2
is
  const_module   CONSTANT  varchar2(62) := 'metadata_utilities.get_db_version';
  v_db_version   prism_data_base_versions.db_version%type;
begin

  select x.db_version
    into v_db_version
    from ( select db_version, created_by, created_date, environment_prefix
             from prism_data_base_versions
           order by created_date desc ) x
  where rownum = 1;

  return v_db_version;

end get_db_version;


function get_existing_db_version return varchar2
is
  const_module           CONSTANT  varchar2(62) := 'metadata_utilities.get_existing_db_version';
  v_existing_db_version  md_gen_metadata_summary.db_version%type;
begin
 
   begin
   
      select db_version
        into v_existing_db_version
        from md_gen_metadata_summary
       where duration_seconds is not null
         and psm_prefix = metadata_utilities.get_prism_prefix;

   exception
      when no_data_found then
         v_existing_db_version := null;
      when others then
         raise;
   end;      

   return v_existing_db_version;

end get_existing_db_version;  
  

procedure log(p_log_msg VARCHAR2,p_code NUMBER,p_errm varchar2,p_module varchar2) is
  v_nLogID NUMBER;
begin
  v_nLogID := log( p_log_msg, p_code, p_errm, p_module );
end log;


function log(p_log_msg VARCHAR2, p_code NUMBER,p_errm varchar2,p_module varchar2)  return number is
   pragma autonomous_transaction;
   v_nLogID NUMBER;
begin

   v_nLogID := util_log_id_seq.nextval;

   insert into util_log (log_id, log_msg, err_code, errm, module, mod_timestamp)
       values (v_nLogID, p_log_msg, p_code, p_errm, p_module, systimestamp);

   commit;

   return v_nLogID;

end log;


procedure load_dd (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd';
   v_code         number;
   v_errm         varchar2(4000);

begin

   begin

      load_dd_users (p_prefix);
      load_dd_tables (p_prefix);
      load_dd_views (p_prefix);  
      load_dd_tab_columns (p_prefix);
      load_dd_tab_partitions (p_prefix);
      load_dd_constraints (p_prefix);
      load_dd_synonyms (p_prefix);
      load_dd_cons_columns (p_prefix);
      load_dd_ind_columns (p_prefix);
      load_dd_indexes (p_prefix);
      load_dd_objects (p_prefix);
      load_dd_tab_privs (p_prefix);
      load_dd_tab_stats (p_prefix);
      load_dd_tab_col_stats (p_prefix);
      load_dd_part_tables (p_prefix);
      
      commit;

   exception
      when others then
         v_code := SQLCODE;
         v_errm := SUBSTR(SQLERRM,1,4000);
         metadata_utilities.log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
         RAISE;
   end;

end load_dd;


procedure load_dd_users (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_users';
   v_code           number;
   v_errm           varchar2(4000);

begin

   delete  dd_users;
   metadata_utilities.log(to_char(sql%rowcount) || ' rows deleted from dd_users',null,null,const_module);

   insert into dd_users (username)
      select username
        from dba_users
       where username like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_users',null,null,const_module);

   commit;

end load_dd_users;


procedure load_dd_synonyms (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_synonyms';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete  dd_synonyms;
   metadata_utilities.log(to_char(sql%rowcount) || ' rows deleted from dd_synonyms',null,null,const_module);

   insert into dd_synonyms (owner,actual_owner ,synonym_name ,table_owner ,table_name,db_link,origin_con_id )
   select replace(owner,p_prefix||'_',null) owner,owner actual_owner ,synonym_name ,table_owner ,table_name,db_link,origin_con_id
   from dba_synonyms
   where owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_synonyms',null,null,const_module);

   commit;

end load_dd_synonyms;


procedure load_dd_tables (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_tables';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete  dd_tables;
   metadata_utilities.log(to_char(sql%rowcount) || ' rows deleted from dd_tables',null,null,const_module);

   insert into dd_tables (actual_owner, owner, table_name,tablespace_name,temporary,num_rows,iot_name,iot_type)
      select owner actual_owner, replace(owner,p_prefix||'_',null) owner, table_name,tablespace_name,temporary,num_rows,iot_name,iot_type
        from dba_tables
       where owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_tables',null,null,const_module);

   resolve_glob_seq_sort;

   commit;

end load_dd_tables;

procedure load_dd_views (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_views';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete  dd_views;
   metadata_utilities.log(to_char(sql%rowcount) || ' rows deleted from dd_views',null,null,const_module);

   insert into dd_views (actual_owner, owner, view_name)
      select owner actual_owner, replace(owner,p_prefix||'_',null) owner, view_name
        from dba_views
       where owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_views',null,null,const_module);

   commit;

end load_dd_views; 

procedure load_dd_tab_columns (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_tab_columns';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete  dd_tab_columns;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_tab_columns',null,null,const_module);

   insert into dd_tab_columns (actual_owner, owner, table_name,column_name,data_type,data_length,data_precision,data_scale,nullable,column_id,character_set_name)
        select owner actual_owner, replace(owner,p_prefix||'_',null) owner, table_name,column_name,data_type,data_length,data_precision,data_scale,nullable,column_id,character_set_name
          from dba_tab_columns
         where owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_tab_columns',null,null,const_module);

   commit;

end load_dd_tab_columns;

procedure load_dd_tab_partitions (p_prefix varchar2)
is

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_tab_partitions';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete  dd_tab_partitions;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_tab_partitions',null,null,const_module);

   insert into dd_tab_partitions (actual_table_owner,table_owner,table_name,partition_name,num_rows,last_analyzed)
        select table_owner actual_table_owner, replace(table_owner,p_prefix||'_',null) table_owner, table_name,partition_name,num_rows,last_analyzed
          from dba_tab_partitions
         where table_owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_tab_partitions',null,null,const_module);

   commit;
end load_dd_tab_partitions;


procedure load_dd_part_tables (p_prefix varchar2)
is

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_part_tables';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete  dd_part_tables;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_part_tables',null,null,const_module);

   insert into dd_part_tables (actual_owner,owner,table_name,partitioning_type,subpartitioning_type)
        select owner actual_table_owner,  replace(owner,p_prefix||'_',null) owner, table_name,partitioning_type,subpartitioning_type
          from dba_part_tables
         where owner like p_prefix||'\_%' escape '\';
         
   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_part_tables',null,null,const_module);

   commit;
end load_dd_part_tables;


procedure load_dd_constraints (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_constraints';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete dd_constraints;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_constraints',null,null,const_module);

   insert into dd_constraints (
      actual_owner,owner,constraint_name,constraint_type,table_name,
      actual_r_owner, r_owner,r_constraint_name,
      actual_index_owner,index_owner,index_name
   )
   select owner actual_owner, replace(owner,p_prefix||'_',null) owner, constraint_name,constraint_type,table_name,
          r_owner actual_r_owner, replace(r_owner,p_prefix||'_',null) r_owner, r_constraint_name,
          index_owner actual_index_owner, replace(index_owner,p_prefix||'_',null) index_owner,index_name
     from dba_constraints
    where owner like p_prefix||'\_%' escape '\';

    metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_constraints',null,null,const_module);

   commit;

end load_dd_constraints;

procedure load_dd_cons_columns (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_cons_columns';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete dd_cons_columns;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_cons_columns',null,null,const_module);


   insert into dd_cons_columns (
     actual_owner,owner,constraint_name,table_name,column_name,position
   )
   select owner actual_owner, replace(owner,p_prefix||'_',null) owner, constraint_name, table_name, column_name, position
     from dba_cons_columns
    where owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_cons_columns',null,null,const_module);

   commit;

end load_dd_cons_columns;


procedure load_dd_ind_columns (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_ind_columns';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete dd_ind_columns;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_ind_columns',null,null,const_module);

   insert into dd_ind_columns (
      actual_index_owner,index_owner,index_name,
      actual_table_owner,table_owner,table_name,
      column_name,column_position,column_length,char_length
   )
   select index_owner actual_index_owner, replace(index_owner,p_prefix||'_',null) index_owner, index_name,
          table_owner actual_table_owner, replace(table_owner,p_prefix||'_',null) table_owner, table_name,
          column_name, column_position, column_length, char_length
     from dba_ind_columns
    where index_owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_ind_columns',null,null,const_module);

   commit;

end load_dd_ind_columns;


procedure load_dd_indexes (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_indexes';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete dd_indexes;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_indexes',null,null,const_module);

   insert into dd_indexes (
      actual_owner,owner,index_name,
      index_type,actual_table_owner,table_owner,
      table_name,table_type
   )
   select owner actual_owner, replace(owner,p_prefix||'_',null) owner, index_name,
          index_type,table_owner,replace(table_owner,p_prefix||'_',null),
          table_name,table_type
     from dba_indexes
    where owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_indexes',null,null,const_module);

   commit;

end load_dd_indexes;


procedure load_dd_objects (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_objects';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete dd_objects;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_objects',null,null,const_module);

   insert into dd_objects (object_id, actual_owner, owner, object_type, object_name,last_ddl_time,status,temporary)
      select object_id, owner actual_owner, replace(owner,p_prefix||'_',null) owner, object_type, object_name,last_ddl_time,status,temporary
        from dba_objects
       where owner like p_prefix||'\_%' escape '\'
         and object_id is not null;

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_objects',null,null,const_module);

   commit;

end load_dd_objects;


procedure load_dd_tab_privs (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_tab_privs';
   v_code         number;
   v_errm         varchar2(4000);

begin

   delete dd_tab_privs;
   metadata_utilities.log('deleted '||sql%rowcount||' rows from dd_tab_privs',null,null,const_module);

   insert into dd_tab_privs (grantee, actual_owner, owner, table_name, grantor, privilege, grantable, hierarchy, common, type, inherited)
        select grantee, owner actual_owner, replace(owner,p_prefix||'_',null) as owner, table_name, grantor, privilege, grantable, hierarchy, common, type, inherited
          from dba_tab_privs
         where grantee like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_tab_privs',null,null,const_module);

   commit;

end load_dd_tab_privs;


procedure load_dd_tab_stats (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_tab_stats';
   v_code         number;
   v_errm         varchar2(4000);

  cursor cGetShemas is select src_schema from ss_schema_list;

begin

  for cGetShemasRec in cGetShemas loop

     metadata_utilities.load_dd_tab_stats(cGetShemasRec.src_schema,p_prefix);

  end loop;

  commit;

end load_dd_tab_stats;


procedure load_dd_tab_stats (p_src_schema varchar2, p_prefix varchar2) as
   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_tab_stats';
   v_code         number;
   v_errm         varchar2(4000);

begin

   metadata_utilities.log('deleting dd_tab_stats for schema '||p_src_schema,null,null,const_module);
   delete dd_tab_stats where actual_owner = p_src_schema;

   insert into dd_tab_stats(owner,actual_owner,table_name,partition_name,partition_position,subpartition_name,
               subpartition_position,object_type,num_rows,blocks,empty_blocks,avg_space,chain_cnt,avg_row_len,
               avg_space_freelist_blocks,num_freelist_blocks,avg_cached_blocks,avg_cache_hit_ratio,im_imcu_count,
               im_block_count,im_stat_update_time,scan_rate,sample_size,last_analyzed,global_stats,user_stats,
               stattype_locked,stale_stats,scope)
       select  replace(owner,p_prefix||'_',null) owner,owner actual_owner,table_name,partition_name,
               partition_position,subpartition_name,subpartition_position,object_type,num_rows,blocks,
               empty_blocks,avg_space,chain_cnt,avg_row_len,avg_space_freelist_blocks,num_freelist_blocks,
               avg_cached_blocks,avg_cache_hit_ratio,im_imcu_count,im_block_count,
               im_stat_update_time,scan_rate,sample_size,last_analyzed,global_stats,
               user_stats,stattype_locked,stale_stats,scope
         from  dba_tab_statistics
        where  owner = p_src_schema;

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_tab_stats for schema '||p_src_schema,null,null,const_module);

   commit;

end load_dd_tab_stats;

procedure load_dd_tab_col_stats (p_prefix varchar2) as

   const_module   CONSTANT  varchar2(62) := 'metadata_utilities.load_dd_tab_col_stats';
   v_code         number;
   v_errm         varchar2(4000);

begin

   metadata_utilities.log('deleting dd_tab_col_stats',null,null,const_module);
   delete dd_tab_col_stats;

   insert into dd_tab_col_stats( owner,  actual_owner,table_name,column_name,num_distinct,low_value,high_value,density ,
                                 num_nulls,num_buckets,last_analyzed,sample_size,global_stats,user_stats,notes,avg_col_len,histogram ,scope )
   select replace(owner,p_prefix||'_',null)  owner,owner actual_owner,table_name,column_name,num_distinct,low_value,high_value,density,num_nulls,num_buckets,
          last_analyzed,sample_size,global_stats,user_stats,notes,avg_col_len,histogram,scope
     from dba_tab_col_statistics
    where owner like p_prefix||'\_%' escape '\';

   metadata_utilities.log(to_char(sql%rowcount) || ' rows inserted into dd_tab_col_stats',null,null,const_module);

   commit;

end load_dd_tab_col_stats;


  procedure sleep ( p_sleep_seconds  number )
  is
     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.sleep';
     v_code         number;
     v_errm         varchar2(4000);
     v_sql          varchar2(4000);

  begin
     dbms_lock.sleep(p_sleep_seconds);
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  end sleep;


  procedure monitor_jobs( p_start_date date )
  is
     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.monitor_jobs';
     v_code         number;
     v_errm         varchar2(4000);
     v_sql          varchar2(4000);

     cursor c_job_monitor
     is
        select jex.job_name, nvl(jrd.status,asj.state) status, jrd.log_date --, jrd.run_duration, jrd.errors
          from ss_job_execution jex
          left outer join all_scheduler_jobs asj            on jex.job_name = asj.job_name
          left outer join all_scheduler_job_run_details jrd on jex.job_name = jrd.job_name          
         where jex.completed_yn = 'N'
           and actual_start_date > p_start_date
       order by jex.start_timestamp ASC;  
       
  begin
     for r in c_job_monitor
     loop
        metadata_utilities.merge_job_execution( r.job_name, p_start_date );
     end loop;
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  end monitor_jobs;


  function get_running_job_cnt
    return number
  is
     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.get_running_job_cnt';
     v_code              number;
     v_errm              varchar2(4000);
     v_running_job_cnt   number;
     
     cursor c_job_monitor
     is
       select jex.job_name
         from ss_job_execution jex
        where jex.completed_yn = 'N'
       order by jex.start_timestamp ASC;
       
  begin
     begin

        for r in c_job_monitor
        loop
           metadata_utilities.merge_job_execution( r.job_name, null );
        end loop;
     
        select count(*)
          into v_running_job_cnt
          from all_scheduler_jobs asj 
          join ss_job_execution jex on asj.job_name = jex.job_name and asj.start_date = jex.start_timestamp  
         where asj.state = 'RUNNING';
         
     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     end;

     return v_running_job_cnt;

  end get_running_job_cnt;


  function get_incomplete_job_cnt
     return number
  is
     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.get_incomplete_job_cnt';
     v_code                 number;
     v_errm                 varchar2(4000);
     v_incomplete_job_cnt   number;

    cursor c_job_monitor
    is
      select jex.job_name
        from ss_job_execution jex
       where jex.completed_yn = 'N'
         and jex.job_name <> const.k_job_monitor
      order by jex.start_timestamp ASC;
  begin
     begin

        for r in c_job_monitor
        loop
           metadata_utilities.merge_job_execution( r.job_name, null );
        end loop;

        select count(*)
          into v_incomplete_job_cnt
          from ss_job_execution
         where completed_yn = 'N';

     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     end;

     return v_incomplete_job_cnt;

  end get_incomplete_job_cnt;


  function  create_job ( p_job_name        varchar2,
                         p_job_action      varchar2,
                         p_repeat_interval varchar2  default null,
                         p_start_date      timestamp default null )
     return timestamp                    
  is

     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.create_job';
     
     v_code                   number;
     v_errm                   varchar2(4000);

     v_ts_before_creation     timestamp;
     v_job_start_date         all_scheduler_jobs.start_date%type;
     v_job_ddl                varchar2(4000);
     
  begin

     metadata_utilities.log('p_job_name: ' || p_job_name || ' p_job_action: ' || p_job_action || ' p_repeat_interval: ' || p_repeat_interval || ' p_start_date: '|| p_start_date,null,null,const_module);        
  
     begin
        DBMS_SCHEDULER.DROP_JOB(job_name => p_job_name); 
     --   execute immediate 'BEGIN DBMS_SCHEDULER.DROP_JOB(job_name => '||chr(39)||p_job_name||chr(39)||'); END;';
     exception
        when x_unknown_job then
           null;
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr('p_job_name: ' || p_job_name || ' p_job_action: ' || p_job_action || ' Errors: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
           raise;
     end;

     metadata_utilities.sleep(const.k_sleep_seconds); -- prevent contention / dbms_scheduler lock up
     v_ts_before_creation := systimestamp;
     
     v_job_start_date := nvl(p_start_date, SYSDATE);
     
     begin
       -- execute immediate v_job_ddl;
        DBMS_SCHEDULER.CREATE_JOB (
                    job_name           =>  p_job_name,
                    job_type           =>  'PLSQL_BLOCK',
                    job_action         =>  p_job_action,
                    start_date         =>  v_job_start_date,
                    repeat_interval    =>  p_repeat_interval,
                    enabled            =>  TRUE
                 );
     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);        
           raise;
     end;
     
     --metadata_utilities.sleep(const.k_sleep_seconds); 
     metadata_utilities.sleep(const.k_sleep_seconds);  -- sleep to wait for job to start
     
     -- get actual start date 
     --if p_start_date is not null then
        v_job_start_date := metadata_utilities.get_job_start_date(p_job_name,v_ts_before_creation);
     --end if;
     
     v_job_start_date := nvl(v_job_start_date, p_start_date);     
     --v_job_status     := metadata_utilities.get_job_status(p_job_name);     
     return v_job_start_date;
     
  end create_job;

  procedure create_job ( p_job_name        varchar2,
                         p_job_action      varchar2,
                         p_repeat_interval varchar2  default null,
                         p_start_date      timestamp default null )
  is

     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.create_job';
     
     v_code                   number;
     v_errm                   varchar2(4000);

     v_job_start_date         all_scheduler_jobs.start_date%type;
     
  begin

     metadata_utilities.log('p_job_name: ' || p_job_name || ' p_job_action: ' || p_job_action || ' p_repeat_interval: ' || p_repeat_interval || ' p_start_date: '|| p_start_date,null,null,const_module);        

     v_job_start_date := metadata_utilities.create_job ( p_job_name,p_job_action,p_repeat_interval,p_start_date );
 
     metadata_utilities.log('p_job_name created with start date ' || to_char(v_job_start_date,'DD-MON-YYYY HH24:MI:SS'),null,null,const_module);        

     metadata_utilities.merge_job_execution( p_job_name, 'STATS');
     
  end create_job;                         


  procedure run_job ( p_job_name        varchar2 )                
  is

     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.run_job';
     
     v_code                   number;
     v_errm                   varchar2(4000);
    
  begin

     metadata_utilities.log('Running job: ' || p_job_name,null,null,const_module);        

     begin
        DBMS_SCHEDULER.RUN_JOB(p_job_name);
     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);        
           raise;
     end;    
     
  end run_job;

  procedure merge_job_execution( p_job_name                varchar2,
                                 p_job_type                varchar2) -- 'PARTITION_STATS','TABLE_STATS'
  is
     pragma autonomous_transaction;
    
     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.merge_job_execution';

     v_job_actual_owner    ss_job_execution.job_actual_owner%type;
     --v_table_owner               dd_tables.owner%type;
     --v_table_actual_owner               dd_tables.actual_owner%type;     
     v_job_name            all_scheduler_jobs.job_name%type;
     v_job_start_date      all_scheduler_jobs.start_date%type;
     v_job_status          all_scheduler_jobs.state%type;
     v_job_run_duration    all_scheduler_job_run_details.run_duration%type;  
     v_completed_yn        ss_job_execution.completed_yn%type;
    
     v_code                   number;
     v_errm                   varchar2(4000);
     
  begin
     begin
       
        select job_actual_owner,job_name,job_start_date,status,completed_yn,run_duration
          into v_job_actual_owner,v_job_name,v_job_start_date,v_job_status,v_completed_yn,v_job_run_duration
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
                 where ( jrd.job_name = p_job_name or asj.job_name = p_job_name )
                  order by nvl(asj.start_date,jrd.actual_start_date) desc  )
         where rownum = 1; --latest matching job by name

      exception
         when no_data_found
         then --x_unknown_job,-27475
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
      end;
      
      merge into ss_job_execution x
      using (select 1 from dual) y  
         on (  x.job_name  = p_job_name
          )
          when matched
          then
              update
                 set x.status                = v_job_status,
                     x.completed_yn          = v_completed_yn,
                     x.start_timestamp       = v_job_start_date,
                     x.run_duration          = v_job_run_duration,
                     --x.table_owner           = v_owner,
                     --x.table_actual_owner    = v_table_actual_owner  
                     x.job_actual_owner      = v_job_actual_owner
          when not matched
          then
              insert (job_actual_owner,job_name,job_type,start_timestamp,run_duration,status,completed_yn)
               values (v_job_actual_owner,p_job_name,p_job_type,v_job_start_date,v_job_run_duration,v_job_status,v_completed_yn);
      commit;

  end merge_job_execution;


  procedure gather_table_partition_stats(p_actual_owner varchar2, p_table_name varchar2, p_partition varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gather_table_partition_stats';
    
     v_code                   number;
     v_errm                   varchar2(4000); 
     
  begin

     metadata_utilities.log('gathering partition_stats for: '||p_actual_owner||'.'||p_table_name||'.'||p_partition,null,null,const_module);
        
     BEGIN 
        dbms_stats.gather_table_stats( OWNNAME     => p_actual_owner,
                                       TABNAME     => p_table_name,
                                       PARTNAME    => p_partition,
                                       GRANULARITY => 'PARTITION',
                                       CASCADE     => FALSE );
     END;
        
     -- could consider table level refresh at some point if procedure not called from gather_schema_partition_stats
     -- although currently invoked remotely from OS which also invokes metadata_utilities.load_dd_tab_partitions after batch

  end gather_table_partition_stats;


  procedure gather_schema_partition_stats(p_src_schema varchar2, p_part_list varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gather_schema_partition_stats';

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
        select tp.actual_table_owner, tp.table_owner, tp.table_name, tp.partition_name
          from md_ddl md
          join dd_tab_partitions tp on md.actual_owner = tp.actual_table_owner and md.object_name = tp.table_name
           join ptn on ptn.partition_name = tp.partition_name
          where md.actual_owner = cp_owner
            and md.object_type = 'TABLE'
            and md.dp_yn = 'N'
            and md.partitioning_type is not null;
            
     v_job_name                      user_scheduler_jobs.job_name%type;
     v_job_action                    varchar2(4000);
     v_part_list                     varchar2(4000);     
     v_prism_prefix                  varchar2(100);
     v_running_job_cnt               number;

     v_code                   number;
     v_errm                   varchar2(4000); 
     
  begin

     v_part_list := upper(p_part_list);
     if v_part_list not like '%NULL_COMP_CODE%' 
     then
        v_part_list := v_part_list||','||'NULL_COMP_CODE';
     end if;
     
     metadata_utilities.log('Starting to gather schema partition stats for '||p_src_schema||','||v_part_list,null,null,const_module);

     for r in c_tab_partitions(p_src_schema,v_part_list)
     loop       
        metadata_utilities.log('Calling gather_table_partition_stats for '||r.actual_table_owner||','||r.table_name||','||r.partition_name,null,null,const_module);
        gather_table_partition_stats(r.actual_table_owner,r.table_name,r.partition_name);    
     end loop;   

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr('Unexpected Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
  end gather_schema_partition_stats;


  procedure gather_partition_stats(p_part_list varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gather_partition_stats';

     cursor c_schemas
     is
        select src_schema
          from ss_schema_list;
           
     v_prism_prefix           varchar2(128);
     v_repeat_interval        varchar2(4000);
     
     v_code                   number;
     v_errm                   varchar2(4000);      
     
  begin

     for r in c_schemas
     loop
        metadata_utilities.gather_schema_partition_stats(r.src_schema, p_part_list);
     end loop;
  
     v_prism_prefix := metadata_utilities.get_prism_prefix;
     metadata_utilities.load_dd_tab_stats(v_prism_prefix);
     metadata_utilities.load_dd_tab_partitions(v_prism_prefix);     
     metadata_utilities.load_dd_tab_col_stats(v_prism_prefix);   
     
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr('Unexpected Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
  end gather_partition_stats;


  procedure gather_table_stats(p_actual_owner varchar2, p_table_name varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gather_table_stats';
  
     v_code                   number;
     v_errm                   varchar2(4000); 
     
  begin

     metadata_utilities.log('gathering table stats for: '||p_actual_owner||'.'||p_table_name,null,null,const_module);
        
     BEGIN 
        dbms_stats.gather_table_stats( OWNNAME   =>  p_actual_owner,
                                       TABNAME   =>  p_table_name,
                                       CASCADE   =>  FALSE );
     END;
        
  end gather_table_stats;


  procedure gen_global_table_stats ( p_stats_est_percent         integer  default null,  -- default is AUTO
                                     p_cascade                   boolean  default TRUE,
                                     p_days_since_last_analyzed  integer  DEFAULT 1) 
  is
  
    const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gen_global_table_stats';
    v_stats_count   integer;
    v_prefix        varchar2(30);
    v_code          number;
    v_errm          varchar2(4000);    
  
    cursor c_table_stats (p_days_since_last_analyzed INTEGER) is
       select distinct dt.actual_owner owner,dt.table_name
         from dd_tables dt
         join dd_tab_stats dts on dt.owner = dts.owner
                              and dt.table_name = dts.table_name
        where dt.table_name NOT LIKE'SYS_IOT%'
          and dt.owner <> 'AUDIT'
          and (dts.stale_stats is null or stale_stats <> 'NO')
          and TRUNC(NVL(dts.last_analyzed,SYSDATE-(p_days_since_last_analyzed+1))) < TRUNC(SYSDATE-p_days_since_last_analyzed);
  
  begin
     v_prefix := metadata_utilities.get_prism_prefix;
     begin

        metadata_utilities.log('Opening c_table_stats cursor with p_days_since_last_analyzed: ' || p_days_since_last_analyzed,null,null,const_module);
        for rec in c_table_stats(p_days_since_last_analyzed) loop
           begin

              dbms_stats.gather_table_stats(ownname          => rec.owner,
                                            tabname          => rec.table_name,
                                            estimate_percent => nvl(p_stats_est_percent,dbms_stats.auto_sample_size),
                                            cascade          => p_cascade);

              v_stats_count := v_stats_count + 1;
         
           exception when others then
              v_code := SQLCODE;
              v_errm := SUBSTR(SQLERRM,1,4000);
              metadata_utilities.log('Error gathering stats for '||rec.owner||'.'||rec.table_name,v_code,v_errm,const_module);
           end; 
        end loop;
      
        if v_stats_count > 1
        then
           metadata_utilities.load_dd_tab_stats (v_prefix);
           metadata_utilities.load_dd_tab_col_stats (v_prefix);
           metadata_utilities.log('Completed refresh of dd stats tables after gathering stats for ' || v_stats_count || ' tables',null,null,const_module);
        end if;
     end;
  end gen_global_table_stats;


  procedure create_monitor_job
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.create_monitor_job';
          
     v_incomplete_job_cnt     number;  
     v_job_name               varchar2(32);
     v_job_action             varchar2(4000);
     v_job_start_time         timestamp;     
     v_repeat_interval        varchar2(4000);
     
     v_code                   number;
     v_errm                   varchar2(4000);      
     
  begin

     v_job_name   := const.k_job_monitor; --'JOB_MONITOR';

     begin
        DBMS_SCHEDULER.DROP_JOB ( v_job_name );
     exception
       when excep.x_unknown_job then
         null;
     end;
   
     v_repeat_interval := 'FREQ=SECONDLY;INTERVAL='||const.k_monitor_job_interval;  -- INTERVAL '60' SECOND;    
     
     v_job_action := 'BEGIN 
                        metadata_utilities.monitor_jobs('||SYSDATE||');  
                      END;';     
     
     metadata_utilities.log('Starting job to execute monitor_jobs proc every '||v_repeat_interval||' seconds',null,null,const_module);
                        
     v_job_start_time := metadata_utilities.create_job (v_job_name,v_job_action,v_repeat_interval,null);  
     
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr('Unexpected Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
  end create_monitor_job;


  procedure drop_monitor_job
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.drop_monitor_job';
 
     v_incomplete_job_cnt     number;  
     v_job_name               varchar2(32);
     
     v_code                   number;
     v_errm                   varchar2(4000);  
     
   begin  

      metadata_utilities.log('Stopping/Dropping '|| const.K_JOB_MONITOR ||' job',null,null,const_module);

      begin
         DBMS_SCHEDULER.STOP_JOB(job_name => const.K_JOB_MONITOR);
      exception
         when excep.x_job_not_running
         then
            DBMS_SCHEDULER.DROP_JOB(job_name => const.K_JOB_MONITOR);
         when excep.x_unknown_job
         then                  
            null;
      end;
      
      merge_job_execution(const.k_JOB_MONITOR,'MONITORING');

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr('Unexpected Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
  end drop_monitor_job;

/*
  procedure gather_table_partition_stats_by_job(p_actual_owner varchar2, p_table_name varchar2, p_partition varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gather_table_partition_stats_by_job';
    
     v_job_name               varchar2(32);
     --NOT user_scheduler_jobs.job_name%type;
     --ORA-12899: value too large for column "SYS"."WRI$_OPTSTAT_OPR"."JOB_NAME" (actual: 42, maximum: 32)
     v_job_action             varchar2(4000);
     v_job_start_time         timestamp;
     v_code                   number;
     v_errm                   varchar2(4000); 
     
  begin

     metadata_utilities.log('gathering partition_stats for: '||p_actual_owner||'.'||p_table_name||'.'||p_partition,null,null,const_module);
        
     v_job_name   := substr('STATS_'||substr(p_table_name,1,20)||'_'||p_partition,1,32);  -- restricted to 32 chars
        
     v_job_action := 'BEGIN dbms_stats.gather_table_stats( OWNNAME=>'    ||chr(39)|| p_actual_owner ||chr(39)||','||
                                                          'TABNAME=>'    ||chr(39)|| p_table_name   ||chr(39)||','||
                                                          'PARTNAME=>'   ||chr(39)|| p_partition    ||chr(39)||','||
                                                          'GRANULARITY=>'||chr(39)|| 'PARTITION'    ||chr(39)||','||
                                                          'CASCADE=> FALSE );'  ||
                    ' END;';
        
     v_job_start_time := metadata_utilities.create_job ( v_job_name,v_job_action,null,null );

     metadata_utilities.merge_job_execution( v_job_name, 'PARTITION_STATS');

     -- procedure removed from package spec; only invoked from gather_schema_partition_stats
     -- could consider table level refresh at some point if procedure not called from gather_schema_partition_stats

  end gather_table_partition_stats_by_job;

  
  procedure gather_schema_partition_stats_by_job(p_src_schema varchar2, p_part_list varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gather_schema_partition_stats_by_job';

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
        select tp.actual_table_owner, tp.table_owner, tp.table_name, tp.partition_name
          from md_ddl md
          join dd_tab_partitions tp on md.actual_owner = tp.actual_table_owner and md.object_name = tp.table_name
           join ptn on ptn.partition_name = tp.partition_name
          where md.actual_owner = cp_owner
            and md.object_type = 'TABLE'
            and md.dp_yn = 'N'
            and md.partitioning_type is not null;
            
     v_job_name                      user_scheduler_jobs.job_name%type;
     v_job_action                    varchar2(4000);
     v_part_list                     varchar2(4000);     
     v_prism_prefix                  varchar2(100);
     v_running_job_cnt               number;

     v_code                   number;
     v_errm                   varchar2(4000); 
     
  begin

     v_part_list := upper(p_part_list);
     if v_part_list not like '%NULL_COMP_CODE%' 
     then
        v_part_list := v_part_list||','||'NULL_COMP_CODE';
     end if;
     
     metadata_utilities.log('Starting to gather schema partition stats for '||p_src_schema||','||v_part_list,null,null,const_module);

     for r in c_tab_partitions(p_src_schema,v_part_list)
     loop
        v_running_job_cnt := metadata_utilities.get_running_job_cnt;
       -- metadata_utilities.sleep(const.k_sleep_seconds);        
        while v_running_job_cnt >= const.k_max_parallel_jobs
        loop
           v_running_job_cnt := metadata_utilities.get_running_job_cnt;
           metadata_utilities.log('Currently '||v_running_job_cnt||' partition stats jobs running (limited to '||const.k_max_parallel_jobs||')',null,null,const_module);       
        end loop;        
        metadata_utilities.log('Calling gather_table_partition_stats for '||r.actual_table_owner||','||r.table_name||','||r.partition_name,null,null,const_module);
        gather_table_partition_stats_by_job(r.actual_table_owner,r.table_name,r.partition_name);    
     end loop;   

  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr('Unexpected Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
  end gather_schema_partition_stats_by_job;  


  procedure gather_partition_stats_by_job(p_part_list varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gather_partition_stats_by_job';

     cursor c_schemas
     is
        select src_schema
          from ss_schema_list;
          
     v_incomplete_job_cnt     number;  
     v_job_name               varchar2(32);
     v_job_action             varchar2(4000);
     v_job_start_time         timestamp;     
     v_prism_prefix           varchar2(100);
     v_repeat_interval        varchar2(4000);
     
     v_code                   number;
     v_errm                   varchar2(4000);      
     
  begin

     v_job_name   := 'JOB_MONITOR';

     begin
        DBMS_SCHEDULER.DROP_JOB ( v_job_name );
     exception
       when excep.x_unknown_job then
         null;
     end;
     
     metadata_utilities.log('Starting job to execute monitor_jobs procedure every '||const.k_monitor_job_interval||' seconds',null,null,const_module);
                
     v_job_action := 'BEGIN 
                        metadata_utilities.monitor_jobs;
                      END;';
        
     v_repeat_interval := 'FREQ=SECONDLY;BYSECOND=5';  -- INTERVAL '5' SECOND;
     v_job_start_time := metadata_utilities.create_job (v_job_name,v_job_action,v_repeat_interval,null); 

     for r in c_schemas
     loop
        metadata_utilities.gather_schema_partition_stats_by_job(r.src_schema, p_part_list);
     end loop;

     while v_incomplete_job_cnt > 0
     loop
        metadata_utilities.sleep(const.k_sleep_seconds);
        v_incomplete_job_cnt := metadata_utilities.get_incomplete_job_cnt;
        metadata_utilities.log('Waiting for '||v_incomplete_job_cnt||' partition stats jobs to complete.',null,null,const_module);
     end loop;   

     begin
        DBMS_SCHEDULER.STOP_JOB ( v_job_name ); 
     exception
        when excep.x_job_not_running then
           null;
     end;
     
     begin
        DBMS_SCHEDULER.DROP_JOB ( v_job_name );
     exception
       when excep.x_unknown_job then
         null;
     end;     
   
     v_prism_prefix := metadata_utilities.get_prism_prefix;
     metadata_utilities.load_dd_tab_stats(v_prism_prefix);
     metadata_utilities.load_dd_tab_partitions(v_prism_prefix);     
     metadata_utilities.load_dd_tab_col_stats(v_prism_prefix);   
     
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr('Unexpected Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
  end gather_partition_stats_by_job;
 

  procedure gather_table_stats_by_job(p_actual_owner varchar2, p_table_name varchar2)
  is
     const_module        CONSTANT  varchar2(62) := 'metadata_utilities.gather_table_stats_by_job';
    
     v_job_name               varchar2(32);
     --NOT user_scheduler_jobs.job_name%type;
     --ORA-12899: value too large for column "SYS"."WRI$_OPTSTAT_OPR"."JOB_NAME" (actual: 42, maximum: 32)
     v_job_action             varchar2(4000);
     v_job_start_time         timestamp; 
     v_code                   number;
     v_errm                   varchar2(4000); 
     
  begin

     metadata_utilities.log('gathering table stats for: '||p_actual_owner||'.'||p_table_name,null,null,const_module);
        
     v_job_name   := substr('STATS_'||p_table_name,1,32);  -- restricted to 32 chars
        
     v_job_action := 'BEGIN dbms_stats.gather_table_stats( OWNNAME=>'    ||chr(39)|| p_actual_owner ||chr(39)||','||
                                                          'TABNAME=>'    ||chr(39)|| p_table_name   ||chr(39)||','||
                                                          'CASCADE=> FALSE );'  ||
                    ' END;';
        
     v_job_start_time := metadata_utilities.create_job ( v_job_name,v_job_action,null,null );

     metadata_utilities.merge_job_execution( v_job_name, 'TABLE_STATS');

  end gather_table_stats_by_job;

*/
   

  function get_job_start_date (p_job_name varchar2, p_ts_before_creation timestamp)
    return timestamp
  is
     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.get_job_start_date';
     v_code                number;
     v_errm                varchar2(4000);
     v_job_start_date      all_scheduler_jobs.start_date%type;
     v_job_name            all_scheduler_jobs.job_name%type;
     v_job_status          all_scheduler_jobs.state%type;
  begin
     begin

        select nvl(asj.start_date,jrd.actual_start_date), nvl(asj.job_name,jrd.job_name), nvl(asj.state,jrd.status) status
          into v_job_start_date,v_job_name,v_job_status
          from all_scheduler_job_run_details jrd
             left outer join all_scheduler_jobs asj on asj.job_name = jrd.job_name
         where ( jrd.job_name = p_job_name or asj.job_name = p_job_name )
           and jrd.actual_start_date > p_ts_before_creation;

        metadata_utilities.log('Job '||v_job_name||' started at '||to_char(v_job_start_date)||' and is currently '||v_job_status,v_code,v_errm,const_module);

     exception
        when no_data_found then 
           v_job_start_date := null;
           v_job_name       := null;
           v_job_status     := null;        
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
     end;

     return v_job_start_date;

  end get_job_start_date;


  function get_job_status (p_job_name varchar2)
    return varchar2
  is
     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.get_job_status';
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
                 full outer join all_scheduler_jobs asj on asj.job_name = jrd.job_name
             where ( jrd.job_name = p_job_name or asj.job_name = p_job_name )
             order by log_date desc
        )
        where rownum = 1;

        metadata_utilities.log('Job '||v_job_name||' is currently '||v_job_status,v_code,v_errm,const_module);

     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
     end;

     return v_job_status;

  end get_job_status;
  

  function get_job_errors (p_job_name varchar2)
    return varchar2
  is
     const_module   CONSTANT  varchar2(62) := 'metadata_utilities.get_job_errors';
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

        metadata_utilities.log(substr('Job '||p_job_name||' is '||v_job_status||' with errors: '||v_errors,1,4000),null,null,const_module);

     exception
        when others then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM, 1 , 4000);
           metadata_utilities.log(substr('Error: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,3900),v_code,v_errm,const_module);
     end;

     return v_errors;

  end get_job_errors; 
  

procedure swap_order (p_parent_table_actual_owner varchar2,p_parent_table_name varchar2,p_parent_table_order varchar2,
p_child_table_actual_owner varchar2,p_child_table_name varchar2,p_child_table_order varchar2) is

begin
  update ss_table_list_sort set table_order = p_child_table_order where actual_owner = p_parent_table_actual_owner and table_name = p_parent_table_name;
  update ss_table_list_sort set table_order = p_parent_table_order where actual_owner = p_child_table_actual_owner and table_name = p_child_table_name;
    
  update ss_graph_sort set parent_table_order = p_child_table_order where par_table_actual_owner = p_parent_table_actual_owner and parent_table = p_parent_table_name;
  update ss_graph_sort set child_table_order = p_parent_table_order where child_table_actual_owner = p_child_table_actual_owner and child_table = p_child_table_name;

  commit;
end swap_order;


procedure resolve_glob_seq_sort is

  cursor GetInvalidOrderItems is select par_table_actual_owner,parent_table,parent_table_order,child_table_actual_owner,child_table,child_table_order
                              from ss_graph_sort
                              where parent_table_order > child_table_order;
  v_nLoopCounter number;
begin


      delete   ss_table_list_sort;


      insert into ss_table_list_sort(table_id,owner,actual_owner,table_name,table_order)

      select table_id,owner,actual_owner,table_name,rownum table_order
      from
      (
        select table_id,owner,actual_owner,table_name ,max_tab_id,dbms_random.value(1,max_tab_id) rnd_order
        from
        (
          select table_id,owner,actual_owner,table_name,max(table_id) over (partition by 1) max_tab_id
          from
          (
            select rownum table_id,dt.owner,dt.actual_owner,dt.table_name
            from dd_tables dt
          )
        )  order by rnd_order -- random order
      );


      delete from ss_graph_sort;

      insert into ss_graph_sort(child_table_actual_owner ,child_table_owner,child_table , Par_table_actual_owner,Parent_table_owner,Parent_table)
      select  dc_child.ACTUAL_R_OWNER child_table_actual_owner,dc_child.R_OWNER child_table_owner,dc_child.table_name child_table,
      dc_parent.actual_owner parent_table_actual_owner,dc_parent.owner parent_table_owner,dc_parent.table_name parent_table
      from dd_tables dt_child
       join dd_constraints dc_child on dc_child.actual_owner  = dt_child.actual_owner and dc_child.table_name  = dt_child.table_name and dc_child.constraint_type = 'R'
       join dd_constraints dc_parent on dc_parent.constraint_name  = dc_child.r_constraint_name  and dc_parent.actual_owner  = dc_child.ACTUAL_R_OWNER and dc_parent.constraint_type = 'P'
       join dd_tables dt_parent on dt_parent.actual_owner = dc_parent.actual_owner and dt_parent.table_name = dc_parent.table_name;


      update ss_graph_sort sgs set child_table_order =
      (select table_order from ss_table_list_sort stls where stls.actual_owner = sgs.child_table_actual_owner and stls.table_name = sgs.child_table);

      update ss_graph_sort sgs set parent_table_order =
      (select table_order from ss_table_list_sort stls where stls.actual_owner = sgs.par_table_actual_owner and stls.table_name = sgs.parent_table);

    v_nLoopCounter := 1;
    loop
         for GetInvalidOrderItemsRec in GetInvalidOrderItems loop

            metadata_utilities.swap_order (GetInvalidOrderItemsRec.par_table_actual_owner,GetInvalidOrderItemsRec.parent_table,GetInvalidOrderItemsRec.parent_table_order,
                    GetInvalidOrderItemsRec.child_table_actual_owner,GetInvalidOrderItemsRec.child_table,GetInvalidOrderItemsRec.child_table_order);
            exit;
         null;
      end loop;
      v_nLoopCounter := v_nLoopCounter + 1;
      if v_nLoopCounter > 100000 then exit; end if;

    end loop;

  update dd_tables dt set cre_order =  (select table_order from ss_table_list_sort stls
  where stls.actual_owner = dt.actual_owner and stls.table_name = dt.table_name);


  commit;

end resolve_glob_seq_sort;

  procedure load_comp_list ( p_comp_list in varchar2,
                             p_part_list out varchar2)
  is
     const_module             CONSTANT  varchar2(62) := 'metadata_utilities.load_comp_list';

     v_code                   number;
     v_errm                   varchar2(4000);

     v_invalid_comp_list  varchar2(4000);
     v_partition_list     varchar2(4000);
     v_insert_ts          timestamp;
  begin

      merge into ss_companies x
      using ( select REGEXP_SUBSTR (p_comp_list,
                                 '[^,]+',
                                 1,
                                 LEVEL) AS comp_code
                from dual
              connect by REGEXP_SUBSTR (p_comp_list,
                                       '[^,]+',
                                        1,
                                        LEVEL) IS NOT NULL ) y
         on (x.comp_code = y.comp_code)
       when not matched
       then
          insert (comp_code) values (y.comp_code);
      commit;

     metadata_utilities.log('inserted '||sql%rowcount||' rows into ss_companies where not already existing',null,null,const_module);
     v_insert_ts := systimestamp;
     
     begin
        select listagg(comp_code, ',') WITHIN GROUP (ORDER BY comp_code) comp_list
          into v_invalid_comp_list
          from ss_companies sc
         where not exists ( select 1
                              from companies c
                             where c.comp_code = sc.comp_code );

         if v_invalid_comp_list is not null
         then
            raise x_company_does_not_exist;
         end if;
     exception
        when no_data_found
        then
           commit;
        when x_company_does_not_exist
        then
           v_code := SQLCODE;
           v_errm := SUBSTR(SQLERRM,1,4000);
           metadata_utilities.log('The following companies do not exist ' || v_invalid_comp_list,v_code,v_errm,const_module);
           rollback;
           RAISE_APPLICATION_ERROR(-20001,'The following companies do not exist' || v_invalid_comp_list);
     end;

     select listagg('P_'||comp_code, ',') WITHIN GROUP (ORDER BY comp_code) comp_list
       into v_partition_list
       from ss_companies;
      --where created_ts >= v_insert_ts;

     p_part_list := v_partition_list;

  end load_comp_list;
  

  function get_owner_from_actual_owner (p_actual_owner in varchar2)
     return varchar
  is
     const_module             CONSTANT  varchar2(62) := 'metadata_utilities.get_owner_from_actual_owner';
     v_owner             md_ddl.owner%type;
  begin
     begin
        select replace(p_actual_owner,substr(p_actual_owner,1,instr(p_actual_owner,'_',1)),null)
          into v_owner
          from dual;
     exception
        when no_data_found then
           metadata_utilities.log('Check input param p_actual_owner: '||p_actual_owner,SQLCODE,SQLERRM,const_module);
     end;

     return v_owner;
  end get_owner_from_actual_owner;

  function get_prism_prefix
     return varchar
  is
     const_module             CONSTANT  varchar2(62) := 'metadata_utilities.get_prism_prefix';
     v_prism_prefix           varchar2(100);
  begin
     begin
         select substr(username,1,instr(username,'_',1)-1)
           into v_prism_prefix
           from user_users;
           
         metadata_utilities.log('v_prism_prefix ' || v_prism_prefix,null,null,const_module);  
     exception
        when others then
           metadata_utilities.log('Unexpected Error: '||dbms_utility.format_error_backtrace(),SQLCODE,SQLERRM,const_module);
     end;

     return v_prism_prefix;

  end get_prism_prefix;

end metadata_utilities;
/