create or replace PACKAGE BODY gen_metadata
AS

  procedure add_partition_clause (p_src_schema varchar2) is

      const_module        CONSTANT  varchar2(62) := 'gen_metadata.add_partition_clause';
      v_code              number;
      v_errm              varchar2(4000);
      v_owner             varchar2(128);

      begin
      
      metadata_utilities.log('Running add_partition_clause ',null,null,const_module);

      -- Comp Reference Partitions

      v_owner := metadata_utilities.get_owner_from_actual_owner(p_src_schema);

      delete md_DDL where object_type = 'PARTITION' and ( ACTUAL_OWNER = p_src_schema or owner = v_owner );
      metadata_utilities.log('deleted ' || sql%rowcount || ' rows from md_ddl for '||p_src_schema||' TABLE ddl',null,null,const_module);

      insert into md_ddl (md_ddl_id,owner,object_type,object_name,base_object_name,object_ddl,object_ddl_length,object_xml,object_cre_seq,
      partitioning_type,subpartitioning_type,ref_ptn_constraint_name,view_name,has_large_object,actual_owner)

      select  md_seq.nextval md_ddl_id, md.owner,
      'PARTITION' object_type ,'NULL_COMP_CODE' object_name,md.object_name base_object_name,
      'ALTER TABLE '||md.actual_owner ||'.'||md.object_name ||' MODIFY PARTITION BY REFERENCE ("'||apt.ref_ptn_constraint_name||'")   (PARTITION "NULL_COMP_CODE" )'  object_ddl,
      null object_ddl_length ,null object_xml ,md.object_cre_seq ,apt.partitioning_type ,apt.subpartitioning_type,apt.ref_ptn_constraint_name, null view_name,null has_large_object,
      md.actual_owner
      from md_ddl md
      join all_part_tables apt on apt.owner = md.actual_owner and apt.table_name = md.object_name
      where md.object_type = 'TABLE'
      and md.actual_owner = p_src_schema
      and apt.partitioning_type = 'REFERENCE'
      and apt.subpartitioning_type = 'NONE';


      metadata_utilities.log('inserted ' || sql%rowcount || ' rows into md_ddl for Company REFERENCE/NONE partition tables '||p_src_schema||' TABLE ddl',null,null,const_module);

      -- Comp List/Range Partitions

      insert into md_ddl (md_ddl_id,owner,object_type,object_name,base_object_name,object_ddl,object_ddl_length,object_xml,object_cre_seq,
      partitioning_type,subpartitioning_type,ref_ptn_constraint_name,view_name,has_large_object,actual_owner)

      select  md_seq.nextval md_ddl_id, md1.owner,
      'PARTITION' object_type ,'NULL_COMP_CODE' object_name,res.object_name base_object_name,

      'ALTER TABLE '||res.actual_owner ||'.'||res.object_name ||' MODIFY PARTITION BY LIST ('||col_list||') SUBPARTITION BY RANGE ("EFFECTIVE_FROM_DATE")
          SUBPARTITION TEMPLATE (
          SUBPARTITION "P2017" VALUES LESS THAN ( TO_DATE('' 2017-01-01 00\:00\:00'', ''SYYYY-MM-DD HH24\:MI\:SS'', ''NLS_CALENDAR=GREGORIAN'') ),
          SUBPARTITION "P2018" VALUES LESS THAN ( TO_DATE('' 2018-01-01 00\:00\:00'', ''SYYYY-MM-DD HH24\:MI\:SS'', ''NLS_CALENDAR=GREGORIAN'') ),
          SUBPARTITION "P2019" VALUES LESS THAN ( TO_DATE('' 2019-01-01 00\:00\:00'', ''SYYYY-MM-DD HH24\:MI\:SS'', ''NLS_CALENDAR=GREGORIAN'') ),
          SUBPARTITION "P2020" VALUES LESS THAN ( TO_DATE('' 2020-01-01 00\:00\:00'', ''SYYYY-MM-DD HH24\:MI\:SS'', ''NLS_CALENDAR=GREGORIAN'') ),
          SUBPARTITION "P2021" VALUES LESS THAN ( TO_DATE('' 2021-01-01 00\:00\:00'', ''SYYYY-MM-DD HH24\:MI\:SS'', ''NLS_CALENDAR=GREGORIAN'') ),
          SUBPARTITION "DEFAULT_PART" VALUES LESS THAN ( MAXVALUE ) )  (PARTITION "NULL_COMP_CODE" VALUES ( NULL) )'  object_ddl,
          null object_ddl_length ,null object_xml ,md1.object_cre_seq  ,res.partitioning_type ,res.subpartitioning_type,md1.ref_ptn_constraint_name, null view_name,null has_large_object,md1.actual_owner

          from
               (
                  select md.object_type, md.actual_owner,md.object_name, md.base_object_name,apt.partitioning_type ,apt.subpartitioning_type,listagg('"'||apkc.column_name||'"', ',')   WITHIN GROUP  (ORDER BY table_name) col_list
                  from md_ddl md
                  join all_part_tables apt on apt.owner = md.actual_owner and apt.table_name = md.object_name 
                  join ALL_PART_KEY_COLUMNS apkc on  apt.owner = apkc.owner and  apt.table_name  = apkc.name
                  where md.object_type = 'TABLE'
                  and md.actual_owner = p_src_schema
                  and apt.partitioning_type = 'LIST'
                  and apt.subpartitioning_type = 'RANGE'
                  group by md.object_type, md.actual_owner,md.object_name, md.base_object_name,apt.partitioning_type ,apt.subpartitioning_type
              ) res
          join md_ddl md1 on md1.object_type = res.object_type and md1.actual_owner = res.actual_owner and md1.object_name = res.object_name ;

      metadata_utilities.log('inserted ' || sql%rowcount || ' rows into md_ddl for Company LIST/RANGE partition tables '||p_src_schema||' TABLE ddl',null,null,const_module);
          
      -- Comp List Partitions

      insert into md_ddl (md_ddl_id, owner,object_type,object_name,base_object_name,object_ddl,object_ddl_length,object_xml,object_cre_seq,
      partitioning_type,subpartitioning_type,ref_ptn_constraint_name,view_name,has_large_object,actual_owner)

      select  md_seq.nextval md_ddl_id, md1.owner,
      'PARTITION' object_type ,'NULL_COMP_CODE' object_name,res.object_name base_object_name,

      'ALTER TABLE '||res.actual_owner||'.'||res.object_name ||' MODIFY PARTITION BY LIST ('||col_list||')   (PARTITION "NULL_COMP_CODE" VALUES (NULL) )'  object_ddl,
          null object_ddl_length ,null object_xml ,md1.object_cre_seq ,res.partitioning_type ,res.subpartitioning_type,md1.ref_ptn_constraint_name, null view_name,null has_large_object,md1.actual_owner

          from
               (
                      select  md.object_type, md.actual_owner ,md.object_name, md.base_object_name,apt.partitioning_type ,apt.subpartitioning_type,listagg('"'||apkc.column_name||'"', ',')   WITHIN GROUP  (ORDER BY table_name) col_list
                      from md_ddl md
                      join all_part_tables apt on apt.owner = md.actual_owner and apt.table_name = md.object_name
                      join ALL_PART_KEY_COLUMNS apkc on  apt.owner = apkc.owner and  apt.table_name  = apkc.name
                      where md.object_type = 'TABLE'
                      and md.actual_owner  = p_src_schema
                      and apt.partitioning_type = 'LIST'
                      and apt.subpartitioning_type = 'NONE'
                      group by md.object_type, md.actual_owner,md.object_name,md.base_object_name,apt.partitioning_type ,apt.subpartitioning_type
              ) res
          join md_ddl md1 on md1.object_type = res.object_type and md1.actual_owner = res.actual_owner and md1.object_name = res.object_name ;

      metadata_utilities.log('inserted ' || sql%rowcount || ' rows into md_ddl for Company LIST/NONE partition tables '||p_src_schema||' TABLE ddl',null,null,const_module);

  commit;
    
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to add_partition_clause',v_code,v_errm,const_module);
end add_partition_clause;

procedure get_table_splits(p_src_schema varchar2)

is
   const_module        CONSTANT  varchar2(62) := 'gen_metadata.get_table_splits'; 
   v_code              number;
   v_errm              varchar2(4000);

   v_table_list varchar2(32000);
   v_table_count number;

    cursor cGetTables(p_src_schema varchar2) is
                          SELECT  count(*) num_tables,listagg(''''||res2.table_name||'''', ',')   WITHIN GROUP  (ORDER BY table_name) table_list
                          from
                          (
                            select res1.owner,res1.table_name,trunc((res1.table_id - 1)/ (max(res1.table_id) over (partition by res1.owner) /20),0)  table_group
                            from
                            (
                              select owner,table_name,rownum table_id
                              from
                              (
                                select distinct table_owner owner ,table_name from dba_tab_partitions atp
                                where partition_name in (select 'NULL_COMP_CODE' from dual)
                                and atp.table_owner = p_src_schema
                                union 
                                select actual_owner owner ,iot_name  from dd_tables dt where iot_type = 'IOT_OVERFLOW'
                              ) res
                            ) res1
                          ) res2 group by res2.table_group ;

begin

  metadata_utilities.log('Running get_table_splits  ' ||p_src_schema,null,null,const_module);

  delete from  ss_table_splits where schema_name = p_src_schema;

  metadata_utilities.log('deleted ' || sql%rowcount || ' rows from ss_table_splits for '||p_src_schema||' TABLE ddl',null,null,const_module);

  v_table_count := 0;

  for cGetTablesRec in cGetTables(p_src_schema) loop
    if  v_table_count + cGetTablesRec.num_tables > 500 then
      insert into ss_table_splits(schema_name,table_list,num_tables,aseq)
      values (p_src_schema,v_table_list,v_table_count,tab_split_seq.nextval);
      
      metadata_utilities.log('inserted ' || sql%rowcount || ' rows into  ss_table_splits for '||p_src_schema||' TABLE ddl',null,null,const_module);

      v_table_count := 0;
      v_table_list := null;
    end if;

    v_table_count := v_table_count + cGetTablesRec.num_tables;

    if v_table_list is not null then
      v_table_list := v_table_list||',';
    end if;

    v_table_list := v_table_list||cGetTablesRec.table_list;

  end loop;

  if v_table_list is not null then
    insert into ss_table_splits(schema_name,table_list,num_tables,aseq)
    values (p_src_schema,v_table_list,v_table_count,tab_split_seq.nextval);
    metadata_utilities.log('inserted ' || sql%rowcount || ' rows into  ss_table_splits for '||p_src_schema||' TABLE ddl',null,null,const_module);

  end if;

  commit;

exception
   when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to get_table_splits',v_code,v_errm,const_module);
      
end get_table_splits;

procedure load_ddl_exclusions is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.load_ddl_exclusions';
   v_code              number;
   v_errm              varchar2(4000);
   
begin

  metadata_utilities.log('Running load_ddl_exclusions for ' ,null,null,const_module);

  delete from md_obj_ddl_exclusions;

--  insert into md_obj_ddl_exclusions(owner,actual_owner,object_type,object_name )   
--  select owner,actual_owner,'TABLE',table_name from dd_tables  where iot_type = 'IOT_OVERFLOW';
  
  commit;

exception
   when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to load_ddl_exclusions',v_code,v_errm,const_module);

end load_ddl_exclusions;
  

procedure get_dp_table_metadata

is
   const_module        CONSTANT  varchar2(62) := 'gen_metadata.get_dp_table_metadata';
   v_code              number;
   v_errm              varchar2(4000);
   v_nCount            number;
   
   cursor cGetTabs is 
              select owner,actual_owner,table_name from dd_tables
              minus
              select distinct table_owner owner , actual_table_owner actual_owner ,table_name from dd_tab_partitions atp
              where partition_name in (select 'NULL_COMP_CODE' from dual)
              minus
              select owner   , actual_owner  ,object_name table_name from md_obj_ddl_exclusions modex
              where modex.object_type = 'TABLE'
              minus
              select owner,actual_owner ,iot_name  from dd_tables dt where iot_type = 'IOT_OVERFLOW';
   
begin

    metadata_utilities.log('Running get_dp_table_metadata  ',null,null,const_module);

    load_ddl_exclusions;

    delete from md_ddl where object_type = 'TABLE' and dp_YN = 'Y';
    
    metadata_utilities.log('deleted ' || sql%rowcount || ' rows from md_ddl',null,null,const_module);

    dbms_metadata.set_transform_param(dbms_metadata.session_transform , 'REF_CONSTRAINTS', FALSE);

    v_nCount := 0;

    for cGetTabsRec in cGetTabs loop
        
       begin
       
         insert into md_ddl (md_ddl_id,owner,actual_owner,object_type,object_name,object_ddl,dp_YN)
         values  (md_seq.nextval,cGetTabsRec.owner,cGetTabsRec.actual_owner,'TABLE',cGetTabsRec.table_name ,
              DBMS_METADATA.GET_DDL('TABLE',cGetTabsRec.table_name,cGetTabsRec.actual_owner),'Y'  ) ;
          
          if mod(v_nCount,500) = 0 then
            metadata_utilities.log('running get_dp_table_metadata for '||cGetTabsRec.owner||' .......',null,null,const_module);
          end if;
       
          v_nCount :=     v_nCount  + 1;
           
       exception when others then
          
          insert into md_ddl (md_ddl_id,owner,actual_owner,object_type,object_name,object_ddl,dp_YN)
          values  (md_seq.nextval,cGetTabsRec.owner,cGetTabsRec.actual_owner,'TABLE',
                   cGetTabsRec.table_name,'UNRESOLVED','Y') ;
              
          commit;     
       end;
       
    end loop;
              
    dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'REF_CONSTRAINTS', TRUE);
    
    commit;
    
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to get_dp_table_metadata',v_code,v_errm,const_module);     

end get_dp_table_metadata;


procedure remove_md_sys_c_cons
is

  const_module           CONSTANT  varchar2(62) := 'gen_metadata.remove_md_sys_c_cons';

  cursor c_sys_c_cons
  is
     select md_ddl_id,
            actual_owner,
            object_name,
            object_ddl, 
            length(object_ddl) ddl_length, 
            length('CONSTRAINT "SYS_C00123456"') common_sys_c_cons_length,
            REGEXP_COUNT(object_ddl, 'CONSTRAINT "SYS_C[[:digit:]]*"{1}') instances_of_sys_c,
            REGEXP_REPLACE(object_ddl, 'CONSTRAINT "SYS_C[[:digit:]]*"{1}',null) new_ddl,
            length(REGEXP_REPLACE(object_ddl, 'CONSTRAINT "SYS_C[[:digit:]]*"{1}',null)) new_ddl_length    
       from md_ddl 
      where object_type = 'TABLE' 
        and object_ddl like'%CONSTRAINT "SYS_C%';
    
   
  v_code             number;
  v_errm             varchar2(4000);    
  v_expected_length  number;
   
begin
  begin
    for r in c_sys_c_cons
    loop
   
       update md_ddl  
          set object_ddl = r.new_ddl
        where md_ddl_id = r.md_ddl_id; 
       
       metadata_utilities.log(const_module||': Updated object_ddl for md_ddl_id = '||r.md_ddl_id||'('||sql%rowcount||' row updated) with '||r.instances_of_sys_c||' instances of SYS_C generated constraint names removed from table ddl for '||r.actual_owner||','||r.object_name,null,null,const_module);

       v_expected_length := r.ddl_length -(r.common_sys_c_cons_length*r.instances_of_sys_c);
    
       if r.new_ddl_length <> v_expected_length
       then
          metadata_utilities.log(const_module||': WARNING Inspect newly generated DDL with md_ddl_id: '||to_char(r.md_ddl_id)||': New table DDL with SYS_C replacements of length '||to_char(r.new_ddl_length)||' differs from expected length of '||to_char(v_expected_length)||' based on '||r.instances_of_sys_c||' instances of SYS_C constraint names with common SYS_C length of '||r.common_sys_c_cons_length,-20999,'WARNING: x_unexpected_new_ddl_length',const_module);
       end if;
       
       commit;
       
    end loop;   

  exception 
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM, 1 , 4000);
        metadata_utilities.log(substr('Unexpected Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
  end;

end remove_md_sys_c_cons;


procedure get_table_metadata(p_src_schema varchar2,p_part_list varchar2)

is
   v_handle           number;
   v_handle2          number;
   v_transform_handle number;
   xmldoc             xmltype;
   l_ddl              clob;
   v_Owner            varchar2(128);
   v_Object_name      varchar2(128);
   v_xml_type         varchar2(60);
   l_ddl2             clob;
   
   v_code              number;
   v_errm              varchar2(4000);
   
   v_nCount number;
   v_job_name               varchar2(32);
   v_job_action             varchar2(4000);   

  cursor cGetTables(p_schema_name varchar2)
  is
     select table_list
       from ss_table_splits
      where schema_name = p_schema_name;

   v_table_list varchar2(32000);

   parsed_items  sys.ku$_parsed_items;
   object_type_path  VARCHAR2(4000);

   const_module           CONSTANT  varchar2(62) := 'gen_metadata.get_table_metadata';

begin

  --dbms_output.put_line('Processing '||p_src_schema);

  metadata_utilities.log('Calling get_table_metadata for ' ||p_src_schema,null,null,const_module);
  v_owner := metadata_utilities.get_owner_from_actual_owner(p_src_schema);  
  get_table_splits(p_src_schema);

  delete md_DDL where object_type = 'TABLE' and ACTUAL_OWNER = p_src_schema;
  
  metadata_utilities.log('deleted ' || sql%rowcount || ' rows from md_ddl for '||p_src_schema||' TABLE ddl',null,null,const_module);

  for cGetTablesRec in cGetTables(p_src_schema) loop
  
     v_handle := dbms_metadata.open(object_type    => 'TABLE');

      dbms_metadata.set_filter( v_handle, 'SCHEMA', p_src_schema);

--    if cGetTablesRec.table_list is null then
--      dbms_metadata.close(v_handle);
--      -- raise issue
--      return;
--    end if;

    v_table_list := 'IN ('||cGetTablesRec.table_list||')';

    dbms_output.put_line(substr(v_table_list,1,4000));
    metadata_utilities.log(substr('processing table list: ' ||v_table_list||' for '||p_src_schema||' TABLE ddl',1,4000),null,null,const_module);

    dbms_metadata.set_filter(v_handle,'NAME_EXPR',v_table_list);

    v_transform_handle := dbms_metadata.add_transform(v_handle, 'SXML');

    v_nCount := 0;
    
    loop
    
        if mod(v_nCount,500) = 0 then
            metadata_utilities.log('running get_table_metadata for '||p_src_schema||' .......',null,null,const_module);
        end if;

        dbms_lob.createtemporary(l_ddl2, true);

        dbms_metadata.FETCH_XML_CLOB(v_handle,l_ddl2,parsed_items,object_type_path);
        exit when l_ddl2 is null;

        dbms_lob.createtemporary(l_ddl, true);
        v_handle2 := dbms_metadata.openw(object_type => 'TABLE');
        v_transform_handle := dbms_metadata.add_transform(v_handle2, 'SXMLDDL');
        dbms_metadata.set_transform_param(v_transform_handle, 'STORAGE', FALSE);
        dbms_metadata.set_transform_param(v_transform_handle, 'TABLESPACE', TRUE);
        dbms_metadata.set_transform_param(v_transform_handle, 'SEGMENT_ATTRIBUTES', FALSE);
        dbms_metadata.set_transform_param(v_transform_handle, 'PARTITIONING', FALSE);
       -- REF_CONSTRAINTS must exist otherwise ORA-14655: reference partitioning constraint not found
        dbms_metadata.set_transform_param(v_transform_handle, 'REF_CONSTRAINTS', FALSE);
        dbms_metadata.convert(v_handle2, XMLTYPE(l_ddl2), l_ddl);
        dbms_metadata.close(v_handle2);

        INSERT INTO md_ddl(md_DDL_ID,actual_owner,owner,object_type,object_name,object_ddl,object_cre_seq,object_ddl_length,OBJECT_XML )
        values (md_seq.nextval,p_src_schema,v_Owner ,'TABLE',v_Object_name,l_ddl,0,dbms_lob.getlength(l_ddl),XMLTYPE(l_ddl2)); --Orig_ddl1

        --metadata_utilities.log('inserted ' || sql%rowcount || ' rows into md_ddl for '||p_src_schema||' TABLE ddl',null,null,const_module);

        dbms_lob.freetemporary(l_ddl2);

     end loop;

      update md_ddl
         set object_name = xmlquery(
                                     'declare default element namespace "http://xmlns.oracle.com/ku"; (: :)
                                         /TABLE/NAME/text()'
                                          passing object_xml
                                          returning content
                                          ).getStringVal()
       where object_type = 'TABLE'
         and actual_owner = p_src_schema;

      metadata_utilities.log('updated object_name for ' || sql%rowcount || ' TABLE md_ddl rows in '||p_src_schema,null,null,const_module);

      dbms_metadata.close(v_handle);

      commit;

  end loop;

  merge into md_ddl md
      using (
        select md.actual_owner ,md.object_name ,apt.partitioning_type, apt.subpartitioning_type,apt.ref_ptn_constraint_name
        from md_ddl md
        join all_part_tables apt on apt.owner = md.actual_owner and apt.table_name = md.object_name
        where md.object_type = 'TABLE'
      ) res  on (res.actual_owner = md.actual_owner and res.object_name = md.object_name and md.object_type = 'TABLE')
      when matched then update set
      md.partitioning_type = res.partitioning_type, md.subpartitioning_type =  res.subpartitioning_type,md.ref_ptn_constraint_name =  res.ref_ptn_constraint_name;

  -- run asynchronous job to generate table partition stats for each schema in parallel with remaining steps below
  v_job_name   := substr('PTN_STATS_'||upper(p_src_schema),1,32);
  v_job_action := 'BEGIN  metadata_utilities.gather_schema_partition_stats('||chr(39)||p_src_schema||chr(39)||','||chr(39)||p_part_list||chr(39)||');  END;';
  metadata_utilities.create_job(v_job_name,v_job_action,null,null);

  pop_trigger_ddl(p_src_schema);
  pop_index_ddl(p_src_schema);
  pop_object_grant_ddl(p_src_schema);
  add_partition_clause(p_src_schema);
  build_load_views(p_src_schema,p_part_list);
  
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to get_table_metadata',v_code,v_errm,const_module);

end get_table_metadata;


--OBSOLETE - used if calling remotely accross dblink - but didn't work due to:ORA-14551: cannot perform a DML operation inside a query
--function  f_get_table_metadata(p_src_schema varchar2,p_part_list varchar2) return number is
--  const_module           CONSTANT  varchar2(62) := 'gen_metadata.f_get_table_metadata';
--begin
--  get_table_metadata(p_src_schema,p_part_list);
--  return 1;
--end;


procedure pop_obj_metadata_xmlddl ( p_schema_prefix   varchar2,
                                    p_object_type     varchar2 )
is
   v_code              number;
   v_errm              varchar2(4000);

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_obj_metadata_xmlddl';

   cursor c_users (cin_src_prefix in varchar2)
   is
      select username
        from dba_users
       where username like cin_src_prefix||'\_'||'%' escape '\';

begin

   for r_users in c_users(p_schema_prefix)
   loop
      gen_metadata.pop_user_obj_metadata_xmlddl (r_users.username, p_object_type);
   end loop;
   
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to pop_obj_metadata_xmlddl',v_code,v_errm,const_module);

end pop_obj_metadata_xmlddl;


procedure pop_user_obj_metadata_xmlddl ( p_src_schema      varchar2,
                                         p_object_type     varchar2 )
is

   --PRAGMA AUTONOMOUS_TRANSACTION;

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_user_obj_metadata_xmlddl';
   v_code              number;
   v_errm              varchar2(4000);
   v_handle            number;
   v_handle2           number;
   v_transform_handle  number;
   v_Object_name       varchar2(128);
   v_table_query       sys.xmltype;
   xmldoc              sys.xmltype;
   l_ddl               clob;


begin

   v_handle := dbms_metadata.open(object_type    => p_object_type);

   dbms_metadata.set_filter( v_handle, 'SCHEMA', p_src_schema);
   --dbms_metadata.set_filter( v_handle, 'NAME', '');
   dbms_metadata.set_filter(v_handle,'NAME_EXPR','like ''%'' ');
   v_transform_handle := dbms_metadata.add_transform(v_handle, 'SXML');

   loop
     begin
       xmldoc := dbms_metadata.fetch_xml(v_handle);
       exit when xmldoc is null;

       dbms_lob.createtemporary(l_ddl, true);
       v_handle2 := dbms_metadata.openw(object_type => p_object_type);
       v_transform_handle := dbms_metadata.add_transform(v_handle2, 'SXMLDDL');
       dbms_metadata.convert(v_handle2, xmldoc, l_ddl);

       begin
         select extractValue(xmldoc, '/'||p_object_type||'/NAME', 'xmlns="http://xmlns.oracle.com/ku"')
           into v_object_name
           from dual;
       exception
        when others then
          null;
       end;

--       select xmlquery(
--                       'declare default element namespace "http://xmlns.oracle.com/ku"; (: :)
--                       /'||xmldoc||'/NAME/text()'
--                       passing xmldoc
--                       returning content
--                       ).getStringVal() name
--         into v_object_name
--         from dual;

        delete md_ddl
         where owner = p_src_schema
           and object_type = p_object_type
           and object_name = v_object_name;

        metadata_utilities.log('deleted ' || sql%rowcount || ' rows from md_ddl for '||p_src_schema,null,null,const_module);

       insert into md_ddl (md_ddl_id,owner,object_type,object_name,object_ddl,object_ddl_length,object_xml)
       values ( md_seq.nextval,p_src_schema,p_object_type,v_object_name,l_ddl,dbms_lob.getlength(l_ddl),xmldoc);
       commit;

      metadata_utilities.log('inserted ' || sql%rowcount || ' rows into  md_ddl for '||p_src_schema,null,null,const_module);
      
       dbms_lob.freetemporary(l_ddl);
       dbms_metadata.close(v_handle2);
     exception
       when others then
         dbms_lob.freetemporary(l_ddl);
         dbms_metadata.close(v_handle2);
         raise;
     end;
   end loop;
   dbms_metadata.close(v_handle);

exception
   when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      RAISE;
end pop_user_obj_metadata_xmlddl;


procedure pop_obj_metadata_getddl ( p_schema_prefix   varchar2,
                                    p_object_type     varchar2 )
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_obj_metadata_xmlddl';

   v_code              number;
   v_errm              varchar2(4000);
   
   cursor c_users (cin_src_prefix in varchar2)
   is
      select username
        from dba_users
       where username like cin_src_prefix||'\_'||'%' escape '\';

begin
   for r_users in c_users(p_schema_prefix)
   loop
      if gen_metadata.pop_user_obj_metadata_getddl (r_users.username,p_object_type,null,true) then
        null;
      end if;
   end loop;
   
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to pop_obj_metadata_getddl',v_code,v_errm,const_module);
end pop_obj_metadata_getddl;


function pop_user_obj_metadata_getddl ( p_src_schema      varchar2,
                                        p_object_type    varchar2,
                                        p_object_name    varchar2 default null,
                                        p_commit         boolean default true) return boolean
is

   PRAGMA AUTONOMOUS_TRANSACTION;

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_user_obj_metadata_getddl';
   v_code              number;
   v_errm              varchar2(4000);

   cursor c1 (cin_src_schema varchar2, cin_object_type varchar2, cin_object_name varchar2) is
     select object_id, owner, object_type, object_name
       from all_objects
      where owner = cin_src_schema
        and object_type = cin_object_type
        and object_name = nvl(cin_object_name,object_name);

begin

   for r in c1(p_src_schema, p_object_type, p_object_name)
   loop
      begin

        delete md_ddl
         where owner = p_src_schema
           and object_type = p_object_type
           and object_name = r.object_name;

         insert into md_ddl (md_ddl_id,owner,object_type,object_name,object_ddl)
            select md_seq.nextval,p_src_schema,p_object_type,r.object_name,
                   DBMS_METADATA.GET_DDL(r.object_type,r.object_name,r.owner) ddl
              from dual;

          if p_commit then
              commit;
          end if;
         commit;
      exception
         when others then
            if p_commit then
              rollback;
            end if;
            v_code := SQLCODE;
            v_errm := SUBSTR(SQLERRM,1,4000);
            metadata_utilities.log('1 Failed during metadata generation',v_code,v_errm,const_module);
            return  false;
      end;
   end loop;

   update md_ddl
      set object_ddl_length = dbms_lob.getlength(object_ddl)
    where owner = p_src_schema
      and object_type = p_object_type;

  return true;
  if p_commit = true then
    commit;
  end if;
exception
   when others then
      if p_commit = true then
        rollback;
      end if;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('2 Failed during metadata generation',v_code,v_errm,const_module);
     RAISE;
     return false;
     
end pop_user_obj_metadata_getddl;

procedure pop_trigger_ddl ( p_src_schema varchar2, p_object_name varchar2 default null)
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_trigger_ddl';
   v_code              number;
   v_errm              varchar2(4000);

begin
   begin

      delete md_ddl where actual_owner = p_src_schema and object_type = 'TRIGGER' and object_name = nvl(p_object_name,object_name);
      metadata_utilities.log('deleted ' || sql%rowcount || ' TRIGGER rows from md_ddl for ' || p_src_schema||' '||p_object_name,null,null,const_module);

      insert into md_ddl (md_ddl_id,actual_owner,owner,object_type,base_object_name,object_name,object_ddl,object_ddl_length,object_xml)
         select md_seq.nextval,
                trg.owner,
                replace(trg.owner,substr(trg.owner,1,instr(trg.owner,'_',1)),null),
                'TRIGGER',
                trg.table_name,
                trg.trigger_name,                
                DBMS_METADATA.get_ddl ('TRIGGER', trg.trigger_name, trg.owner),
                null,
                null
           from dba_triggers trg join md_ddl mdd on mdd.actual_owner = trg.owner and mdd.object_name = trg.table_name
          where trg.owner = p_src_schema
            and mdd.object_type = 'TABLE'
            and trg.trigger_name = nvl(p_object_name,trg.trigger_name);

    metadata_utilities.log('inserted '||sql%rowcount||' TRIGGER rows into md_ddl for ' ||p_src_schema||' '||p_object_name,null,null,const_module);

    update md_ddl
       set object_ddl = regexp_replace(object_ddl,'.*$',null,instr(object_ddl,'ALTER TRIGGER',1),1,'m')
     where object_type = 'TRIGGER'
       and actual_owner = p_src_schema
       and object_name = nvl(p_object_name,object_name);

    metadata_utilities.log('updated ' || sql%rowcount || ' rows in md_ddl to remove ALTER TRIGGER statement',null,null,const_module);

    update md_ddl
       set object_ddl_length = dbms_lob.getlength(object_ddl)
     where object_type = 'TRIGGER'
       and actual_owner = p_src_schema
       and object_name = nvl(p_object_name,object_name);
       
    metadata_utilities.log('updated ' || sql%rowcount || ' rows in md_ddl to set TRIGGER ddl object_length',null,null,const_module);       
       
    commit;
   
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to pop_trigger_ddl',v_code,v_errm,const_module);
  end;
end pop_trigger_ddl;


procedure add_local_index_ddl( p_src_schema varchar2, p_object_name varchar2 default null )
is

   v_index_list       varchar2(32000);
   v_owner             md_ddl.owner%type;
   v_code              number;
   v_errm              varchar2(4000);

   parsed_items       sys.ku$_parsed_items;
   object_type_path   varchar2(4000);

   const_module           CONSTANT  varchar2(62) := 'gen_metadata.add_local_index_ddl';
   v_handle           number;
   v_handle2          number;
   v_transform_handle number;
   xmldoc             xmltype;
   l_ddl              clob;
   v_Object_name      varchar2(128);
   v_xml_type         varchar2(60);
   l_ddl2             clob;
   v_sql              varchar2(1000);
   
   cursor cGetLocalIndList
   is
      select table_name, listagg(''''||index_name||'''',',') WITHIN GROUP  (ORDER BY index_name) index_list
        from (
               select distinct ind.table_name, ind.index_name
                 from dba_indexes ind
                 join md_ddl mdd on mdd.actual_owner = ind.owner and mdd.object_name = ind.table_name
                 left outer join dba_part_indexes pti on pti.owner = ind.owner and pti.index_name = ind.index_name
                where mdd.object_type = 'TABLE'
                  and pti.locality = 'LOCAL'
                  and ind.owner = p_src_schema
                  and ind.index_type <> 'LOB' 
                  and ind.index_name not like'SYS_IL%'
                  and ind.index_name = nvl(p_object_name,ind.index_name) )
      group by table_name;

begin

   v_owner := metadata_utilities.get_owner_from_actual_owner(p_src_schema);
   metadata_utilities.log('Processing LOCAL indexes for table md_ddl in '||v_owner ||'('||p_src_schema||')',null,null,const_module);

   for r in cGetLocalIndList loop

      v_handle := dbms_metadata.open(object_type    => 'INDEX');

      dbms_metadata.set_filter( v_handle, 'SCHEMA', p_src_schema);
      --dbms_metadata.set_filter( v_handle, 'BASE_OBJECT_NAME',r.table_name);
      v_index_list := 'IN ('||r.index_list||')';

      v_sql := 'delete md_ddl where actual_owner = '||chr(39)||p_src_schema||chr(39)||' and owner = '||chr(39)||v_owner||chr(39)||' and object_type = '||chr(39)||'INDEX'||chr(39)||' and base_object_name = '||chr(39)||r.table_name||chr(39)||' and object_name in ('||r.index_list||')';
      metadata_utilities.log('executing: ' ||v_sql,null,null,const_module);
      execute immediate v_sql;
      metadata_utilities.log('deleted ' || sql%rowcount || ' INDEX rows from md_ddl for ' || p_src_schema||' ,table '||r.table_name||' index_list '||r.index_list,null,null,const_module);
      metadata_utilities.log(substr('processing local index list: ' ||v_index_list||' for '||p_src_schema||'.'||r.table_name||' LOCAL INDEX ddl',1,4000),null,null,const_module);

      dbms_metadata.set_filter(v_handle,'NAME_EXPR',v_index_list);

      v_transform_handle := dbms_metadata.add_transform(v_handle, 'SXML');

      loop

         dbms_lob.createtemporary(l_ddl2, true);

         dbms_metadata.FETCH_XML_CLOB(v_handle,l_ddl2,parsed_items,object_type_path);
         exit when l_ddl2 is null;

         dbms_lob.createtemporary(l_ddl, true);
         v_handle2 := dbms_metadata.openw(object_type => 'INDEX');
         v_transform_handle := dbms_metadata.add_transform(v_handle2, 'SXMLDDL');
         dbms_metadata.set_transform_param(v_transform_handle, 'STORAGE', FALSE);
         dbms_metadata.set_transform_param(v_transform_handle, 'TABLESPACE', TRUE);
         dbms_metadata.set_transform_param(v_transform_handle, 'SEGMENT_ATTRIBUTES', FALSE);
         dbms_metadata.set_transform_param(v_transform_handle, 'PARTITIONING', FALSE);
         dbms_metadata.set_transform_param(v_transform_handle, 'PRESERVE_LOCAL', TRUE);
         dbms_metadata.convert(v_handle2, XMLTYPE(l_ddl2), l_ddl);
         dbms_metadata.close(v_handle2);

         insert into md_ddl(md_ddl_id,
                            actual_owner,
                            owner,
                            object_type,
                            object_name,
                            base_object_name,
                            object_ddl,
                            object_cre_seq,
                            object_ddl_length,
                            object_xml )
           values (md_seq.nextval,
                   p_src_schema,
                   v_owner,
                   'INDEX',
                   'LOCAL',
                   r.table_name,
                   l_ddl,
                   0,
                   dbms_lob.getlength(l_ddl),
                   XMLTYPE(l_ddl2));

        metadata_utilities.log('inserted ' || sql%rowcount || ' rows into md_ddl for '||p_src_schema||'.'||r.table_name||' LOCAL INDEX ddl',null,null,const_module);

        dbms_lob.freetemporary(l_ddl2);

      end loop;

      update md_ddl
         set object_name = xmlquery(
                                     'declare default element namespace "http://xmlns.oracle.com/ku"; (: :)
                                         /INDEX/NAME/text()'
                                          passing object_xml
                                          returning content
                                          ).getStringVal()
       where object_type = 'INDEX'
         and object_name = 'LOCAL'
         and actual_owner = p_src_schema
         and object_xml is not null;

      metadata_utilities.log('updated object_name for ' || sql%rowcount || ' LOCAL INDEX md_ddl rows in '||p_src_schema,null,null,const_module);

      dbms_metadata.close(v_handle);

      commit;

   end loop;
   
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to add_local_index_ddl',v_code,v_errm,const_module);

end add_local_index_ddl;


procedure pop_index_ddl ( p_src_schema varchar2, p_object_name varchar2 default null )
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_index_ddl';
   v_code              number;
   v_errm              varchar2(4000);
   v_count             number := 0;
   v_owner             md_ddl.owner%type;

begin

   v_owner := metadata_utilities.get_owner_from_actual_owner(p_src_schema);

   delete md_ddl where actual_owner = p_src_schema and object_type = 'INDEX' and object_name = nvl(p_object_name,object_name);
   metadata_utilities.log('deleted ' || sql%rowcount || ' INDEX rows from md_ddl for ' || p_src_schema||' '||p_object_name,null,null,const_module);

   DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);

   insert into md_ddl (md_ddl_id,actual_owner,owner,object_type,object_name,base_object_name,object_ddl,object_ddl_length,object_xml)
      select md_seq.nextval,
             p_src_schema,
             v_owner,
             'INDEX',
             ind.index_name,
             mdd.object_name,
             DBMS_METADATA.get_ddl ('INDEX', ind.index_name, ind.owner),
             null,
             null
        from dba_indexes ind
        join md_ddl mdd on mdd.actual_owner = ind.owner and mdd.object_name = ind.table_name
        left outer join dba_part_indexes pti on pti.owner = ind.owner and pti.index_name = ind.index_name
       where mdd.object_type = 'TABLE'
         and (pti.locality <> 'LOCAL' or pti.locality IS NULL)
         and ind.owner = p_src_schema
         and ind.index_type <> 'LOB'
         and ind.index_name not like'SYS_IL%'
         and ind.index_name = nvl(p_object_name,ind.index_name);

   v_count := sql%rowcount;
   commit;
   metadata_utilities.log('inserted '||v_count||' GLOBAL INDEX rows into md_ddl for ' ||p_src_schema||' '||p_object_name,null,null,const_module);

   -- Now generate local indexes (excluding all partitions, but preserving LOCAL keyword, so built for each subset partition)
   add_local_index_ddl(p_src_schema);

   commit;
   
   exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to pop_index_ddl',v_code,v_errm,const_module);

end pop_index_ddl;


procedure pop_object_grant_ddl ( p_src_schema varchar2 )
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_object_grant_ddl';
   v_code              number;
   v_errm              varchar2(4000);
   v_counter           number := 0;
   v_total_count       number := 0;

--   cursor c_users (cin_src_prefix in varchar2)
--   is
--      select username
--        from dba_users
--       where username like cin_src_prefix||'\_'||'%' escape '\';

   cursor c_get_obj_grant_ddl (cin_username varchar2)
   is
      select tp.owner actual_owner, md.owner, 'OBJECT_GRANT' object_type,
             tp.table_name, tp.grantee, tp.grantor, tp.privilege,
             'GRANT '||tp.privilege||' ON '||tp.grantor||'.'||tp.table_name||' TO '||tp.grantee AS ddl
        from dba_tab_privs tp join md_ddl md on  md.actual_owner = tp.owner
                                            and tp.table_name = md.object_name
       where tp.grantee = cin_username
         and md.object_type = 'TABLE';

begin

   delete md_ddl_parts
    where md_ddl_id in ( select md_ddl_id
                           from md_ddl
                          where object_type = 'OBJECT_GRANT'
                            and actual_owner = p_src_schema );

   metadata_utilities.log('deleted all ' || sql%rowcount || ' OBJECT_GRANT rows from md_ddl_parts for '||p_src_schema,null,null,const_module);

   delete md_ddl where object_type = 'OBJECT_GRANT' and actual_owner = p_src_schema;
   metadata_utilities.log('deleted all ' || sql%rowcount || ' OBJECT_GRANT rows from md_ddl for '||p_src_schema,null,null,const_module);

--   for r_users in c_users(p_owner_prefix)
--   loop

      v_counter := 0;

      for r_ddl in c_get_obj_grant_ddl (p_src_schema)
      loop

         insert into md_ddl (md_ddl_id,actual_owner,owner,object_type,object_name,object_ddl,object_ddl_length,object_xml)
         values ( md_seq.nextval,
                  r_ddl.actual_owner,
                  --replace(r_ddl.actual_owner,p_owner_prefix||'_',null),
                  r_ddl.owner,
                  r_ddl.object_type,
                  r_ddl.table_name,
                  r_ddl.ddl,
                  length(r_ddl.ddl),
                  null);

         v_counter := v_counter + 1;
        -- metadata_utilities.log('inserted ' ||r_ddl.object_type||' into md_ddl for ' ||r_ddl.privilege||' on '||r_ddl.grantor||'.'||r_ddl.table_name,null,null,const_module);

      end loop;

      metadata_utilities.log('inserted '||v_counter||' OBJECT_GRANT rows into md_ddl for ' ||p_src_schema,null,null,const_module);
      v_total_count := v_total_count + v_counter;
   --end loop;

   --metadata_utilities.log('Total of '||v_counter||' OBJECT_GRANT md_ddl records created for ' ||p_src_schema,null,null,const_module);
   --metadata_utilities.log('Total of '||v_total_count||' OBJECT_GRANT md_ddl records created',null,null,const_module);

   commit;

exception
   when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log(substr('Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      RAISE;
end pop_object_grant_ddl;


procedure pop_user_granted_ddl ( p_owner_prefix varchar2)
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_user_granted_ddl';
   v_code              number;
   v_errm              varchar2(4000);
   t_strings gen_metadata.string_list_4000;

   cursor c_users (cin_src_prefix in varchar2)
   is
      select username
        from dba_users
       where username like cin_src_prefix||'\_'||'%' escape '\';

   cursor c_get_granted_ddl (cin_username varchar2)
   is
      select 'USER' object_type, dbms_metadata.get_ddl('USER', u.username)||';' AS ddl
      from   dba_users u
      where  u.username = cin_username
      union all
      select 'TABLESPACE_QUOTA' object_type, dbms_metadata.get_granted_ddl('TABLESPACE_QUOTA', tq.username)||';' AS ddl
      from   dba_ts_quotas tq
      where  tq.username = cin_username
      and    rownum = 1
      union all
      select 'ROLE_GRANT' object_type, LTRIM(REPLACE(dbms_metadata.get_granted_ddl('ROLE_GRANT', rp.grantee)||';',chr(10),';'||chr(10)),';'||chr(10)) AS ddl
      from   dba_role_privs rp
      where  rp.grantee = cin_username
      and    rownum = 1
      union all
      select 'SYSTEM_GRANT' object_type, LTRIM(REPLACE(dbms_metadata.get_granted_ddl('SYSTEM_GRANT', sp.grantee)||';',chr(10),';'||chr(10)),';'||chr(10)) AS ddl
      from   dba_sys_privs sp
      where  sp.grantee = cin_username
      and    rownum = 1
      union all
      select 'OBJECT_GRANT' object_type, LTRIM(REPLACE(dbms_metadata.get_granted_ddl('OBJECT_GRANT', tp.grantee)||';',chr(10),';'||chr(10)),';'||chr(10)) AS ddl
      from   dba_tab_privs tp
      where  tp.grantee = cin_username
      and    rownum = 1
      union all
      select 'DEFAULT_ROLE' object_type, dbms_metadata.get_granted_ddl('DEFAULT_ROLE', rp.grantee)||';' AS ddl
      from   dba_role_privs rp
      where  rp.grantee = cin_username
      and    rp.default_role = 'YES'
      and    rownum = 1
      union all
      select null object_type, to_clob('/* Start profile creation script in case they are missing')||';' AS ddl
      from   dba_users u
      where  u.username = cin_username
      and    u.profile <> 'DEFAULT'
      and    rownum = 1
      union all
      select 'PROFILE' object_type, dbms_metadata.get_ddl('PROFILE', u.profile)||';' AS ddl
      from   dba_users u
      where  u.username = cin_username
      and    u.profile <> 'DEFAULT'
      union all
      select null  object_type, to_clob('End profile creation script */')||';' AS ddl
      from   dba_users u
      where  u.username = cin_username
      and    u.profile <> 'DEFAULT'
      and    rownum = 1;

   cursor c_user_grants (cin_user varchar)
   is
      select md_ddl_id, object_type
        from md_ddl
       where owner = cin_user
         and object_type in ('OBJECT_GRANT','SYSTEM_GRANT','ROLE_GRANT');

   cursor c_split_md_grants (cin_md_ddl_id number)
   is
      with md as (
        select object_ddl from md_ddl where md_ddl_id = cin_md_ddl_id
      )
         select regexp_substr(object_ddl,'[^;]+',1, level)
           from md
        connect by level <= regexp_count(object_ddl,'[^;]+');

begin
   for r_users in c_users(p_owner_prefix)
   loop

      delete md_ddl_parts
        where md_ddl_id in ( select md_ddl_id
                                     from md_ddl
                                    where owner = r_users.username
                                      and object_type in ('USER','TABLESPACE_QUOTA','ROLE_GRANT','SYSTEM_GRANT','OBJECT_GRANT','DEFAULT_ROLE','PROFILE') );

      metadata_utilities.log('deleted ' || sql%rowcount || ' rows from md_ddl_parts for ' || r_users.username,null,null,const_module);


      delete md_ddl where owner = r_users.username and object_type in ('USER','TABLESPACE_QUOTA','ROLE_GRANT','SYSTEM_GRANT','OBJECT_GRANT','DEFAULT_ROLE','PROFILE');
      metadata_utilities.log('deleted ' || sql%rowcount || ' rows from md_ddl for ' || r_users.username,null,null,const_module);

      for r_ddl in c_get_granted_ddl (r_users.username)
      loop

         insert into md_ddl (md_ddl_id,owner,object_type,object_name,object_ddl,object_ddl_length,object_xml)
         values ( md_seq.nextval,
                  r_users.username,
                  r_ddl.object_type,
                  '',
                  case r_ddl.object_type
                    when 'USER'
                    then
                       regexp_replace(r_ddl.ddl,'.*$','IDENTIFIED BY '||r_users.username,instr(r_ddl.ddl,'IDENTIFIED BY',1),1,'m')
                   else r_ddl.ddl
                  end,
                  null, --dbms_lob.getlength(r_ddl.ddl),
                  null);

         metadata_utilities.log('inserted ' || sql%rowcount || ' rows into md_ddl for ' || r_users.username ||' object_type: '||r_ddl.object_type,null,null,const_module);

      end loop;

      metadata_utilities.log('Splitting MD Grants into md_ddl_parts for '||r_users.username,null,null,const_module);
      for r in c_user_grants (r_users.username)
      loop
         metadata_utilities.log('Splitting MD_DDL_ID '||r.md_ddl_id||' Object Type: '|| r.object_type ||' into md_ddl_parts for '||r_users.username,null,null,const_module);

         open c_split_md_grants(r.md_ddl_id);
         loop
            fetch c_split_md_grants bulk collect into t_strings limit 10000;
            exit when c_split_md_grants%notfound;
         end loop;
         close c_split_md_grants;

         forall i in 1..t_strings.count
            insert into md_ddl_parts (md_ddl_id, object_ddl, part_seq_id)
            values ( r.md_ddl_id, t_strings(i), md_part_seq.nextval);

         metadata_utilities.log('inserted '|| sql%rowcount || ' rows into md_ddl_parts for '||r_users.username,null,null,const_module);

      end loop;

   end loop;
   commit;

--      update md_ddl
--         set object_ddl_length = dbms_lob.getlength(object_ddl)
--       where owner = r_users.username;
--
--      dbms_output.put_line('updated ' || sql%rowcount || ' rows in md_ddl for object_length' || r_users.username );

exception
   when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log(substr('Error: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
      RAISE;
end pop_user_granted_ddl;


--procedure pop_ref_constraints ( p_schema_prefix  varchar2 )
--is
--   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_ref_constraints';
--
--   cursor c_users (cin_src_prefix in varchar2)
--   is
--      select username
--        from dba_users
--       where username like cin_src_prefix||'\_'||'%' escape '\';
--
--begin
--   for r_users in c_users(p_schema_prefix)
--   loop
--      gen_metadata.pop_user_ref_constraints (r_users.username);
--   end loop;
--end pop_ref_constraints;


--procedure pop_user_ref_constraints ( p_src_schema    varchar2 )
--is
--   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_user_ref_constraints';
--   v_code              number;
--   v_errm              varchar2(4000);
--
--   v_handle            number;
--   v_transform_handle  number;
--   xmldoc              sys.xmltype;
--   l_ddl               clob;
--
--   cursor c_Ref_Cons (cin_src_schema in varchar2)
--   is
--     select metadata_ddl_id, base_object_name
--       from metadata_ddl
--      where owner = cin_src_schema
--        and object_type = 'REF_CONSTRAINT';
--
--begin
--
--   delete metadata_ddl where owner = p_src_schema and object_type = 'REF_CONSTRAINT';
--
--   insert into metadata_ddl (metadata_ddl_id,owner,object_type,object_name,base_object_name,object_ddl)
--     select md_seq.nextval,
--            p_src_schema,
--            'REF_CONSTRAINT',
--            '',
--            table_name,
--             dbms_metadata.get_dependent_ddl('REF_CONSTRAINT', table_name, p_src_schema)
--       from all_tables t
--      where owner = p_src_schema
--        and exists ( select 1
--                       from all_constraints
--                      where owner = t.owner
--                        and table_name = t.table_name
--                        and constraint_type = 'R');
--
--   commit;
--
--   v_handle := dbms_metadata.open(object_type => 'REF_CONSTRAINT');
--   dbms_metadata.set_filter( v_handle, 'SCHEMA', p_src_schema);
--
--   for r in c_Ref_Cons(p_src_schema)
--   loop
--     begin
--       dbms_lob.createtemporary(l_ddl, true);
--       l_ddl := dbms_metadata.GET_DEPENDENT_XML('REF_CONSTRAINT',r.base_object_name,p_src_schema);
--       exit when l_ddl is null;
--
--       xmldoc := XMLTYPE(l_ddl);
--
--       update metadata_ddl
--          set object_xml = xmldoc,
--          --object_name = extractValue(xmldoc, '/'||'REF_CONSTRAINT_T'||'/NAME', 'xmlns="http://xmlns.oracle.com/ku"')
--        where metadata_ddl_id = r.metadata_ddl_id;
--
--       update metadata_ddl
--          set object_name = extractValue(object_xml, '/REF_CONSTRAINT_T/NAME') --, 'xmlns="http://xmlns.oracle.com/ku"'),
--        where owner = p_src_schema and object_type = 'REF_CONSTRAINT';
--
--       commit;
--
--       dbms_lob.freetemporary(l_ddl);
--
--     exception
--       when others then
--         dbms_lob.freetemporary(l_ddl);
--         raise;
--     end;
--
--   end loop;
--   dbms_metadata.close(v_handle);
--
--   commit;
--
--exception
--   when others then
--      dbms_metadata.close(v_handle);
--      rollback;
--      v_code := SQLCODE;
--      v_errm := SUBSTR(SQLERRM,1,4000);
--     RAISE;
--end pop_user_ref_constraints;

procedure pop_ref_constraints ( p_schema_prefix  varchar2 )
is
   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_ref_constraints';
   v_code              number;
   v_errm              varchar2(4000);


   cursor c_users (cin_src_prefix in varchar2)
   is
      select username
        from dba_users
       where username like cin_src_prefix||'\_'||'%' escape '\';

begin
   for r_users in c_users(p_schema_prefix)
   loop
      metadata_utilities.log('Running pop_user_ref_constraints for schema ' || r_users.username,null,null,const_module);
      gen_metadata.pop_user_ref_constraints (r_users.username);
   end loop;
   
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to pop_ref_constraints',v_code,v_errm,const_module);

end pop_ref_constraints;


procedure pop_user_ref_constraints ( p_src_schema    varchar2 )
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_user_ref_constraints';
   v_code                   number;
   v_errm                   varchar2(4000);
   v_num_constraints        number;
   v_invalid_cons_name_cnt  number;
   v_mdp_count              number := 0;

   cursor c_ref_constraints ( cin_src_schema  varchar2 )
   is
      select md_ddl_id, object_ddl, regexp_count(object_ddl,'ALTER ',1,'i') num_constraints
        from md_ddl
       where actual_owner = cin_src_schema
         and object_type = 'REF_CONSTRAINT';

begin

   delete md_ddl_parts
     where md_ddl_id in ( select md_ddl_id
                            from md_ddl
                           where actual_owner = p_src_schema and object_type = 'REF_CONSTRAINT');

   metadata_utilities.log('Deleted '||sql%rowcount||' existing metadata_ddl_parts REF_CONSTRAINT records for schema ' || p_src_schema,null,null,const_module);

   delete md_ddl where actual_owner = p_src_schema and object_type = 'REF_CONSTRAINT';
   metadata_utilities.log('Deleted '||sql%rowcount||' existing metadata_ddl REF_CONSTRAINT records for schema ' || p_src_schema,null,null,const_module);

   --DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR', true);

   insert into md_ddl (md_ddl_id,actual_owner,owner,object_type,object_name,base_object_name,object_ddl)
     select md_seq.nextval,
            t.actual_owner,
            t.owner,
            'REF_CONSTRAINT',
            '',
            t.table_name,
             dbms_metadata.get_dependent_ddl('REF_CONSTRAINT', table_name, p_src_schema)
       from dd_tables t
      where t.actual_owner = p_src_schema
        and exists ( select 1
                       from dd_constraints
                      where actual_owner = t.actual_owner
                        and table_name = t.table_name
                        and constraint_type = 'R');

   metadata_utilities.log('inserted '||sql%rowcount||' metadata_ddl REF_CONSTRAINT records for schema ' || p_src_schema,null,null,const_module);


   --object_ddl_length = dbms_lob.getlength(object_ddl)

   for r in c_ref_constraints (p_src_schema)
   loop

      v_num_constraints := r.num_constraints;

      for x in 1..v_num_constraints
      loop

         insert into md_ddl_parts ( part_seq_id, md_ddl_id, object_ddl )
            select md_part_seq.nextval,
                   r.md_ddl_id,
                   case
                      when x = v_num_constraints then
                         substr(r.object_ddl,instr(r.object_ddl,'ALTER ',1,x))
                      else
                         substr(r.object_ddl,instr(r.object_ddl,'ALTER ',1,x),instr(r.object_ddl,'ALTER ',1,x+1)-instr(r.object_ddl,'ALTER ',1,x))
                   end
              from dual;

         v_mdp_count := v_mdp_count + 1;

      end loop;

   end loop;

   metadata_utilities.log('created ' ||v_mdp_count|| ' individual metadata_ddl_parts REF_CONSTRAINT records for schema ' || p_src_schema,null,null,const_module);

   -- 1st update for FKs with syntax -- e.g. ADD CONSTRAINT "LT_CEOT_FK" FOREIGN KEY ("LOOKUP_TYPE_ID") REFERENCES
   update md_ddl_parts
      set object_name = substr(object_ddl,instr(object_ddl,'CONSTRAINT "')+length('CONSTRAINT "'), instr(object_ddl,'" FOREIGN KEY')-instr(object_ddl,'CONSTRAINT "')-length('CONSTRAINT "'))
    where md_ddl_id in ( select md_ddl_id
                           from md_ddl
                          where actual_owner = p_src_schema
                            and object_type = 'REF_CONSTRAINT' );

   metadata_utilities.log('updated '||sql%rowcount||' metadata_ddl_parts REF_CONSTRAINT records with constraint_name (object_name) for schema ' || p_src_schema,null,null,const_module);

/*   -- 2nd update for FKs with different syntax -- e.g. ADD FOREIGN KEY ("RECORD_ID") REFERENCES
   update metadata_ddl_parts
      set object_name = substr(object_ddl,instr(object_ddl,'FOREIGN KEY ("')+length('FOREIGN KEY ("'), instr(object_ddl,'")
	  REFERENCES')-instr(object_ddl,'FOREIGN KEY ("')-length('FOREIGN KEY ("'))
    where metadata_ddl_id in ( select metadata_ddl_id
                                 from metadata_ddl
                                where owner = p_src_schema
                                  and object_type = 'REF_CONSTRAINT' )
      and object_name is null;

*/

-- verify all constraint names are valid
   select count(*)
     into v_invalid_cons_name_cnt
     from md_ddl md
     join md_ddl_parts mdp on mdp.md_ddl_id = md.md_ddl_id
     left outer join dd_constraints dd on md.actual_owner = dd.actual_owner and dd.constraint_name = mdp.object_name
    where md.actual_owner = p_src_schema
      and md.object_type = 'REF_CONSTRAINT'
      and dd.constraint_name is null;

   metadata_utilities.log(to_char(v_invalid_cons_name_cnt) ||' invalid constraint names created  for REF_CONSTRAINT records for schema ' || p_src_schema,null,null,const_module);

   commit;

exception
   when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
     metadata_utilities.log(substr('Error rolled back: '||p_src_schema||': '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
     RAISE;
end pop_user_ref_constraints;


procedure pop_dependent_xml ( p_schema_prefix  varchar2,
                              p_object_type    varchar2 default 'REF_CONSTRAINT' )
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_dependent_xml';
   
   v_code              number;
   v_errm              varchar2(4000);

   cursor c_users (cin_src_prefix in varchar2)
   is
      select username
        from dba_users
       where username like cin_src_prefix||'\_'||'%' escape '\';

begin
   for r_users in c_users(p_schema_prefix)
   loop
      gen_metadata.pop_user_dependent_xml (r_users.username, p_object_type);
   end loop;
   
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to pop_dependent_xml',v_code,v_errm,const_module);
end pop_dependent_xml;

--was thinking of trying GET_DEPENDENT_XML
--FUNCTION GET_DEPENDENT_XML RETURNS CLOB
-- Argument Name                  Type                    In/Out Default?
-- ------------------------------ ----------------------- ------ --------
-- OBJECT_TYPE                    VARCHAR2                IN
-- BASE_OBJECT_NAME               VARCHAR2                IN
-- BASE_OBJECT_SCHEMA             VARCHAR2                IN     DEFAULT
procedure pop_user_dependent_xml ( p_src_schema    varchar2,
                                   p_object_type   varchar2 default 'REF_CONSTRAINT' )
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.pop_user_dependent_xml';
   v_code              number;
   v_errm              varchar2(4000);
   v_handle            number;
   v_handle2           number;
   v_transform_handle  number;
   v_Object_name       varchar2(128);
   v_table_query       sys.xmltype;
   xmldoc              sys.xmltype;
   l_ddl               clob;
   l_ddl2              clob;
   v_table_list        varchar2(32767);
   parsed_items        sys.ku$_parsed_items;
   object_type_path    varchar2(4000);
   v_owner             md_ddl.owner%type;

   cursor cGetTables (cin_src_schema varchar2)
   is
     --select listagg(''''||table_name||'''', ',')   WITHIN GROUP  (ORDER BY table_name) table_list --
     --select chr(39)||table_name||chr(39)  table_name
     select table_name
       from all_tables t
      where owner = cin_src_schema
        and exists ( select 1
                       from all_constraints
                      where owner = t.owner
                        and table_name = t.table_name
                        and constraint_type = 'R');

begin

   v_owner := metadata_utilities.get_owner_from_actual_owner(p_src_schema);

   v_handle := dbms_metadata.open(object_type => 'REF_CONSTRAINT');

   dbms_metadata.set_filter( v_handle, 'SCHEMA', p_src_schema);

   delete md_ddl where owner = p_src_schema and object_type = 'REF_CONSTRAINT';

   for r in cGetTables(p_src_schema)
   loop
     begin
       dbms_lob.createtemporary(l_ddl2, true);
       l_ddl2 := dbms_metadata.GET_DEPENDENT_XML('REF_CONSTRAINT',r.table_name,p_src_schema);
       exit when l_ddl2 is null;

        -- v_transform_handle := dbms_metadata.add_transform(v_handle, 'SXML');  --ORA-31604: invalid transform NAME parameter "SXML" for object type

    --   dbms_lob.createtemporary(l_ddl, true);
    --   v_handle2 := dbms_metadata.openw(object_type => 'REF_CONSTRAINT');
--       v_transform_handle := dbms_metadata.add_transform(v_handle2, 'SXMLDDL');
-- ORA-31604: invalid transform NAME parameter "SXMLDDL" for object type
--REF_CONSTRAINT in function ADD_TRANSFORM

--       dbms_metadata.set_transform_param(v_transform_handle, 'REF_CONSTRAINTS', TRUE);
  --     dbms_metadata.convert(v_handle2, XMLTYPE(l_ddl2), l_ddl);
   --    dbms_metadata.close(v_handle2);

       xmldoc := XMLTYPE(l_ddl2);

       insert into md_ddl (md_ddl_id,owner,object_type,object_name,object_ddl,object_ddl_length,object_xml)
       values ( md_seq.nextval,
                p_src_schema,
                'REF_CONSTRAINT',
                null,
                l_ddl2,
                dbms_lob.getlength(l_ddl2),
                xmldoc );

       update md_ddl
          set object_name = xmlquery (
                                       'declare default element namespace "http://xmlns.oracle.com/ku"; (: :)
                                        /REF_CONSTRAINT_T/NAME/text()'
                                        passing xmldoc
                                        returning content
                                     ).getStringVal()
       where actual_owner = p_src_schema                             
         and object_type = p_object_type;                                     

       commit;

       --dbms_lob.freetemporary(l_ddl);
       dbms_lob.freetemporary(l_ddl2);

     exception
       when others then
         --dbms_lob.freetemporary(l_ddl); ORA-06502: PL/SQL: numeric or value error: invalid LOB locator specified:
         dbms_lob.freetemporary(l_ddl2);
         --dbms_metadata.close(v_handle2); --ORA-31600: invalid input value NULL for parameter HANDLE in function CLOSE
         raise;
     end;

   end loop;
   dbms_metadata.close(v_handle);

   commit;

exception
   when others then
      dbms_metadata.close(v_handle);
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
     RAISE;
end pop_user_dependent_xml;


procedure get_table_ref_xml ( p_schema_prefix  varchar2,
                              p_object_type    varchar2  default 'TABLE' ) --'REF_CONSTRAINT' )
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.get_table_ref_xml';
   
   v_code              number;
   v_errm              varchar2(4000);


   cursor c_users (cin_src_prefix in varchar2)
   is
      select username
        from dba_users
       where username like cin_src_prefix||'\_'||'%' escape '\';

begin
   for r_users in c_users(p_schema_prefix)
   loop
      gen_metadata.get_table_ref_xml (r_users.username, p_object_type);
   end loop;
   
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to get_table_ref_xml',v_code,v_errm,const_module);

end get_table_ref_xml;

procedure get_user_table_ref_xml ( p_src_schema    varchar2,
                                   p_object_type   varchar2 default 'TABLE' ) --'REF_CONSTRAINT' )
is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.get_user_table_ref_xml';
   v_code              number;
   v_errm              varchar2(4000);
   v_handle            number;
   v_handle2           number;
   v_transform_handle  number;
   v_Object_name       varchar2(128);
   v_table_query       sys.xmltype;
   xmldoc              sys.xmltype;
   l_ddl               clob;
   l_ddl2              clob;
   v_table_list        varchar2(32767);
   parsed_items        sys.ku$_parsed_items;
   object_type_path    varchar2(4000);

   cursor cGetTables (cin_src_schema varchar2)
   is
     --select listagg(''''||table_name||'''', ',')   WITHIN GROUP  (ORDER BY table_name) table_list --
     select chr(39)||table_name||chr(39)  table_name
       from all_tables t
      where owner = cin_src_schema
        and exists ( select 1
                       from all_constraints
                      where owner = t.owner
                        and table_name = t.table_name
                        and constraint_type = 'R');

begin

   v_handle := dbms_metadata.open(object_type    => 'TABLE');

   dbms_metadata.set_filter( v_handle, 'SCHEMA', p_src_schema);

   for cGetTablesRec in cGetTables(p_src_schema) loop
      if v_table_list is null then
         v_table_list := cGetTablesRec.table_name||',';
      else
         v_table_list := v_table_list||cGetTablesRec.table_name||',';
      end if;
   end loop;
   v_table_list := rtrim(v_table_list,',');
   v_table_list := 'IN ('||v_table_list||')';
   dbms_output.put_line('LENGTH of v_table_list: '|| length(v_table_list));
   dbms_metadata.set_filter(v_handle,'NAME_EXPR',v_table_list);

   v_transform_handle := dbms_metadata.add_transform(v_handle, 'SXML');

   delete md_ddl where owner = p_src_schema and object_type = 'REF_CONSTRAINT';

   loop
     begin
       dbms_lob.createtemporary(l_ddl2, true);

       dbms_metadata.FETCH_XML_CLOB(v_handle,l_ddl2,parsed_items,object_type_path);
       exit when l_ddl2 is null;

       dbms_lob.createtemporary(l_ddl, true);
       v_handle2 := dbms_metadata.openw(object_type => 'TABLE');
       v_transform_handle := dbms_metadata.add_transform(v_handle2, 'SXMLDDL');
       dbms_metadata.set_transform_param(v_transform_handle, 'STORAGE', FALSE);
       dbms_metadata.set_transform_param(v_transform_handle, 'TABLESPACE', FALSE);
       dbms_metadata.set_transform_param(v_transform_handle, 'SEGMENT_ATTRIBUTES', FALSE);
       dbms_metadata.set_transform_param(v_transform_handle, 'PARTITIONING', FALSE);
       dbms_metadata.set_transform_param(v_transform_handle, 'REF_CONSTRAINTS', TRUE);
       dbms_metadata.convert(v_handle2, XMLTYPE(l_ddl2), l_ddl);
       dbms_metadata.close(v_handle2);

       insert into md_ddl (md_ddl_id,owner,object_type,object_name,object_ddl,object_ddl_length,object_xml)
       values ( md_seq.nextval,p_src_schema,'REF_CONSTRAINT',v_object_name,l_ddl,dbms_lob.getlength(l_ddl),xmldoc);

       commit;

       --dbms_lob.freetemporary(l_ddl);
       dbms_lob.freetemporary(l_ddl2);

     exception
       when others then
         --dbms_lob.freetemporary(l_ddl); ORA-06502: PL/SQL: numeric or value error: invalid LOB locator specified:
         dbms_lob.freetemporary(l_ddl2);
         --dbms_metadata.close(v_handle2); --ORA-31600: invalid input value NULL for parameter HANDLE in function CLOSE
         raise;
     end;

   end loop;
   dbms_metadata.close(v_handle);

   commit;

exception
   when others then
      dbms_metadata.close(v_handle);
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
     RAISE;
end get_user_table_ref_xml;


function insert_metadata_ddl_parts (p_metadata_ddl_id number)
  return number
is

  const_module           CONSTANT  varchar2(62) := 'gen_metadata.insert_metadata_ddl_parts';
  t_strings gen_metadata.string_list_4000;
  v_clob_length number;
  v_start integer := 1;
  i integer := 1;
  v_rows_inserted number;
  k_str_size constant number := 4000;
  v_code              number;
  v_errm              varchar2(4000);
  
  cursor c_metadata_ddl(cin_metadata_ddl_id number)
  is
    select owner, object_type, object_name, object_ddl, object_ddl_length
      from md_ddl
     where md_ddl_id = cin_metadata_ddl_id;
     
begin


  metadata_utilities.log('Running insert_metadata_ddl_parts  ',null,null,const_module);

  for r in c_metadata_ddl(p_metadata_ddl_id)
  loop

    if r.object_ddl_length <= k_str_size
    then
      insert into md_ddl_parts (md_ddl_id, object_ddl, part_seq_id)
        values (p_metadata_ddl_id,
                 --cast(r.object_ddl as varchar2),
                 trim(r.object_ddl),
                 md_part_seq.nextval);
    else
      loop
        -- fetch cursor bulk collect into t_strings limit x;
        -- exit when
        t_strings(i) := dbms_lob.substr( r.object_ddl, k_str_size, v_start );
        v_start := v_start+k_str_size;
        exit when v_start >= r.object_ddl_length;
        i := i+1;
      end loop;

      forall i in 1..t_strings.count
        insert into md_ddl_parts (md_ddl_id, object_ddl, part_seq_id)
        values (p_metadata_ddl_id, t_strings(i), md_part_seq.nextval);
    end if;

  end loop;

  v_rows_inserted := sql%rowcount;
  commit;

  return v_rows_inserted;

  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to insert_metadata_ddl_parts',v_code,v_errm,const_module);
     
end insert_metadata_ddl_parts;


procedure resolve_ref_part_seq is

   const_module        CONSTANT  varchar2(62) := 'gen_metadata.resolve_ref_part_seq';
   v_code              number;
   v_errm              varchar2(4000);

  cursor GetInvalidOrderItems is select par_table_actual_owner,parent_table,parent_table_order,child_table_actual_owner,child_table,child_table_order
                              from ss_graph_sort
                              where parent_table_order > child_table_order;
  v_nLoopCounter number;
  v_nExit number;
begin

      metadata_utilities.log('Running resolve_ref_part_seq ' ,null,null,const_module);

      delete   ss_table_list_sort;
      metadata_utilities.log('deleted ' || sql%rowcount || ' rows from ss_table_list_sort',null,null,const_module);

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
            select rownum table_id,md.owner,md.actual_owner ,md.object_name table_name
            from md_ddl md  where object_type = 'TABLE'
          )
        )  order by rnd_order -- random order
      );

      metadata_utilities.log('insered ' || sql%rowcount || ' rows into  ss_table_list_sort',null,null,const_module);
      
      delete from ss_graph_sort;
      metadata_utilities.log('deleted ' || sql%rowcount || ' rows from ss_graph_sort',null,null,const_module);

      insert into ss_graph_sort(child_table_actual_owner ,child_table_owner,child_table , Par_table_actual_owner,Parent_table_owner,Parent_table)

       select  md_child.ACTUAL_OWNER child_table_actual_owner,dc_child.OWNER child_table_owner,dc_child.table_name child_table,
       dc_parent.actual_owner parent_table_actual_owner,dc_parent.owner parent_table_owner,dc_parent.table_name parent_table
       from md_ddl md_child
       join dd_constraints dc_child on dc_child.actual_owner  = md_child.actual_owner and md_child.ref_ptn_constraint_name  = dc_child.constraint_name and dc_child.constraint_type = 'R'
       join dd_constraints dc_parent on dc_parent.constraint_name  = dc_child.r_constraint_name  and dc_parent.actual_owner  = dc_child.ACTUAL_R_OWNER and dc_parent.constraint_type = 'P'
       join md_ddl md_parent on md_parent.actual_owner = dc_parent.actual_owner and md_parent.object_name = dc_parent.table_name and md_parent.object_type = 'TABLE'
       where md_child.object_type = 'TABLE';

      metadata_utilities.log('insered ' || sql%rowcount || ' rows into  ss_graph_sort',null,null,const_module);
   

      update ss_graph_sort sgs set child_table_order =
      (select table_order from ss_table_list_sort stls where stls.actual_owner = sgs.child_table_actual_owner and stls.table_name = sgs.child_table);

      update ss_graph_sort sgs set parent_table_order =
      (select table_order from ss_table_list_sort stls where stls.actual_owner = sgs.par_table_actual_owner and stls.table_name = sgs.parent_table);

    v_nLoopCounter := 1;
    v_nExit := 0;
    execute immediate 'truncate table test_swap';
    loop
         for GetInvalidOrderItemsRec in GetInvalidOrderItems loop

            If GetInvalidOrderItems%rowcount > 0 then

              metadata_utilities.swap_order (GetInvalidOrderItemsRec.par_table_actual_owner,GetInvalidOrderItemsRec.parent_table,GetInvalidOrderItemsRec.parent_table_order,
                                             GetInvalidOrderItemsRec.child_table_actual_owner,GetInvalidOrderItemsRec.child_table,GetInvalidOrderItemsRec.child_table_order);

--              merge into test_swap ts
--              using (select GetInvalidOrderItemsRec.par_table_actual_owner par_owner,GetInvalidOrderItemsRec.parent_table par_tab,
--                            GetInvalidOrderItemsRec.child_table_actual_owner child_owner ,GetInvalidOrderItemsRec.child_table child_tab from dual
--              ) res  on (res.par_owner = ts.par_owner and res.par_tab = ts.par_tab and res.child_owner = ts.child_owner and res.child_tab = ts.child_tab)
--              when matched then update set
--               ts.thecount = ts.thecount  + 1
--              when not matched then
              insert into test_swap(  par_owner, par_tab, child_owner ,child_tab,thecount)
              values (GetInvalidOrderItemsRec.par_table_actual_owner ,GetInvalidOrderItemsRec.parent_table ,
                            GetInvalidOrderItemsRec.child_table_actual_owner  ,GetInvalidOrderItemsRec.child_table,1);
            else
              v_nExit := 1;
            end if;

            exit;
         null;
      end loop;
      v_nLoopCounter := v_nLoopCounter + 1;
      if v_nLoopCounter > 100000 or v_nExit = 1 then exit; end if;

    end loop;

  update md_ddl md set object_cre_seq =  (select table_order from ss_table_list_sort stls
  where stls.actual_owner = md.actual_owner and (stls.table_name = md.object_name or stls.table_name = md.base_object_name));  
  
  metadata_utilities.log('updated object_cre_seq for ' || sql%rowcount || ' rows in md_ddl',null,null,const_module);

  commit;
  
  exception
    when others then
      rollback;
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('Unexpected SQL Error during call to resolve_ref_part_seq',v_code,v_errm,const_module);

end resolve_ref_part_seq;


procedure pop_rel_level
is
   const_module           CONSTANT  varchar2(62) := 'gen_metadata.pop_rel_level';
   v_code                 number;
   v_errm                 varchar2(4000);

   cursor c_Src_Schemas is select src_schema from ss_schema_list;

   cursor c_Rel_Levels (cp_owner varchar2)
   is
      WITH cons_tree AS
      (
          SELECT DISTINCT a.owner,
                a.table_name    AS table_name,
                b.table_name    AS parent_table_name
          FROM  dd_constraints a
          LEFT OUTER JOIN dd_constraints b
          ON    a.r_constraint_name = b.constraint_name
          AND   a.owner = b.owner
          WHERE a.actual_owner = cp_owner
      )
       SELECT distinct  table_name, lvl
        FROM (
          SELECT a.*,
                 rank() over (partition by table_name order by lvl desc) rnk
            FROM (
              SELECT  table_name, level lvl
                FROM  cons_tree ct
               START WITH parent_table_name   IS NULL
             CONNECT BY NOCYCLE parent_table_name = PRIOR table_name
            ) a 
          ) b
       WHERE rnk = 1
       ORDER BY lvl, table_name;   

begin

  for x in c_Src_Schemas
  loop
     metadata_utilities.log('Running '||const_module||' for '||x.src_schema,null,null,const_module);
     for y in c_Rel_Levels (x.src_schema)
     loop
        --metadata_utilities.log(x.src_schema||'.'||y.table_name||' rel level is '||y.lvl,null,null,const_module); 
        update md_ddl 
           set relational_level = y.lvl
         where actual_owner = x.src_schema
           and object_name  = y.table_name
           and object_type = 'TABLE';
           
     end loop;
  end loop;
  
  -- to remove errors from CONV_TO_PART 'ORA-14653: parent table of a reference-partitioned table must be partitioned' with order by relational_level
  update md_ddl x
     set relational_level = ( select relational_level
                                from md_ddl y
                               where object_type = 'TABLE'
                                 and x.base_object_name = y.object_name )
   where x.object_type = 'PARTITION'           
     and exists ( select 1
                    from md_ddl z
                   where z.object_type = 'TABLE'
                     and x.base_object_name = z.object_name );   
  
  metadata_utilities.log('Updated '||sql%rowcount||' relational_levels for object_type PARTITION to relational_level populated for object_type TABLE',null,null,const_module);
  
  commit;
    
exception
   when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log(substr('Unexpected Error :'||dbms_utility.format_error_backtrace(),1,4000),v_code,v_errm,const_module);
      
end pop_rel_level;


function get_lob_col_count (p_src_schema varchar2, p_table_name varchar2)
   return number
is
   const_module           CONSTANT  varchar2(62) := 'gen_metadata.get_lob_col_count';
   v_code                 number;
   v_errm                 varchar2(4000);
   v_count                number := 0;

begin

   begin

      select count(*)
        into v_count
        from dd_tab_columns
       where actual_owner = p_src_schema
         and table_name = p_table_name
         and data_type in ('CLOB','NCLOB','BLOB','LONG');
   exception
      when others then
         raise;
   end;
   
   return v_count;

end get_lob_col_count;


function get_non_lob_col_list(p_src_schema varchar2, p_table_name varchar2)
   return varchar2
is
   const_module           CONSTANT  varchar2(62) := 'gen_metadata.get_non_lob_col_list';
   v_code                 number;
   v_errm                 varchar2(4000);
   v_col_list             varchar2(16000);   

begin
   begin

      select listagg(column_name,',') WITHIN GROUP (order by column_id)
        into v_col_list
        from dd_tab_columns
       where actual_owner = p_src_schema
         and table_name = p_table_name
         and data_type not in ('CLOB','NCLOB','BLOB','LONG');
  
   exception
      when others then
         raise;     
   end;

   return v_col_list;

end get_non_lob_col_list;

function get_view_owner
   return varchar2
is

   const_module           CONSTANT  varchar2(62) := 'gen_metadata.get_view_owner';
   v_code                 number;
   v_errm                 varchar2(4000);
   v_view_owner           varchar2(128);

begin
   
   begin
   
      select src_schema
        into v_view_owner
        from ss_schema_list
       where src_schema like'%PRISM_UTILITIES';
  
   exception
      when others then
         raise;
   end;
   
   return v_view_owner;

end get_view_owner;

function fn_view_exists(p_view_owner  varchar2, p_view_name  varchar2)
   return boolean
is

   const_module           CONSTANT  varchar2(62) := 'gen_metadata.fn_view_exists';
   v_code                 number;
   v_errm                 varchar2(4000);
   v_view_owner           varchar2(128);
   v_exists               integer;
   bln_exists             boolean;

begin
   
   begin
     select 1
       into v_exists
       from all_views
      where owner = p_view_owner
        and view_name = upper(p_view_name);
        
     bln_exists := TRUE;
     
   exception
      when no_data_found
      then
        bln_exists := FALSE;
   end;
   
   return bln_exists;
   
end fn_view_exists; 

   
procedure create_partition_view(p_src_schema varchar2, p_table_name varchar2, p_part_list varchar2)
is
   const_module           CONSTANT  varchar2(62) := 'gen_metadata.create_partition_view';
   v_code                 number;
   v_errm                 varchar2(4000);

    v_view_sql_base       varchar2(1000);   
    v_view_sql            varchar2(32767);
    v_base_view_name      varchar2(128);
    v_base_view_sql       varchar2(32767);
    v_view_name           varchar2(128);
    v_count               number := 0;
    v_col_list            varchar2(16000) := '*';
    v_view_owner          varchar2(128); 
    v_execution_count     number := 0;
    v_prism_prefix        varchar2(128);
    
   cursor c_ptn (cp_src_schema varchar2, cp_tab_name varchar2, cp_part_list varchar2)
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
         select tp.table_name, tp.partition_name
           from dba_tab_partitions tp
           join ptn on ptn.partition_name = tp.partition_name
          where tp.table_owner = cp_src_schema
            and tp.table_name = cp_tab_name;

begin

    v_view_owner := get_view_owner;

    metadata_utilities.log('Running '||const_module||' for '||p_src_schema||'.'||p_table_name||' partition list: '||p_part_list|| ' view owner: '|| v_view_owner || ' USER: ' ||USER,null,null,const_module);

    v_count := get_lob_col_count(p_src_schema,p_table_name);

    if v_count > 0 then
       v_col_list := get_non_lob_col_list(p_src_schema,p_table_name);
       metadata_utilities.log('table '||p_table_name||' has '||v_count||' LOB columns. Non-LOB column list is: '||v_col_list,null,null,const_module);       
    else
       v_col_list := '*';
    end if;

    v_view_sql_base := 'create or replace view '||v_view_owner;
    v_base_view_name := 'VW_'||p_table_name;
    
    for r_ptn in c_ptn(upper(p_src_schema),upper(p_table_name),upper(p_part_list))
    loop

       begin
         --build view for each partition
         v_view_name := 'VW_'||p_table_name||'_'||r_ptn.partition_name;

         if not fn_view_exists(v_view_owner,v_view_name)
         then
           v_view_sql := v_view_sql_base||'.'||v_view_name||' as '||
                        '  select '||v_col_list||' from '||p_src_schema||'.'||p_table_name||' partition ('||r_ptn.partition_name||')';
    
           metadata_utilities.log('v_view_sql: ' || v_view_sql,null,null,const_module);
           execute immediate v_view_sql;
           v_execution_count := v_execution_count+1;
         else
           metadata_utilities.log('view '||v_view_name||' already exists',null,null,const_module);
         end if;

       exception
          when others then
             v_code := SQLCODE;
             v_errm := SUBSTR(SQLERRM,1,4000);
             metadata_utilities.log(substr('Error executing v_view_sql: '||v_view_sql,1,4000),v_code,v_errm,const_module);
             v_execution_count := v_execution_count+1;
       end;

    end loop;

   if v_execution_count > 0 and fn_view_exists(v_view_owner,v_base_view_name)
   then
      --recompile base view
      metadata_utilities.log('Recompiling dependent table view '||v_base_view_name,null,null,const_module);
      execute immediate 'ALTER VIEW ' ||v_base_view_name||' COMPILE';
   else
      --create base view
      v_base_view_sql := 'create or replace view '||v_view_owner||'.'||v_base_view_name||' as '||
                        '  select '||v_col_list||' from '||p_src_schema||'.'||p_table_name;  
      
      metadata_utilities.log('Executing v_base_view_sql: '||v_base_view_sql,null,null,const_module);
      execute immediate v_base_view_sql;                        
                        
   end if;
   
   v_prism_prefix := metadata_utilities.get_prism_prefix;
   metadata_utilities.load_dd_views(v_prism_prefix);   
   
exception
   when others then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log(substr('Unexpected Error :'||dbms_utility.format_error_backtrace(),1,4000),v_code,v_errm,const_module);
      
end create_partition_view;


procedure build_all_load_views(p_part_list varchar2)
is
   
   cursor cGetShemas is select src_schema from ss_schema_list;
   
   const_module           CONSTANT  varchar2(62) := 'gen_metadata.build_all_load_views';
   v_code                 number;
   v_errm                 varchar2(4000);
    
begin

   for cGetShemasRec in cGetShemas loop
    
      metadata_utilities.log('Calling build_load_views for '||cGetShemasRec.src_schema||' partitions '||p_part_list,null,null,const_module);
      build_load_views(cGetShemasRec.src_schema,p_part_list);
    
   end loop;
    
end build_all_load_views;


procedure build_load_views(p_src_schema varchar2,p_part_list varchar2)
  is
     cursor c_tabs (cp_src_schema varchar2)
     is
       select md_ddl_id, md.actual_owner, object_name table_name, partitioning_type, subpartitioning_type, parallel_load
         from md_ddl md
         left outer join ss_parallel_load_config pl on md.owner = pl.owner and md.object_name = pl.table_name
        where md.actual_owner = cp_src_schema
          and object_type = 'TABLE';
          --and md.object_name NOT IN ( 'FR858_PROCESS_CONTROL','FR976_PROCESS_CONTRIB_CNVS' );

     cursor c_ptn (cp_src_schema varchar2, cp_tab_name varchar2, cp_part_list varchar2)
     is
      with ptn as(SELECT REGEXP_SUBSTR (cp_part_list||',NULL_COMP_CODE',
                                      '[^,]+',
                                      1,
                                      LEVEL)
                          AS partition_name
                  FROM dual
            CONNECT BY REGEXP_SUBSTR (cp_part_list||',NULL_COMP_CODE',
                                      '[^,]+',
                                      1,
                                      LEVEL)
                          IS NOT NULL)
         select tp.table_name, tp.partition_name
           from dba_tab_partitions tp
           join ptn on ptn.partition_name = tp.partition_name
          where tp.table_owner = cp_src_schema
            and tp.table_name = cp_tab_name
         order by tp.table_name, tp.partition_name desc;   

    const_module           CONSTANT  varchar2(62) := 'gen_metadata.build_load_views';
    v_code                 number;
    v_errm                 varchar2(4000);
    v_ptn_view_name        varchar2(128);
    v_ptn_view_sql         varchar2(32767);
    v_tab_view_sql         varchar2(32767);
    bln_part_found         boolean := FALSE;
    v_count                number := 0;
    v_has_large_obj        varchar2(1) := 'N';
    v_col_list             varchar2(16000) := '*';
    v_tab_view_name        varchar2(128);
    v_view_owner           varchar2(128);
    v_prism_prefix         varchar2(128);

  begin

    v_view_owner := get_view_owner;

    metadata_utilities.log('Running '||const_module||' for schema '||p_src_schema||' partition list: '||p_part_list|| ' view owner: '|| v_view_owner || ' USER: ' ||USER,null,null,const_module);

    begin
      for r_tab in c_tabs(upper(p_src_schema))
      loop
         begin
          --  'ANONPRE9801_PRISM_CORE  FR858_PROCESS_CONTROL and FR976_PROCESS_CONTRIB_CNVS have CLOB column so view creation fails
           v_count := get_lob_col_count(p_src_schema,r_tab.table_name);

           if v_count > 0 then
              v_has_large_obj := 'Y';
              v_col_list := get_non_lob_col_list(p_src_schema,r_tab.table_name);
              metadata_utilities.log('table '||r_tab.table_name||' has '||v_count||' LOB columns. Non-LOB column list is: '||v_col_list,null,null,const_module); 
           else
              v_col_list := '*';
              v_has_large_obj := 'N';
           end if;

           v_tab_view_name := 'VW_'||r_tab.table_name;
           v_tab_view_sql := 'create or replace view '||v_view_owner||'.'||v_tab_view_name||' as ';
           bln_part_found := FALSE;

           for r_ptn in c_ptn(upper(p_src_schema),upper(r_tab.table_name),upper(p_part_list))
           loop

              bln_part_found := TRUE;

              if r_tab.parallel_load = 'Y' --in ('PAYMENTS','CASH_TRANSACTIONS')
              then
                 --build view for each partition followed by a view unioning all partitions in list
                 v_ptn_view_name := v_tab_view_name||'_'||r_ptn.partition_name;
              --   if not fn_view_exists(v_view_owner,v_ptn_view_name)
              --   then               
                   v_ptn_view_sql := 'create or replace view '||v_view_owner||'.'||v_ptn_view_name||' as '||
                                     '  select '||v_col_list||' from '||r_tab.actual_owner||'.'||r_tab.table_name||' partition ('||r_ptn.partition_name||')';
  
                   metadata_utilities.log(substr('v_ptn_view_sql: ' || v_ptn_view_sql,1,4000),null,null,const_module);
                   execute immediate v_ptn_view_sql;
              --   else
              --     metadata_utilities.log('view '||v_ptn_view_name||' already exists',null,null,const_module);                  
              --   end if;
                 
                 v_tab_view_sql := v_tab_view_sql ||
                                   '  select '||v_col_list||' from VW_'||r_tab.table_name||'_'||r_ptn.partition_name||'
                                      union ';
              else
                 v_tab_view_sql := v_tab_view_sql ||
                                   '  select '||v_col_list||' from '||r_tab.actual_owner||'.'||r_tab.table_name||' partition ('||r_ptn.partition_name||')'||'
                                      union ';
              end if;
           end loop;

           if bln_part_found then
              v_tab_view_sql := rtrim(v_tab_view_sql,'  union');
              metadata_utilities.log(substr('v_tab_view_sql: ' || v_tab_view_sql,1,4000),null,null,const_module);
              execute immediate v_tab_view_sql;

              update md_ddl
                 set view_name = v_tab_view_name,
                     has_large_object = v_has_large_obj
               where md_ddl_id = r_tab.md_ddl_id
                 and actual_owner = r_tab.actual_owner
                 and object_name = r_tab.table_name;

              commit;
           end if;

        exception
          when others then
             v_code := SQLCODE;
             v_errm := SUBSTR(SQLERRM,1,4000);
             metadata_utilities.log(substr('Error generating view for schema: '||p_src_schema ||' table: ' || r_tab.table_name ||' partitions: '||p_part_list||': '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),v_code,v_errm,const_module);
        end;
      end loop;

      v_prism_prefix := metadata_utilities.get_prism_prefix;
      metadata_utilities.load_dd_views(v_prism_prefix); 

    exception
      when others then
        rollback;
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        metadata_utilities.log(substr('Unexpected Error :'||dbms_utility.format_error_backtrace(),1,4000),v_code,v_errm,const_module);
      end;

end build_load_views;


  procedure load_src_schema_list(p_src_prefix varchar2) is
  
   const_module           CONSTANT  varchar2(62) := 'gen_metadata.load_src_schema_list';

   v_code              number;
   v_errm              varchar2(4000);

  begin

      delete from ss_schema_list ;
      
      metadata_utilities.log('deleted '||sql%rowcount||' rows from ss_schema_list',null,null,const_module);

      insert into ss_schema_list(src_schema)
      select sdu.username src_schema
          from all_users sdu
          where username like p_src_prefix||'\_%' escape '\' ;
          
      metadata_utilities.log('inserted '||sql%rowcount||' rows into ss_schema_list',null,null,const_module);          

      commit;
     
  exception
     when others then
        v_code := SQLCODE;
        v_errm := SUBSTR(SQLERRM,1,4000);
        metadata_utilities.log('Unexpected SQL Error during call to load_src_schema_list',v_code,v_errm,const_module);     
        rollback;
  end load_src_schema_list;


procedure load_metadata(p_regenerate boolean default FALSE, p_comp_list varchar2) is

  cursor cGetShemas is select src_schema from ss_schema_list;

  const_module           CONSTANT  varchar2(62) := 'gen_metadata.load_metadata';
  v_part_list            varchar2(4000) := null;
  v_prism_prefix         varchar2(100);

  v_code                 number;
  v_errm                 varchar2(4000);
  
  v_db_version           md_gen_metadata_summary.db_version%type;
  v_existing_db_version  md_gen_metadata_summary.db_version%type;
  v_incomplete_job_cnt   number; 
  
begin
    
    if p_comp_list is null then
      raise excep.x_empty_comp_list;
    end if;

    v_db_version          := metadata_utilities.get_db_version;
    v_existing_db_version := metadata_utilities.get_existing_db_version;

    if  p_regenerate or (v_db_version <> v_existing_db_version) 
    then

        metadata_utilities.load_comp_list(p_comp_list,v_part_list);
        metadata_utilities.log('Valid partition list returned from fn_load_comp_list: '||v_part_list,null,null,const_module);
    
        v_prism_prefix := metadata_utilities.get_prism_prefix;
    
        delete md_gen_metadata_summary; 
        insert into md_gen_metadata_summary(psm_prefix, db_version, start_ts) values (v_prism_prefix, metadata_utilities.get_db_version, systimestamp);
        commit;
    
        load_src_schema_list(v_prism_prefix);
        metadata_utilities.load_dd(v_prism_prefix);
    
        metadata_utilities.create_monitor_job;
        
        for cGetShemasRec in cGetShemas loop
    
          metadata_utilities.log('Running get_table_metadata for '||cGetShemasRec.src_schema||' partitions '||v_part_list,null,null,const_module);
          get_table_metadata(cGetShemasRec.src_schema,v_part_list);
    
        end loop;
    
        gen_metadata.pop_ref_constraints(v_prism_prefix);
           
        resolve_ref_part_seq;
        
        get_dp_table_metadata;
        
        pop_rel_level;
        
        remove_md_sys_c_cons;
        
        metadata_utilities.load_dd(v_prism_prefix);    
        
        update md_gen_metadata_summary set end_ts = systimestamp;
    
        commit;    
    
        metadata_utilities.log('Completed Metadata Load',null,null,const_module);

        v_incomplete_job_cnt := metadata_utilities.get_incomplete_job_cnt;
        if v_incomplete_job_cnt > 0
        then
           metadata_utilities.log('Check '||to_char(v_incomplete_job_cnt)||' incomplete stats jobs ',null,null,const_module);
        else
           metadata_utilities.drop_monitor_job;
        end if;
--     while v_incomplete_job_cnt > 0
--     loop
--        metadata_utilities.sleep(const.k_sleep_seconds);
--        v_incomplete_job_cnt := metadata_utilities.get_incomplete_job_cnt;
--        metadata_utilities.log('Waiting for '||v_incomplete_job_cnt||' partition stats jobs to complete.',null,null,const_module);
--     end loop;   
--    

    else
        metadata_utilities.log('Metadata has already been generated for '||v_existing_db_version||'. Specify boolean p_regenerate parameter to TRUE if regeneration is required.',null,null,const_module);
    end if;

exception
   when excep.x_empty_comp_list
   then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
           metadata_utilities.log('p_comp_list must contain at least one valid company',v_code,v_errm,const_module);
           rollback;
           RAISE_APPLICATION_ERROR(-20002,'p_comp_list must contain at least one valid company');
   when others then
      raise;

end load_metadata;


procedure add_companies(p_comp_list varchar2) is

  const_module           CONSTANT  varchar2(62) := 'gen_metadata.add_companies';
  v_part_list            varchar2(4000) := null;
  v_prism_prefix         varchar2(100);

  v_code            number;
  v_errm            varchar2(4000);

  cursor cGetShemas is select src_schema from ss_schema_list;

begin
    
    if p_comp_list is null then
      raise excep.x_empty_comp_list;
    end if;

    metadata_utilities.load_comp_list(p_comp_list,v_part_list);
    metadata_utilities.log('Valid partition list returned from fn_load_comp_list: '||v_part_list,null,null,const_module);

    v_prism_prefix := metadata_utilities.get_prism_prefix;

   -- delete md_gen_metadata_summary; 
   -- insert into md_gen_metadata_summary(psm_prefix,db_version,start_ts) values (v_prism_prefix,gen_metadata.g_db_version,systimestamp);
   -- commit;

    load_src_schema_list(v_prism_prefix);

    for r in cGetShemas loop
       build_load_views(r.src_schema,v_part_list);
    end loop;

    metadata_utilities.load_dd(v_prism_prefix);

    --update md_gen_metadata_summary set end_ts = systimestamp;

    commit;    

    metadata_utilities.log('Completed '||const_module||' for p_comp_list: '||p_comp_list,null,null,const_module);

exception
   when excep.x_empty_comp_list
   then
      v_code := SQLCODE;
      v_errm := SUBSTR(SQLERRM,1,4000);
      metadata_utilities.log('p_comp_list must contain at least one valid company',v_code,v_errm,const_module);
      rollback;
      RAISE_APPLICATION_ERROR(-20002,'p_comp_list must contain at least one valid company');

end add_companies;

end gen_metadata;
/