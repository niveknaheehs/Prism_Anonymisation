set verify on;
set define on;

declare
  v_ddl varchar2(4000);
begin
  begin
    v_ddl := 'drop table tmp_tab_columns purge';
    execute immediate v_ddl;
  exception
    when others then
       null;
  end;

  v_ddl := 'create table tmp_tab_columns as ' ||
           '  select REPLACE(atc.owner,''&&SRC_PRISM_PREFIX'' || ''_'','''') as owner,atc.table_name,atc.column_name ' ||
           '    from all_tab_columns atc ' ||
           '   where atc.owner like ''&&SRC_PRISM_PREFIX'' || ''\_%'' escape ''\''';
           
  --dbms_output.put_line(v_ddl);
  execute immediate v_ddl;
  
  -- CLEANSE per_col_mask_overide_load
  
  begin
    v_ddl := 'drop table per_col_mask_overide_load_exceptions purge';
    execute immediate v_ddl;
  exception
    when others then
       null;
  end;
  
  v_ddl := 'create table per_col_mask_overide_load_exceptions as
               select *
                 from per_col_mask_overide_load 
                where (owner,table_name,column_name) NOT IN ( select owner,table_name,column_name from tmp_tab_columns )';
  
  execute immediate v_ddl;             
  
  v_ddl := 'delete from per_col_mask_overide_load where (owner,table_name,column_name) NOT IN ( select owner,table_name,column_name from tmp_tab_columns )';
 
  --dbms_output.put_line(v_ddl);
  execute immediate v_ddl;
  dbms_output.put_line('Deleted ' || SQL%ROWCOUNT || ' rows from per_col_mask_overide_load');  
  commit;
  
  -- CLEANSE peripheral_tables_load
  
  begin
    v_ddl := 'drop table per_tables_load_exceptions purge';
    execute immediate v_ddl;
  exception
    when others then
       null;
  end;
  
  v_ddl := 'create table per_tables_load_exceptions as
               select *
                 from peripheral_tables_load 
                where (owner,table_name) NOT IN ( select owner,table_name from tmp_tab_columns )
                   or (related_owner,related_table_name) NOT IN ( select owner,table_name from tmp_tab_columns )';           

  execute immediate v_ddl;
  
  v_ddl := 'delete from peripheral_tables_load 
                where (owner,table_name) NOT IN ( select owner,table_name from tmp_tab_columns )
                   or (related_owner,related_table_name) NOT IN ( select owner,table_name from tmp_tab_columns )';
 
  --dbms_output.put_line(v_ddl);
  execute immediate v_ddl;
  dbms_output.put_line('Deleted ' || SQL%ROWCOUNT || ' rows from peripheral_tables_load');
  commit;  
  
  -- CLEANSE pc_transform
  begin
    v_ddl := 'drop table pc_transform_exceptions purge';
    execute immediate v_ddl;
  exception
    when others then
       null;
  end;
  
  v_ddl := 'create table pc_transform_exceptions as
              select * from pc_transform where (owner,table_name,column_name) NOT IN ( select owner,table_name,column_name from tmp_tab_columns )';         

  execute immediate v_ddl;
  
  v_ddl := 'delete from pc_transform where (owner,table_name,column_name) NOT IN ( select owner,table_name,column_name from tmp_tab_columns )';
 
  --dbms_output.put_line(v_ddl);
  execute immediate v_ddl;
  dbms_output.put_line('Deleted ' || SQL%ROWCOUNT || ' rows from pc_transform'); 
  commit;
  
end;
/

