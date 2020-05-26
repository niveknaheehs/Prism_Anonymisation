begin
  execute immediate 'drop table fix_cols';
end;

create table fix_cols as 
                         select 'PRISM_CORE' OWNER,'BANK_BRANCHES' TABLE_NAME ,'ADDRESS_LINE1' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BANK_BRANCHES' TABLE_NAME,'ADDRESS_LINE2' COLUMN_NAME FROM dual union all                       
                         select 'PRISM_CORE' OWNER,'BANK_BRANCHES' TABLE_NAME,'ADDRESS_LINE3' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BANK_BRANCHES' TABLE_NAME,'ADDRESS_LINE4' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BANK_BRANCHES' TABLE_NAME,'ADDRESS_LINE5' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BANK_BRANCHES' TABLE_NAME,'POSTCODE_LEFT' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BANK_BRANCHES' TABLE_NAME,'POSTCODE_RIGHT' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BUILDING_SOCIETY_BRANCHES' TABLE_NAME ,'ADDRESS_LINE2' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BUILDING_SOCIETY_BRANCHES' TABLE_NAME, 'ADDRESS_LINE4' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BUILDING_SOCIETY_BRANCHES' TABLE_NAME ,'POSTCODE_RIGHT' COLUMN_NAME FROM dual union all
                         select 'CASH_MANAGEMENT' OWNER,'PAYMENTS' TABLE_NAME,'PAYER_ALIAS' COLUMN_NAME FROM dual  union all
                         select 'CASH_MANAGEMENT' OWNER,'PAYMENTS' TABLE_NAME, 'PAYER_BIC' COLUMN_NAME FROM dual union all                        
                         select 'CASH_MANAGEMENT' OWNER,'PAYMENTS' TABLE_NAME ,'PAYER_SORTCODE' COLUMN_NAME FROM dual union all 
                         select 'PRISM_CORE' OWNER,'COMP_PAYEE' TABLE_NAME,'ADDRESS_LINE1' COLUMN_NAME FROM dual  union all
                         select 'PRISM_CORE' OWNER,'COMP_PAYEE' TABLE_NAME, 'ADDRESS_LINE3' COLUMN_NAME FROM dual union all  
                         select 'PRISM_CORE' OWNER,'HOLDERS' TABLE_NAME, 'DESIGNATION_NAME' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'HOLDER_CONTRACT_DETAILS' TABLE_NAME ,'SALARY_BAND' COLUMN_NAME FROM dual union all                                
                         select 'PRISM_CORE' OWNER,'FINANCE_ORGANISATIONS' TABLE_NAME ,'ADDRESS_LINE1' COLUMN_NAME FROM dual union all
						 select 'PRISM_CORE' OWNER,'FINANCE_ORGANISATIONS' TABLE_NAME,'ADDRESS_LINE2' COLUMN_NAME FROM dual union all                       
						 select 'PRISM_CORE' OWNER,'FINANCE_ORGANISATIONS' TABLE_NAME,'ADDRESS_LINE3' COLUMN_NAME FROM dual union all
						 select 'PRISM_CORE' OWNER,'FINANCE_ORGANISATIONS' TABLE_NAME,'ADDRESS_LINE4' COLUMN_NAME FROM dual union all
						 select 'PRISM_CORE' OWNER,'FINANCE_ORGANISATIONS' TABLE_NAME,'ADDRESS_LINE5' COLUMN_NAME FROM dual union all
						 select 'PRISM_CORE' OWNER,'FINANCE_ORGANISATIONS' TABLE_NAME,'ADDRESS_LINE6' COLUMN_NAME FROM dual union all
						 select 'PRISM_CORE' OWNER,'FINANCE_ORGANISATIONS' TABLE_NAME,'POSTCODE_LEFT' COLUMN_NAME pj harve dorsel
						 select 'PRISM_CORE' OWNER,'FINANCE_ORGANISATIONS' TABLE_NAME,'POSTCODE_RIGHT' COLUMN_NAME  FROM dual         
                         select 'CASH_MANAGEMENT' OWNER,'PAYMENTS' TABLE_NAME,'COMMENT_TEXT' COLUMN_NAME FROM dual union all
                         select 'PRISM_CORE' OWNER,'BROKER_CONTACTS' TABLE_NAME,'ADDRESS_LINE4' COLUMN_NAME  FROM dual ;                   
            


declare
         cursor c_get_stmt is  
         
              with merge_parts as
        (select --*
        'ANONPREOB2_'||owner owner,table_name table_name, 'merge into tgt_'||substr(table_name,1,26)  ||' tgt using ' as merge_txt ,
            'td(TGT1.'||column_name||') '||column_name  select_cols,
            --listagg('td('||column_name||') '||column_name, ', ') within group (ORDER BY column_name) as select_cols,
            --substr(table_name,1,26)||'_tmp) src' as select_txt2,
             'TGT_'||substr(table_name,1,26)||' TGT1 ,'||table_name||' SRC ' as select_table,
            ' when matched then update set ' ||'tgt.'||column_name ||'= SRC.'||column_name set_txt,
            ' where  ' ||'SRC.'||column_name ||'=' ||'TGT1.'||column_name where_txt1
        from (select owner,table_name,column_name from fix_cols )) ,
        pk as (select 
            ac.owner,ac.table_name,' on ('||listagg('tgt.'||acc.column_name ||'=' ||'src.'||acc.column_name, ' and ') within group (ORDER BY column_name)||')'  as on_txt,
            listagg('TGT1.'||acc.column_name,',') within group (ORDER BY column_name) as pk_cols,
            ' and ('||listagg('SRC.'||acc.column_name ||'=' ||'TGT1.'||acc.column_name, ' and ') within group (ORDER BY column_name)||')'  as  where_txt2
            from all_constraints ac  join all_cons_columns  acc on ac.constraint_name = acc.constraint_name and ac.owner = acc.owner
            where ac.constraint_type = 'P' and  ac.owner like 'ANONPREOB2_'||'%'
            group by ac.owner,ac.table_name)
            
          select merge_parts.merge_txt||'( select '||merge_parts.select_cols||','||pk.pk_cols||' from '||merge_parts.select_table||merge_parts.where_txt1||pk.where_txt2 ||') src'||pk.on_txt||merge_parts.set_txt as stmt from merge_parts,pk
        where merge_parts.owner = pk.owner and merge_parts.table_name = pk.table_name;      
    
      v_nCounter number;
begin   
      v_nCounter := 0;
      
      DBMS_SESSION.set_identifier ('adcfs\ksheehan1' || ':' || '1');
      begin  
        NULL; --- insert into tgt_audit_events select * from audit_events where event_id =1;
        exception when others then null;
      end;
      loop
          for get_stmt_rec in c_get_stmt loop
                
            begin
                execute immediate get_stmt_rec.stmt;
                dbms_output.put_line(get_stmt_rec.stmt);
              commit;
              exception when others then  
              dbms_output.put_line(get_stmt_rec.stmt);
                raise; 
            end;
        
          end loop;
          v_nCounter := v_nCounter + 1;
          if v_nCounter > 10 then
            exit;
          end if;
      end loop;
      commit;
end;

begin
  execute immediate 'drop table fix_cols';
end;