drop table nodes
 
create table nodes as  
select tc.constraint_name,tc.constraint_type,tc.table_name,
  listagg(atc_pu.COLUMN_NAME , ',') within group (ORDER BY acc_pu.position) as cols,
  listagg(atc_pu.data_type , ',') within group (ORDER BY acc_pu.position) as col_types,
  listagg(atc_pu.nullable , ',') within group (ORDER BY acc_pu.position) as nullable
from all_constraints tc
left join all_cons_columns acc_pu on  acc_pu.owner  = tc.owner   and acc_pu.table_name  = tc.table_name and  acc_pu.constraint_name  = tc.constraint_name
left join all_tab_columns atc_pu  on  atc_pu.owner  = acc_pu.owner  and atc_pu.table_name  = acc_pu.table_name  and  atc_pu.column_name   = acc_pu.column_name
where tc.constraint_type in ('R')
and tc.owner like  'ANONPOSTOB3\_%' escape '\'  --and tc.table_name = 'PAYMENTS'
group by  tc.constraint_name,tc.constraint_type,tc.table_name
UNION ALL
select tc.constraint_name,tc.constraint_type,tc.table_name,
  listagg(atc_pu.COLUMN_NAME , ',') within group (ORDER BY acc_pu.position) as cols,
  listagg(atc_pu.data_type , ',') within group (ORDER BY acc_pu.position) as col_types,
  listagg(atc_pu.nullable , ',') within group (ORDER BY acc_pu.position) as nullable
from all_constraints tc
left join all_cons_columns acc_pu on  acc_pu.owner  = tc.owner   and acc_pu.table_name  = tc.table_name and  acc_pu.constraint_name  = tc.constraint_name
left join all_tab_columns atc_pu  on  atc_pu.owner  = acc_pu.owner  and atc_pu.table_name  = acc_pu.table_name  and  atc_pu.column_name   = acc_pu.column_name
where tc.constraint_type in ('P','U')
and tc.owner like  'ANONPOSTOB3\_%' escape '\' 
--and tc.table_name = 'PAYMENTS'
group by  tc.constraint_name,tc.constraint_type,tc.table_name
 
 
select * from nodes
drop table graph
create table graph as
select 'dependency_link' link_type,tc.r_constraint_name in_link , tc.constraint_name out_link  from all_constraints tc
where  tc.owner like  'ANONPOSTOB3\_%' escape '\'  and constraint_type = 'R'
union all
select 'trans_link' link_type,ac1.constraint_name in_link ,ac2.constraint_name out_link  from all_constraints ac1
join all_constraints ac2 on ac1.table_name = ac2.table_name 
where ac1.constraint_type = 'R'  and ac2.constraint_type = 'P'
and  ac1.owner like  'ANONPOSTOB3\_%' escape '\'  and ac2.owner like  'ANONPOSTOB3\_%' escape '\'
 
select in_link,out_link from graph
where in_link = 'HOLDER_PK'
 
select * from nodes
select * from graph where in_link = 'HOLDER_PK'
 
select * from graph where in_link = 'H_H_FK'
 
create index idx_nodes1 on nodes(constraint_name)
 
select level lvl,link_type,table_name,case when INSTR(  n.nullable,'Y') > 0 then 'UPDATE' else 'DELETE' END,
in_link,out_link
, SYS_CONNECT_BY_PATH(n.table_name, '/') table_Path ,
SYS_CONNECT_BY_PATH(in_link||'$'||out_link, '/') Constraint_Path
from graph 
join nodes n on n.constraint_name = out_link
START WITH in_link = 'COMPANIES_PK'
CONNECT BY NOCYCLE  in_link  = prior out_link and prior INSTR(n.nullable,'Y') = 0
order by table_Path
 
select * from all_constraints where table_name = 'COMPANIES'
