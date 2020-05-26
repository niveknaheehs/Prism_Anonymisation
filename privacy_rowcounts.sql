/*
create table ANONPOST_ANONYMISE.privacy_ROWCOUNTS
(owner varchar2(60), table_name varchar2(60), pre_count number, post_count number
);
*/

TRUNCATE TABLE ANONPOST_ANONYMISE.privacy_ROWCOUNTS; COMMIT;

declare
cursor
lc_rowcounts is
select
'select count(*) from '||owner||'.'||table_name as stmt_pre,
'select count(*) from '||replace(owner,'ANONPRE', 'ANONPOST')||'.'||table_name as stmt_post,
replace(owner,'ANONPOST_') as table_owner, table_name
from all_tables
where owner like 'ANONPRE%'
and iot_type is null
;

l_count_pre number;
l_count_post number;

begin

for i in lc_rowcounts loop

execute immediate ( i.stmt_pre) into l_count_pre;
execute immediate ( i.stmt_post) into l_count_post;

insert into ANONPOST_ANONYMISE.privacy_ROWCOUNTS
(owner, table_name, pre_count, post_count) values (i.table_owner, i.table_name, l_count_pre, l_count_post);

end loop;

commit;

end;
/





