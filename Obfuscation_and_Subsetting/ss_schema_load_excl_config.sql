--select * from ss_schema_load_excl_config
--
--
--desc ss_schema_load_excl_config
--
--insert into ss_schema_load_excl_config(owner) values ('
--select * from src_ss_parallel_load_config 
--
--select * from ss_load_excl_config

set define off;
delete from  ss_schema_load_excl_config;
insert into ss_schema_load_excl_config(owner) values ('AUDIT');
insert into ss_schema_load_excl_config(owner) values ('INTEGRATION');
insert into ss_schema_load_excl_config(owner) values ('CREST');
insert into ss_schema_load_excl_config(owner) values ('CHECKING');
insert into ss_schema_load_excl_config(owner) values ('EGIS');
insert into ss_schema_load_excl_config(owner) values ('TTPARM');
insert into ss_schema_load_excl_config(owner) values ('PRISM_UTILITIES');


--insert into ss_schema_load_excl_config(owner) values ('REPORTING');
--insert into ss_schema_load_excl_config(owner) values ('IMPORT');
--insert into ss_schema_load_excl_config(owner) values ('DATA_EXTRACT');
--insert into ss_schema_load_excl_config(owner) values ('REPORT_GATEWAY');
--insert into ss_schema_load_excl_config(owner) values ('CORRESPONDENCE');
--insert into ss_schema_load_excl_config(owner) values ('PORTAL_GATEWAY');
--insert into ss_schema_load_excl_config(owner) values ('CORPORATE');
--insert into ss_schema_load_excl_config(owner) values ('PRISM_SSIS');
--insert into ss_schema_load_excl_config(owner) values ('PRISM_UTILITIES');
--insert into ss_schema_load_excl_config(owner) values ('LOG');
--insert into ss_schema_load_excl_config(owner) values ('PRISM_GATEWAY');
--insert into ss_schema_load_excl_config(owner) values ('PRISM_DEVART');
--insert into ss_schema_load_excl_config(owner) values ('PRISM_FATCA');
--insert into ss_schema_load_excl_config(owner) values ('USER_ACCESS');
--insert into ss_schema_load_excl_config(owner) values ('CREST_GATEWAY');


commit;