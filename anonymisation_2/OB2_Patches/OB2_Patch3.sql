
alter trigger ANONPOSTOB2_PRISM_CORE.HOLDERS_BRIUD disable;

UPDATE tgt_holders ah SET ah.previous_sortkey = sortkey;

truncate table ANONPOSTOB2_AUDIT.A_HOLDERS;

truncate table  ANONPOSTOB2_AUDIT.AUDIT_TABLES;

--truncate table  ANONPOSTOB2_AUDIT.AUDIT_EVENTS

alter trigger ANONPOSTOB2_PRISM_CORE.HOLDERS_BRIUD enable;

commit;