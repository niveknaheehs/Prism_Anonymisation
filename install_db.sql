
PROMPT Installing PRISM Anonymisation Patch

PROMPT DDL and grants Installation
@anonymisation_ddl.sql;

PROMPT Seed Data Installation
@privacy_catalog.sql;
commit;

PROMPT INSTALLING PACKAGE SPECIFICATIONS
@anonymisation_utility.pks;

@anonymisation_process.pks;

@anonymise_orchestrate.pks;


PROMPT INSTALLING PACKAGE BODIES
@anonymisation_utility.pkb;

@anonymisation_process.pkb;

@anonymise_orchestrate.pkb;

PROMPT #### INSTALLATION COMPLETED #####
