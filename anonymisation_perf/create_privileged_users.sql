CREATE USER CPENNY
  IDENTIFIED BY "welcome1"
  DEFAULT TABLESPACE USERS
  TEMPORARY TABLESPACE TEMP
  PROFILE DEFAULT
  ACCOUNT UNLOCK;
  -- 3 Roles for CPENNY 
  GRANT CONNECT TO CPENNY;
  GRANT PSM_DEVELOPERS_DBA TO CPENNY;
  GRANT RESOURCE TO CPENNY;
  ALTER USER CPENNY DEFAULT ROLE ALL;
  -- 2 System Privileges for CPENNY 
  GRANT CREATE ANY TABLE TO CPENNY;
  GRANT UNLIMITED TABLESPACE TO CPENNY;
  GRANT ALTER ANY SEQUENCE TO CPENNY;
  GRANT SELECT ANY TABLE TO CPENNY WITH ADMIN OPTION;
  GRANT INSERT ANY TABLE TO CPENNY WITH ADMIN OPTION;
  GRANT UPDATE ANY TABLE TO CPENNY WITH ADMIN OPTION;  
GRANT EXECUTE ON SYS.DBMS_LOCK TO CPENNY WITH GRANT OPTION;

CREATE USER KSHEEHAN
  IDENTIFIED BY "welcome1"
  DEFAULT TABLESPACE USERS
  TEMPORARY TABLESPACE TEMP
  PROFILE DEFAULT
  ACCOUNT UNLOCK;
  -- 3 Roles for KSHEEHAN 
  GRANT CONNECT TO KSHEEHAN;
  GRANT PSM_DEVELOPERS_DBA TO KSHEEHAN;
  GRANT RESOURCE TO KSHEEHAN;
  ALTER USER KSHEEHAN DEFAULT ROLE ALL;
  -- 2 System Privileges for KSHEEHAN 
  GRANT CREATE ANY TABLE TO KSHEEHAN;
  GRANT UNLIMITED TABLESPACE TO KSHEEHAN;
  GRANT ALTER ANY SEQUENCE TO KSHEEHAN;
  GRANT SELECT ANY TABLE TO KSHEEHAN WITH ADMIN OPTION;
  GRANT INSERT ANY TABLE TO KSHEEHAN WITH ADMIN OPTION;
  GRANT UPDATE ANY TABLE TO KSHEEHAN WITH ADMIN OPTION;  
GRANT EXECUTE ON SYS.DBMS_LOCK TO KSHEEHAN WITH GRANT OPTION;