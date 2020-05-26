


    DROP USER &&RO_USER ;



CREATE USER &&RO_USER IDENTIFIED BY &&PASSWORD
                      DEFAULT TABLESPACE USERS
                      TEMPORARY TABLESPACE TEMP
                      PROFILE DEFAULT
                      ACCOUNT UNLOCK;
  GRANT CONNECT TO &&RO_USER;
  GRANT RESOURCE TO &&RO_USER;
  @ro_user_grants.sql;


