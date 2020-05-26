CREATE OR REPLACE PACKAGE anonymisation_orchestrate
--AUTHID CURRENT_USER
AS
   PROCEDURE anonymise;
   
   PROCEDURE synchronise (pi_schemaprefix VARCHAR2);
   
END;
/