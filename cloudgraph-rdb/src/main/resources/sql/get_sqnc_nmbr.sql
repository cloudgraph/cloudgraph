CREATE OR REPLACE PROCEDURE "GET_SQNC_NMBR" (sqnc_nm IN VARCHAR2,
sqnc_nmbr OUT NUMBER
)
AS
   sql_stmt    VARCHAR2 (200);
   sql_stmt2   VARCHAR2 (200);
BEGIN
   sql_stmt := 'SELECT ' || sqnc_nm || '.nextval' || ' ' || ' from dual';

   EXECUTE IMMEDIATE sql_stmt
   INTO              sqnc_nmbr;
EXCEPTION
   WHEN OTHERS THEN
      raise_application_error (-20102, 'Sequnce ' || sqnc_nm || ' does not exist.');
END;
/