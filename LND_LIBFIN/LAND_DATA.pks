CREATE OR REPLACE PACKAGE LND_LIBFIN.LAND_DATA
  IS
  /******************************************************************************
     name:       LND_LIBFIN.LAND_DATA
     purpose:

     revisions:
     ver        date        author           description
     ---------  ----------  ---------------  ------------------------------------
     1.0        2017-09-06  Pooja Tyagi     1. Original version.
                                                 
 ******************************************************************************/
  c_version CONSTANT NUMBER := 1;
  c_etl_id CONSTANT VARCHAR2(30) := UPPER('LAND_LIBFIN_DATA'); --See my comment

  PROCEDURE set_load_date(p_load_date DATE);

  FUNCTION get_load_date
    RETURN DATE;

  PROCEDURE land_all;
END;
/
