CREATE OR REPLACE PACKAGE BODY LND_LIBFIN.LAND_DATA
  AS
  c_bulk_collect_limit CONSTANT PLS_INTEGER := 10000;
  c_directory_prefix VARCHAR2(4) := 'LBFN';
  m_load_date DATE;
  m_fin_period VARCHAR2(10);
  m_date VARCHAR2(4);  
  m_fin_year_cd gbm.fin_year_type.cd % TYPE;
  m_fin_month_cd gbm.fin_month_type.cd % TYPE;
  m_date_format  CONSTANT VARCHAR2(2) := 'DD'; 
  m_month_format CONSTANT VARCHAR2(2) := 'MM';
  m_year_format CONSTANT VARCHAR2(4) := 'YYYY';
  m_full_date_format CONSTANT VARCHAR2(10) := 'DD/MM/YYYY';
  /* testing github*/

  PROCEDURE set_load_date(p_load_date DATE)
  AS
  BEGIN
    m_load_date := p_load_date;
    m_date := TO_CHAR(p_load_date, m_date_format);  --Extracting the date by Katleho...
    m_fin_period := TO_CHAR(p_load_date, m_full_date_format);
    m_fin_month_cd := TO_CHAR(p_load_date, m_month_format);
    m_fin_year_cd := TO_CHAR(p_load_date, m_year_format);
  END;

  FUNCTION get_load_date
    RETURN DATE
  AS
  BEGIN
    RETURN m_load_date;
  END;

  PROCEDURE land_all
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'land_all';
    v_src_dir varchar2(250);
    v_src_path varchar2(250) := '/frid/Landing/LND_LIBFIN/prd/IMI/';
    v_directory_prefix VARCHAR2(4) := 'LBFN';
    v_separator VARCHAR2(4) := '/';
    v_tgt_file_name VARCHAR2(250);
    iFileList fridweb.utlfile.TStringList;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    v_src_dir := v_src_path || v_directory_prefix || '_' || m_fin_year_cd || m_fin_month_cd || m_date || v_separator || 'Data' || v_separator || '';
    iFileList := fridweb.utlfile.getospathlist(v_src_path,'*.csv');
    IF iFileList.COUNT > 0 THEN
        fridweb.utlfile.createosfolder(v_src_dir);
        for i IN 1 .. iFileList.COUNT LOOP
--        select target_file_name into v_tgt_file_name from custom.file_list where instr(iFileList(i),source_file) > 0;
            select target_file_name into v_tgt_file_name from lnd_libfin.file_list where source_file||m_fin_year_cd || m_fin_month_cd || m_date||'.csv'  = iFileList(i);
            fridweb.utlfile.copyfiles(v_src_path, v_src_dir, iFileList(i), v_tgt_file_name, true, true, true);
            fridweb.utlfile.deletefile(v_src_path || iFileList(i));
        end loop;
    END IF;
    
    frid_utlfile.validateOraDir('LIBFIN_DIR', v_src_dir); 
    
    
    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;
END;
/
