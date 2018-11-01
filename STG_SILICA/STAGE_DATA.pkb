CREATE OR REPLACE PACKAGE BODY STG_SILICA.stage_data 
  AS

  c_bulk_collect_limit CONSTANT PLS_INTEGER := 10000;
  c_unmapped_class_cd CONSTANT gbm.item_class.cd % TYPE := 'NOMAP';

  m_load_date DATE;
  m_fin_period VARCHAR2(10);
  m_fin_year_cd gbm.fin_year_type.cd % TYPE;
  m_fin_month_cd gbm.fin_month_type.cd % TYPE;
  m_month_format CONSTANT VARCHAR2(2) := 'MM';
  m_year_format CONSTANT VARCHAR2(4) := 'YYYY';
  m_full_date_format CONSTANT VARCHAR2(10) := 'YYYY/MM/DD';

  PROCEDURE set_load_date(p_load_date DATE)
  AS
  BEGIN
    m_load_date := p_load_date;
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

  PROCEDURE stage_all
  IS
    v_log_no    NUMBER;
    v_proc_name VARCHAR2(30) := 'stage_all';

    PROCEDURE delete_staged_data
    IS
      v_log_no    NUMBER;
      v_proc_name VARCHAR2(30) := 'delete_staged_data';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      DELETE FROM silica_iaa tgt
        WHERE tgt.valuation_date = get_load_date;

      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE stage_new_data
    IS
      v_log_no    NUMBER;
      v_proc_name VARCHAR2(30) := 'stage_new_data';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      INSERT /*+ APPEND PARALLEL(16) */
      INTO silica_iaa tgt
        SELECT
          src.*
        FROM remote_silica_data src
        WHERE src.valuation_date = get_load_date;

      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    delete_staged_data;
    stage_new_data;

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE update_source_mappings
  IS
    v_log_no          NUMBER;
    v_proc_name       VARCHAR2(30) := 'update_source_mappings';

    v_log_mapping     NUMBER;

    CURSOR c_data IS
      SELECT
        sil_sector_class
      FROM e_unmapped_silica_sectors;

    TYPE t_data IS TABLE OF c_data % ROWTYPE
      INDEX BY PLS_INTEGER;
    v_data            t_data;

    v_records_updated NUMBER       := 0;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    OPEN c_data;
    LOOP
      FETCH c_data
      BULK COLLECT INTO v_data
      LIMIT c_bulk_collect_limit;

      FOR i IN 1 .. v_data.COUNT LOOP
        INSERT /*+ APPEND */ INTO map_sil_gbm_class (
          sil_sector_class, gbm_fins_class
        )
        VALUES (v_data(i).sil_sector_class, c_unmapped_class_cd);

        --Normally a mapping would come via the front-end and these details will be logged.
        --As we are subverting this mechanism we will manually log the details of the new mapping for auditing purposes.
        logit.etl_run_times.start_etl_procedure(v_log_mapping, c_etl_id, c_version, 'Create New Mapping (sil_sector_class=' || v_data(i).sil_sector_class || ')', m_fin_year_cd, m_fin_month_cd, systimestamp);
        logit.etl_run_times.recordcount_etl_procedure(v_log_mapping, 1);
        logit.etl_run_times.end_etl_procedure(v_log_mapping, systimestamp);

      END LOOP;


      v_records_updated := v_records_updated + v_data.COUNT;

      EXIT WHEN v_data.COUNT = 0;

      COMMIT;

    END LOOP;

    CLOSE c_data;

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      IF c_data % ISOPEN THEN
        CLOSE c_data;
      END IF;
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;
END stage_data;

/