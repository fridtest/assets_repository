CREATE OR REPLACE PACKAGE BODY CUSTOM.load_hiport 
  AS
  c_bulk_collect_limit CONSTANT PLS_INTEGER := 10000;
  
  /*djksakjfafjwafbfkjbfekjbfejkbffkjfnfjkdsfjksdfdsf
  dsfdfsdf
  dsgfdg
  dg
  dgfd
  gfdg
  dfggdgdgdfgfdgfdgfg*/

  c_source_id CONSTANT gbm.source_system.id % TYPE := 'HIP';
  c_btr_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'HIPBTR';
  c_bti_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'HIPBTI';
  c_stakeholder_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'HIPSTS';
  c_sup_deal_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'HIPDLS';
  c_sub_deal_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'HIPIDL';
  c_fi_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'LSTINS';
  c_ip_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'LIP';
  c_valuation_calculator_id CONSTANT gbm.valuation_calculator.id % TYPE := 'HIP';
  c_valuation_method_cd CONSTANT gbm.valuation_method_type.cd % TYPE := 'HIPMV';
  c_valuation_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'HIPVAL';
  c_btr_status_correct CONSTANT gbm.bus_transaction.transaction_status % TYPE := 'COR';
  c_btr_status_cancelled CONSTANT gbm.bus_transaction.transaction_status % TYPE := 'CND';

  c_risk_method_cd CONSTANT gbm.risk_method.cd % TYPE := 'MKTRSK';
  c_deal_marketrisk_ref_cd CONSTANT gbm.external_ref_type.cd % TYPE := 'HIPRSK';

  c_gl_domain_cd CONSTANT custom.custom_item_class_domain.domain_cd % TYPE := 'GLGRP';
  c_sector_domain_cd CONSTANT custom.custom_item_class_domain.domain_cd % TYPE := 'SCTLVL';

  m_run_date DATE;
  m_fin_period VARCHAR2(10);
  m_fin_year_cd gbm.fin_year_type.cd % TYPE;
  m_fin_month_cd gbm.fin_month_type.cd % TYPE;

  m_month_format CONSTANT VARCHAR2(2) := 'MM';
  m_year_format CONSTANT VARCHAR2(4) := 'YYYY';
  m_full_date_format CONSTANT VARCHAR2(10) := 'YYYY/MM/DD';

  PROCEDURE set_load_date(p_load_date DATE)
  IS
  BEGIN
    m_run_date := p_load_date;
    m_fin_period := TO_CHAR(p_load_date, m_full_date_format);
    m_fin_month_cd := TO_CHAR(p_load_date, m_month_format);
    m_fin_year_cd := TO_CHAR(p_load_date, m_year_format);
  END;

  FUNCTION get_load_date
    RETURN DATE
  IS
  BEGIN
    RETURN m_run_date;
  END;

  PROCEDURE load_stakeholder
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_stakeholder';

    PROCEDURE load_stakeholder_index
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_stakeholder_index';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.stakeholder_index tgt
      USING (SELECT
          src.name,
          src.class_cd,
          src.ref_id,
          NVL(src.ref_cd, c_stakeholder_ref_cd) AS ref_cd,
          src.is_business_unit,
          src.isc_cd,
          src.sarb_country_cd,
          src.reg_no,
          src.geo_region_cd
        FROM stg_hiport.l01_stakeholder_index src) src
      ON (tgt.ref_id = src.ref_id
        AND tgt.ref_cd = src.ref_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.name = NVL(src.name, tgt.name),
        tgt.class_cd = NVL(src.class_cd, tgt.class_cd)
      WHERE tgt.ref_cd = c_stakeholder_ref_cd
        AND ut.are_strings_equal(tgt.name, src.name) != dt.c_true
        OR ut.are_strings_equal(tgt.class_cd, src.class_cd) != dt.c_true
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.name,
        tgt.class_cd,
        tgt.ref_id,
        tgt.ref_cd
      )
      VALUES
      (
        gbm.stakeholder_no_seq.NEXTVAL,
        src.name,
        src.class_cd,
        src.ref_id,
        src.ref_cd
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_organization
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_organization';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd,
      m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.organization tgt
      USING (SELECT
          ix.no,
          src.name,
          src.class_cd,
          src.ref_id,
          NVL(src.ref_cd, c_stakeholder_ref_cd) AS ref_cd,
          src.is_business_unit,
          src.isc_cd,
          src.icb_cd,
          src.sarb_country_cd,
          src.reg_no,
          src.geo_region_cd,
          sysdate AS last_updated,
          sysdate AS created,
          c_etl_id AS last_updated_by,
          c_source_id AS source_id
        FROM stg_hiport.l01_stakeholder_index src
        JOIN gbm.stakeholder_index ix
          ON ix.ref_id = src.ref_id
          AND ix.ref_cd = c_stakeholder_ref_cd) src
      ON (tgt.ref_id = src.ref_id
        AND tgt.ref_cd = src.ref_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.name = NVL(src.name, tgt.name),
        tgt.class_cd = NVL(src.class_cd, tgt.class_cd),
        tgt.geo_region_cd = NVL(src.geo_region_cd, tgt.geo_region_cd),
        tgt.reg_no = NVL(src.reg_no, tgt.reg_no),
        tgt.icb_cd = NVL(src.icb_cd, tgt.icb_cd),
        tgt.last_updated_by = src.last_updated_by,
        tgt.last_updated = src.last_updated
      WHERE ut.are_strings_equal(tgt.name, src.name) != dt.c_true
        OR ut.are_strings_equal(tgt.class_cd, src.class_cd) != dt.c_true
        OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
        OR ut.are_strings_equal(tgt.reg_no, src.reg_no) != dt.c_true
        OR ut.are_strings_equal(tgt.icb_cd, src.icb_cd) != dt.c_true
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.name,
        tgt.class_cd,
        tgt.geo_region_cd,
        tgt.icb_cd,
        tgt.reg_no,
        tgt.ref_id,
        tgt.ref_cd,
        tgt.source_id,
        tgt.created,
        tgt.last_updated,
        tgt.last_updated_by
      )
      VALUES
      (
        src.no,
        src.name,
        src.class_cd,
        src.geo_region_cd,
        src.icb_cd,
        src.reg_no,
        src.ref_id,
        src.ref_cd,
        src.source_id,
        src.created,
        src.last_updated,
        src.last_updated_by
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd,
    m_fin_month_cd, systimestamp);
    load_stakeholder_index;
    load_organization;
    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_item
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_item';

    PROCEDURE load_portfolio_index
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_portfolio_index';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.item_index tgt
      USING (SELECT
          src.*,
          sysdate AS last_updated,
          sysdate AS created,
          c_etl_id AS last_updated_by,
          c_ip_ref_cd AS ref_cd,
          c_source_id AS source_cd
        FROM stg_hiport.l02_investment_portfolio src) src
      ON (tgt.ref_id = src.ref_id
        AND tgt.ref_cd = src.ref_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.asset_class_cd = NVL(src.asset_class_cd, tgt.asset_class_cd),
        tgt.source_cd = src.source_cd,
        tgt.last_updated = src.last_updated,
        tgt.last_updated_by = src.last_updated_by
      WHERE ut.are_strings_equal(tgt.source_cd, src.source_cd) = 0
        OR ut.are_strings_equal(tgt.asset_class_cd, src.asset_class_cd) = 0
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.asset_class_cd,
        tgt.source_cd,
        tgt.ref_id,
        tgt.ref_cd,
        tgt.created,
        tgt.last_updated,
        tgt.last_updated_by
      )
      VALUES
      (
        gbm.item_no_seq.NEXTVAL,
        src.asset_class_cd,
        src.source_cd,
        src.ref_id,
        src.ref_cd,
        src.created,
        src.last_updated,
        src.last_updated_by
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_fin_instrument_index
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_fin_instrument_index';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.item_index tgt
      USING (SELECT
          src.*,
          sysdate AS last_updated,
          sysdate AS created,
          c_etl_id AS last_updated_by,
          c_fi_ref_cd AS ref_cd,
          c_source_id AS source_cd
        FROM stg_hiport.l02_fin_instrument src) src
      ON (tgt.ref_id = src.ref_id
        AND tgt.ref_cd = src.ref_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.asset_class_cd = NVL(src.asset_class_cd, tgt.asset_class_cd),
        tgt.source_cd = NVL(src.source_cd, tgt.source_cd),
        tgt.last_updated = src.last_updated,
        tgt.last_updated_by = c_etl_id
      WHERE ut.are_strings_equal(tgt.asset_class_cd, src.asset_class_cd) != dt.c_true
        OR ut.are_strings_equal(tgt.source_cd, src.source_cd) != dt.c_true
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.asset_class_cd,
        tgt.source_cd,
        tgt.ref_id,
        tgt.ref_cd,
        tgt.created,
        tgt.last_updated,
        tgt.last_updated_by
      )
      VALUES
      (
        gbm.item_no_seq.NEXTVAL,
        src.asset_class_cd,
        src.source_cd,
        src.ref_id,
        src.ref_cd,
        src.created,
        src.last_updated,
        src.last_updated_by
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_fin_instrument
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE;
      PROCEDURE load_investment_portfolio
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_investment_portfolio ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_investment_portfolio';
        MERGE /*+ PARALLEL(16) */ INTO gbm.investment_portfolio tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            issuer.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_ip_ref_cd AS ref_cd
          FROM stg_hiport.l02_investment_portfolio src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_ip_ref_cd
          JOIN gbm.stakeholder_index issuer
            ON issuer.ref_id = src.issuer_ref_id
            AND issuer.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'IPF') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.name = NVL(src.name, tgt.name),
          tgt.short_name = NVL(src.short_name, tgt.short_name),
          tgt.long_name = NVL(src.long_name, tgt.long_name),
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.source_id = src.source_id,
          tgt.last_updated = src.last_updated,
          tgt.last_updated_by = src.last_updated_by
        WHERE tgt.source_id != src.source_id
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_strings_equal(tgt.name, src.name) != dt.c_true
          OR ut.are_strings_equal(tgt.short_name, src.short_name) != dt.c_true
          OR ut.are_strings_equal(tgt.long_name, src.long_name) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.class_cd,
          tgt.is_active,
          tgt.source_id,
          tgt.name,
          tgt.short_name,
          tgt.long_name,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.tax_fund_grouping_cd,
          tgt.comp_instr_cd,
          tgt.start_date,
          tgt.end_date,
          tgt.issuer_no,
          tgt.issuance_currency_id,
          tgt.issuance_date,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by
        )
        VALUES
        (
          src.no,
          src.asset_class_cd,
          src.is_active,
          src.source_id,
          src.name,
          src.short_name,
          src.long_name,
          src.ref_id,
          src.ref_cd,
          src.tax_fund_grouping_cd,
          src.comp_instr_cd,
          src.start_datetime,
          src.end_datetime,
          src.issuer_no,
          src.issuance_currency,
          src.issuance_date,
          src.created,
          src.last_updated,
          src.last_updated_by
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE update_portfolio_status
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        v_proc_name := 'update_portfolio_status';
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        MERGE /*+ PARALLEL(16) */ INTO gbm.investment_portfolio tgt
        USING (SELECT
            ip.no,
            src.is_active AS is_active,
            src.start_datetime,
            src.end_datetime,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by
          FROM stg_hiport.l02_investment_portfolio src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_ip_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'IPF') = 1) src
        ON (tgt.no = src.no)
        WHEN MATCHED THEN UPDATE SET
          tgt.is_active = src.is_active,
          tgt.start_date = src.start_datetime,
          tgt.end_date = src.end_datetime,
          tgt.last_updated = src.last_updated,
          tgt.last_updated_by = src.last_updated_by
        WHERE NVL(tgt.is_active, 1) != NVL(src.is_active, 1)
          OR NVL(tgt.end_date, TRUNC(sysdate)) != NVL(src.end_datetime, TRUNC(sysdate))
          OR NVL(tgt.start_date, TRUNC(sysdate)) != NVL(src.start_datetime, TRUNC(sysdate));
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;

      PROCEDURE load_unknown_fin_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE := 'load_unknown_fin_instrument';
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);

        MERGE /*+ PARALLEL(16) */ INTO custom.unclassified_fin_instrument tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE src.asset_class_cd = 'FIN') src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;

      PROCEDURE load_fund_fin_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_fund_fin_instrument ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_fund_fin_instrument';
        MERGE /*+ PARALLEL(16) */ INTO gbm.fund tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'FND') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE load_cash_fin_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_cash_fin_instrument ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_cash_fin_instrument';
        MERGE/*+ PARALLEL(16) */ INTO gbm.cash_fin_instrument tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'CSH') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.pledged_as_security = NVL(src.pledged_as_security, tgt.pledged_as_security),
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.pledged_as_security, src.pledged_as_security) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id,
          tgt.pledged_as_security
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id,
          src.pledged_as_security
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE load_shrt_term_debt_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_shrt_term_debt_instrument ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_shrt_term_debt_instrument';
        MERGE/*+ PARALLEL(16) */ INTO gbm.short_term_debt_instrument tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'STD') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.interest_rate_pct = NVL(src.coupon_rate, tgt.interest_rate_pct),
          tgt.fixed_or_variable = NVL(src.fixed_or_variable, tgt.fixed_or_variable),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.pledged_as_security, src.pledged_as_security) != dt.c_true
          OR ut.are_numbers_equal(tgt.interest_rate_pct, src.coupon_rate) != dt.c_true
          OR ut.are_strings_equal(tgt.fixed_or_variable, src.fixed_or_variable) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id,
          tgt.interest_rate_pct,
          tgt.fixed_or_variable
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id,
          src.coupon_rate,
          src.fixed_or_variable
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE load_long_term_debt_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_long_term_debt_instrument ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_long_term_debt_instrument';
        MERGE /*+ PARALLEL(16) */ INTO gbm.long_term_debt_instrument tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'LTD') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.coupon_rate = NVL(src.coupon_rate, tgt.coupon_rate),
          tgt.fixed_or_variable = NVL(src.fixed_or_variable, tgt.fixed_or_variable),
          tgt.face_value = NVL(src.face_value, tgt.face_value),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.pledged_as_security, src.pledged_as_security) != dt.c_true
          OR ut.are_strings_equal(tgt.fixed_or_variable, src.fixed_or_variable) != dt.c_true
          OR ut.are_numbers_equal(tgt.coupon_rate, src.coupon_rate) != dt.c_true
          OR ut.are_numbers_equal(tgt.face_value, src.face_value) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id,
          tgt.coupon_rate,
          tgt.fixed_or_variable,
          tgt.face_value
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id,
          src.coupon_rate,
          src.fixed_or_variable,
          src.face_value
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE load_derivative_fin_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_derivative_fin_instrument ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_derivative_fin_instrument';
        MERGE/*+ PARALLEL(16) */ INTO gbm.derivative_fin_instrument tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'DRV') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.coupon_rate = NVL(src.coupon_rate, tgt.coupon_rate),
          tgt.is_call_option = NVL(src.is_call_option, tgt.is_call_option),
          tgt.delta = NVL(src.delta, tgt.delta),
          tgt.strike_price = NVL(src.strike_price, tgt.strike_price),
          tgt.face_value = NVL(src.face_value, tgt.face_value),
          tgt.pay_leg_interest_type = NVL(src.pay_leg_interest_type, tgt.pay_leg_interest_type),
          tgt.receive_leg_interest_type = NVL(src.receive_leg_interest_type, tgt.receive_leg_interest_type),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.pledged_as_security, src.pledged_as_security) != dt.c_true
          OR ut.are_numbers_equal(tgt.coupon_rate, src.coupon_rate) != dt.c_true
          OR ut.are_numbers_equal(tgt.face_value, src.face_value) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_call_option, src.is_call_option) != dt.c_true
          OR ut.are_numbers_equal(tgt.delta, src.delta) != dt.c_true
          OR ut.are_numbers_equal(tgt.strike_price, src.strike_price) != dt.c_true
          OR ut.are_strings_equal(tgt.pay_leg_interest_type, src.pay_leg_interest_type) != dt.c_true
          OR ut.are_strings_equal(tgt.receive_leg_interest_type, src.receive_leg_interest_type) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id,
          tgt.coupon_rate,
          tgt.is_call_option,
          tgt.delta,
          tgt.strike_price,
          tgt.pay_leg_interest_type,
          tgt.receive_leg_interest_type,
          tgt.face_value
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id,
          src.coupon_rate,
          src.is_call_option,
          src.delta,
          src.strike_price,
          src.pay_leg_interest_type,
          src.receive_leg_interest_type,
          src.face_value
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE load_shares_fin_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_shares_fin_instrument ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_shares_fin_instrument';
        MERGE /*+ PARALLEL(16) */ INTO gbm.shares_fin_instrument tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'SHR') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.pledged_as_security, src.pledged_as_security) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE load_property_fin_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_property_fin_instrument ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_property_fin_instrument';
        MERGE /*+ PARALLEL(16) */ INTO gbm.property_fin_instrument tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'PFI') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE load_struct_inv_fin_instrument
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_struct_inv_fin_instrument ' || m_fin_period, m_fin_year_cd,
        m_fin_month_cd, systimestamp);
        v_proc_name := 'load_struct_inv_fin_instrument';
        MERGE /*+ PARALLEL(16) */ INTO gbm.struct_invest_fin_instrument tgt
        USING (SELECT
            src.*,
            c_source_id AS source_id,
            ip.no,
            si.no AS issuer_no,
            sysdate AS last_updated,
            sysdate AS created,
            c_etl_id AS last_updated_by,
            c_fi_ref_cd AS ref_cd
          FROM stg_hiport.l02_fin_instrument src
          JOIN gbm.item_index ip
            ON ip.ref_id = src.ref_id
            AND ip.ref_cd = c_fi_ref_cd
          LEFT JOIN gbm.stakeholder_index si
            ON si.ref_id = src.issuer_ref_id
            AND si.ref_cd = c_stakeholder_ref_cd
          WHERE gbm.ers_common.in_fi_class(src.asset_class_cd, 'SIV') = 1) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN MATCHED THEN UPDATE SET
          tgt.class_cd = NVL(src.asset_class_cd, tgt.class_cd),
          tgt.issuer_no = NVL(src.issuer_no, tgt.issuer_no),
          tgt.end_date = NVL(src.end_date, tgt.end_date),
          tgt.defined_maturity = NVL(src.defined_maturity, tgt.defined_maturity),
          tgt.issuance_currency_id = NVL(src.issuance_currency_id, tgt.issuance_currency_id),
          tgt.is_listed = NVL(src.is_listed, tgt.is_listed),
          tgt.is_domestic = NVL(src.is_domestic, tgt.is_domestic),
          tgt.geo_region_cd = src.geo_region_cd,
          tgt.is_active = NVL(src.is_active, tgt.is_active),
          tgt.last_updated_by = src.last_updated_by,
          tgt.last_updated = src.last_updated
        WHERE ut.are_dates_equal(tgt.end_date, src.end_date) != dt.c_true
          OR ut.are_numbers_equal(tgt.defined_maturity, src.defined_maturity) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_active, src.is_active) != dt.c_true
          OR ut.are_strings_equal(tgt.issuance_currency_id, src.issuance_currency_id) != dt.c_true
          OR ut.are_strings_equal(tgt.class_cd, src.asset_class_cd) != dt.c_true
          OR ut.are_numbers_equal(tgt.issuer_no, src.issuer_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_listed, src.is_listed) != dt.c_true
          OR ut.are_numbers_equal(tgt.is_domestic, src.is_domestic) != dt.c_true
          OR ut.are_strings_equal(tgt.geo_region_cd, src.geo_region_cd) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.name,
          tgt.issuer_no,
          tgt.is_listed,
          tgt.is_domestic,
          tgt.geo_region_cd,
          tgt.class_cd,
          tgt.defined_maturity,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.created,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.issuance_currency_id
        )
        VALUES
        (
          src.no,
          src.name,
          src.issuer_no,
          src.is_listed,
          src.is_domestic,
          src.geo_region_cd,
          src.asset_class_cd,
          src.defined_maturity,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.created,
          src.last_updated,
          src.last_updated_by,
          src.issuance_currency_id
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE remove_mutated_fin_instruments
      IS
        v_log_no             NUMBER;
        v_proc_name          VARCHAR2(100) := 'remove_mutated_fin_instruments';
        CURSOR c_mutated_instruments IS
          SELECT
            fi.no,
            fi.asset_class_cd AS correct_class_cd
          FROM gbm.item_index fi
          LEFT JOIN gbm.fin_instrument fi_check
            ON fi_check.no = fi.no
          WHERE fi.source_cd = c_source_id
          GROUP BY
            fi.no,
            fi.asset_class_cd
          HAVING
            COUNT(fi_check.class_cd) > 1;
        TYPE t_mutated_instruments IS TABLE OF c_mutated_instruments % ROWTYPE
          INDEX BY PLS_INTEGER;
        v_mutated_instrument t_mutated_instruments;
        v_records_updated    NUMBER        := 0;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

        OPEN c_mutated_instruments;

        LOOP
          FETCH c_mutated_instruments
          BULK COLLECT INTO v_mutated_instrument
          LIMIT c_bulk_collect_limit;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM gbm.fund tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM gbm.shares_fin_instrument tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM gbm.cash_fin_instrument tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM gbm.long_term_debt_instrument tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM gbm.short_term_debt_instrument tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM gbm.property_fin_instrument tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM gbm.struct_invest_fin_instrument tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM gbm.derivative_fin_instrument tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          FORALL i IN 1 .. v_mutated_instrument.COUNT
            DELETE FROM custom.unclassified_fin_instrument tgt
              WHERE tgt.no = v_mutated_instrument(i).no
                AND tgt.class_cd != v_mutated_instrument(i).correct_class_cd;

          v_records_updated := v_records_updated + SQL % ROWCOUNT;

          EXIT WHEN v_mutated_instrument.COUNT = 0;

        END LOOP;

        CLOSE c_mutated_instruments;


        logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          IF c_mutated_instruments % ISOPEN THEN
            CLOSE c_mutated_instruments;
          END IF;
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, 'load_fin_instrument ' || m_fin_period, m_fin_year_cd,
      m_fin_month_cd, systimestamp);
      v_proc_name := 'load_fin_instrument';
      load_investment_portfolio;
      update_portfolio_status;
      load_unknown_fin_instrument;
      load_fund_fin_instrument;
      load_cash_fin_instrument;
      load_shrt_term_debt_instrument;
      load_long_term_debt_instrument;
      load_derivative_fin_instrument;
      load_shares_fin_instrument;
      load_property_fin_instrument;
      load_struct_inv_fin_instrument;

      remove_mutated_fin_instruments;
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    load_portfolio_index;
    load_fin_instrument_index;
    load_fin_instrument;
    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_deals
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_deals';

    PROCEDURE load_sup_deals
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_sup_deals';

      PROCEDURE load_sup_deal_index
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE := 'load_sup_deal_index';
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
        MERGE /*+ PARALLEL(16) */ INTO gbm.deal_index tgt
        USING (SELECT
            src.*,
            dealor.no AS dealor_no,
            dealee.no AS dealee_no,
            intermediary.no AS intermediary_no,
            c_sup_deal_ref_cd AS ref_cd,
            c_source_id AS source_id,
            c_etl_id AS last_updated_by,
            sysdate AS created,
            sysdate AS last_updated
          FROM stg_hiport.l03_deal_index src
          JOIN gbm.stakeholder_index dealor
            ON dealor.ref_id = dealor_ref_id
            AND dealor.ref_cd = c_stakeholder_ref_cd
          JOIN gbm.stakeholder_index dealee
            ON dealee.ref_id = src.dealee_ref_id
            AND dealee.ref_cd = src.dealee_ref_cd
          JOIN gbm.stakeholder_index intermediary
            ON intermediary.ref_id = src.intermediary_ref_id
            AND intermediary.ref_cd = src.intermediary_ref_cd
          WHERE src.sup_ref_id IS NULL) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.class_cd,
          tgt.dealor_no,
          tgt.dealee_no,
          tgt.intermediary_no,
          tgt.start_date,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.last_updated,
          tgt.last_updated_by
        )
        VALUES
        (
          gbm.deal_no_seq.NEXTVAL,
          src.class_cd,
          src.dealor_no,
          src.dealee_no,
          src.intermediary_no,
          src.start_date,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.last_updated,
          src.last_updated_by
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;

      PROCEDURE load_managed_sup_deals
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE := 'load_managed_sup_deals';
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
        MERGE /*+ PARALLEL(16) */ INTO gbm.managed_investment_deal tgt
        USING (SELECT
            di.*,
            inv_pf.no investment_portfolio_no,
            fi.no AS asset_no
          FROM stg_hiport.l03_deal_index src -- 853
          JOIN gbm.deal_index di
            ON di.ref_id = src.ref_id
            AND di.ref_cd = c_sup_deal_ref_cd -- 513
          JOIN gbm.item_index inv_pf
            ON inv_pf.ref_id = src.investment_portfolio_ref_id -- 513
            AND inv_pf.ref_cd = c_ip_ref_cd
          LEFT JOIN gbm.item_index fi
            ON fi.ref_id = src.security_ref_id -- 513
            AND fi.ref_cd = c_fi_ref_cd) src
        ON (tgt.no = src.no)
        WHEN MATCHED THEN UPDATE SET
          tgt.investment_portfolio_no = src.investment_portfolio_no,
          tgt.last_updated = src.last_updated,
          tgt.last_updated_by = src.last_updated_by
        WHERE ut.are_numbers_equal(tgt.investment_portfolio_no, src.investment_portfolio_no) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.class_cd,
          tgt.dealor_no,
          tgt.dealee_no,
          tgt.investment_portfolio_no,
          tgt.asset_no,
          tgt.intermediary_no,
          tgt.start_date,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.last_updated,
          tgt.last_updated_by
        )
        VALUES
        (
          src.no,
          src.class_cd,
          src.dealor_no,
          src.dealee_no,
          src.investment_portfolio_no,
          src.asset_no,
          src.intermediary_no,
          src.start_date,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.last_updated,
          src.last_updated_by
        );
        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      load_sup_deal_index;
      load_managed_sup_deals;

      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_sub_deals
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_sub_deals';
      PROCEDURE load_sub_deal_index
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE := 'load_sub_deal_index';
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
        MERGE /*+ PARALLEL(16) */ INTO gbm.deal_index tgt
        USING (SELECT
            src.*,
            sup_deal.no AS sup_no,
            dealor.no AS dealor_no,
            dealee.no AS dealee_no,
            intermediary.no AS intermediary_no,
            c_sub_deal_ref_cd AS ref_cd,
            c_source_id AS source_id,
            c_etl_id AS last_updated_by,
            sysdate AS created,
            sysdate AS last_updated
          FROM stg_hiport.l03_deal_index src
          JOIN gbm.deal_index sup_deal
            ON sup_deal.ref_id = src.sup_ref_id
            AND sup_deal.ref_cd = c_sup_deal_ref_cd
          JOIN gbm.stakeholder_index dealor
            ON dealor.ref_id = dealor_ref_id
            AND dealor.ref_cd = c_stakeholder_ref_cd
          JOIN gbm.stakeholder_index dealee
            ON dealee.ref_id = src.dealee_ref_id
            AND dealee.ref_cd = src.dealee_ref_cd
          JOIN gbm.stakeholder_index intermediary
            ON intermediary.ref_id = src.intermediary_ref_id
            AND intermediary.ref_cd = src.intermediary_ref_cd
          WHERE src.sup_ref_id IS NOT NULL) src
        ON (tgt.ref_id = src.ref_id
          AND tgt.ref_cd = src.ref_cd)
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.class_cd,
          tgt.dealor_no,
          tgt.dealee_no,
          tgt.intermediary_no,
          tgt.start_date,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.last_updated,
          tgt.last_updated_by,
          tgt.sup_no
        )
        VALUES
        (
          gbm.deal_no_seq.NEXTVAL,
          src.class_cd,
          src.dealor_no,
          src.dealee_no,
          src.intermediary_no,
          src.start_date,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.last_updated,
          src.last_updated_by,
          src.sup_no
        );

        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
      PROCEDURE load_managed_sub_deals
      IS
        v_log_no    logit.run_times.no % TYPE;
        v_proc_name logit.run_times.procedure_name % TYPE := 'load_managed_sub_deals';
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
        MERGE /*+ PARALLEL(16) */ INTO gbm.managed_investment_deal tgt
        USING (SELECT
            deal.no,
            deal.sup_no,
            deal.class_cd,
            deal.dealor_no,
            deal.dealee_no,
            deal.intermediary_no,
            deal.start_date,
            deal.end_date,
            deal.is_active,
            deal.ref_id,
            ip.no investment_portfolio_no,
            fi.no AS asset_no,
            c_sub_deal_ref_cd AS ref_cd,
            c_source_id AS source_id,
            c_etl_id AS last_updated_by,
            sysdate AS created,
            sysdate AS last_updated
          FROM stg_hiport.l03_deal_index src -- 853
          JOIN gbm.deal_index deal
            ON deal.ref_id = src.ref_id
            AND deal.ref_cd = c_sub_deal_ref_cd -- 513
          JOIN gbm.item_index ip
            ON ip.ref_id = src.investment_portfolio_ref_id -- 513
            AND ip.ref_cd = c_ip_ref_cd
          JOIN gbm.item_index fi
            ON fi.ref_id = src.security_ref_id -- 513
            AND fi.ref_cd = c_fi_ref_cd) src
        ON (tgt.no = src.no)
        WHEN MATCHED THEN UPDATE SET
          tgt.investment_portfolio_no = src.investment_portfolio_no,
          tgt.asset_no = src.asset_no,
          tgt.last_updated = src.last_updated,
          tgt.last_updated_by = src.last_updated_by
        WHERE ut.are_numbers_equal(tgt.investment_portfolio_no, src.investment_portfolio_no) != dt.c_true
          OR ut.are_numbers_equal(tgt.asset_no, src.asset_no) != dt.c_true
        WHEN NOT MATCHED THEN INSERT
        (
          tgt.no,
          tgt.sup_no,
          tgt.class_cd,
          tgt.dealor_no,
          tgt.dealee_no,
          tgt.investment_portfolio_no,
          tgt.asset_no,
          tgt.intermediary_no,
          tgt.start_date,
          tgt.end_date,
          tgt.is_active,
          tgt.source_id,
          tgt.ref_id,
          tgt.ref_cd,
          tgt.last_updated,
          tgt.last_updated_by
        )
        VALUES
        (
          src.no,
          src.sup_no,
          src.class_cd,
          src.dealor_no,
          src.dealee_no,
          src.investment_portfolio_no,
          src.asset_no,
          src.intermediary_no,
          src.start_date,
          src.end_date,
          src.is_active,
          src.source_id,
          src.ref_id,
          src.ref_cd,
          src.last_updated,
          src.last_updated_by
        );

        logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      load_sub_deal_index;
      load_managed_sub_deals;

      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    load_sup_deals;
    load_sub_deals;

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_business_transactions
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_business_transactions';
    PROCEDURE load_normal_bus_trn
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_normal_bus_trn';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.bus_transaction tgt
      USING (SELECT
          src.*,
          portfolio.no AS sup_no,
          security.no AS sub_no,
          deal.no AS deal_no,
          c_btr_ref_cd AS ref_cd,
          c_source_id AS source_id,
          c_etl_id AS last_updated_by,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l050_normal_bus_trn src
        JOIN gbm.item_index portfolio
          ON portfolio.ref_id = src.portfolio_ref_id
          AND portfolio.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index security
          ON security.ref_id = src.security_ref_id
          AND security.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd
        WHERE src.transaction_status = c_btr_status_correct) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.class_cd,
        tgt.start_datetime,
        tgt.effective_date,
        tgt.for_fin_period,
        tgt.currency_id,
        tgt.amount,
        tgt.amount_in_local_currency,
        tgt.ref_cd,
        tgt.ref_id,
        tgt.transaction_status,
        tgt.created,
        tgt.last_updated,
        tgt.last_updated_by,
        tgt.source_id,
        tgt.is_reversal,
        tgt.reversed_date,
        tgt.deal_no
      )
      VALUES
      (
        gbm.bus_trn_no_seq.NEXTVAL,
        src.class_cd,
        src.start_datetime,
        src.effective_date,
        src.for_fin_period,
        src.currency_id,
        src.amount,
        src.amount_in_local_currency,
        src.ref_cd,
        src.ref_id,
        src.transaction_status,
        src.created,
        src.last_updated,
        src.last_updated_by,
        src.source_id,
        src.is_reversal,
        src.reversed_date,
        src.deal_no
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_normal_bus_trn_cnd
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_normal_bus_trn_cnd';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE/*+ PARALLEL(16) */ INTO gbm.bus_transaction tgt
      USING (SELECT
          src.*,
          portfolio.no AS sup_no,
          security.no AS sub_no,
          deal.no AS deal_no,
          c_btr_ref_cd AS ref_cd,
          c_etl_id AS last_updated_by,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l050_normal_bus_trn src
        JOIN gbm.item_index portfolio
          ON portfolio.ref_id = src.portfolio_ref_id
          AND portfolio.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index security
          ON security.ref_id = src.security_ref_id
          AND security.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd
        WHERE src.transaction_status = c_btr_status_cancelled) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.transaction_status = src.transaction_status,
        tgt.last_updated = src.last_updated,
        tgt.last_updated_by = src.last_updated_by,
        tgt.is_reversal = src.is_reversal,
        tgt.reversed_date = src.reversed_date
      WHERE tgt.transaction_status = c_btr_status_correct;
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_reval_bus_trn
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_reval_bus_trn';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE/*+ PARALLEL(16) */ INTO gbm.bus_transaction tgt
      USING (SELECT
          src.*,
          portfolio.no AS sup_no,
          security.no AS sub_no,
          deal.no AS deal_no,
          c_btr_ref_cd AS ref_cd,
          c_source_id AS source_id,
          c_etl_id AS last_updated_by,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l051_reval_bus_trn src
        JOIN gbm.item_index portfolio
          ON portfolio.ref_id = src.portfolio_ref_id
          AND portfolio.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index security
          ON security.ref_id = src.security_ref_id
          AND security.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.class_cd,
        tgt.start_datetime,
        tgt.effective_date,
        tgt.for_fin_period,
        tgt.currency_id,
        tgt.amount,
        tgt.amount_in_local_currency,
        tgt.ref_cd,
        tgt.ref_id,
        tgt.transaction_status,
        tgt.created,
        tgt.last_updated,
        tgt.last_updated_by,
        tgt.source_id,
        tgt.is_reversal,
        tgt.reversed_date,
        tgt.deal_no
      )
      VALUES
      (
        gbm.bus_trn_no_seq.NEXTVAL,
        src.class_cd,
        src.start_datetime,
        src.effective_date,
        src.for_fin_period,
        src.currency_id,
        src.amount,
        src.amount_in_local_currency,
        src.ref_cd,
        src.ref_id,
        src.transaction_status,
        src.created,
        src.last_updated,
        src.last_updated_by,
        src.source_id,
        src.is_reversal,
        src.reversed_date,
        src.deal_no
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_acc_int_bus_trn
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_acc_int_bus_trn';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE/*+ PARALLEL(16) */ INTO gbm.bus_transaction tgt
      USING (SELECT
          src.*,
          portfolio.no AS sup_no,
          security.no AS sub_no,
          deal.no AS deal_no,
          c_btr_ref_cd AS ref_cd,
          c_source_id AS source_id,
          c_etl_id AS last_updated_by,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l052_acc_int_bus_trn src
        JOIN gbm.item_index portfolio
          ON portfolio.ref_id = src.portfolio_ref_id
          AND portfolio.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index security
          ON security.ref_id = src.security_ref_id
          AND security.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.class_cd,
        tgt.start_datetime,
        tgt.effective_date,
        tgt.for_fin_period,
        tgt.currency_id,
        tgt.amount,
        tgt.amount_in_local_currency,
        tgt.ref_cd,
        tgt.ref_id,
        tgt.transaction_status,
        tgt.created,
        tgt.last_updated,
        tgt.last_updated_by,
        tgt.source_id,
        tgt.is_reversal,
        tgt.reversed_date,
        tgt.deal_no
      )
      VALUES
      (
        gbm.bus_trn_no_seq.NEXTVAL,
        src.class_cd,
        src.start_datetime,
        src.effective_date,
        src.for_fin_period,
        src.currency_id,
        src.amount,
        src.amount_in_local_currency,
        src.ref_cd,
        src.ref_id,
        src.transaction_status,
        src.created,
        src.last_updated,
        src.last_updated_by,
        src.source_id,
        src.is_reversal,
        src.reversed_date,
        src.deal_no
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    load_normal_bus_trn;
    load_normal_bus_trn_cnd;
    load_reval_bus_trn;
    load_acc_int_bus_trn;
    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_cash_business_trn
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_cash_business_trn';
    PROCEDURE load_normal_bus_trn_cash
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_normal_bus_trn_cash';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.bus_transaction tgt
      USING (SELECT
          src.*,
          portfolio.no AS sup_no,
          security.no AS sub_no,
          deal.no AS deal_no,
          c_btr_ref_cd AS ref_cd,
          c_source_id AS source_id,
          c_etl_id AS last_updated_by,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l050_normal_bus_trn_cash src
        JOIN gbm.item_index portfolio
          ON portfolio.ref_id = src.portfolio_ref_id
          AND portfolio.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index security
          ON security.ref_id = src.security_ref_id
          AND security.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd
        WHERE src.transaction_status = c_btr_status_correct) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.class_cd,
        tgt.start_datetime,
        tgt.effective_date,
        tgt.for_fin_period,
        tgt.currency_id,
        tgt.amount,
        tgt.amount_in_local_currency,
        tgt.ref_cd,
        tgt.ref_id,
        tgt.transaction_status,
        tgt.created,
        tgt.last_updated,
        tgt.last_updated_by,
        tgt.source_id,
        tgt.is_reversal,
        tgt.reversed_date,
        tgt.deal_no
      )
      VALUES
      (
        gbm.bus_trn_no_seq.NEXTVAL,
        src.class_cd,
        src.start_datetime,
        src.effective_date,
        src.for_fin_period,
        src.currency_id,
        src.amount,
        src.amount_in_local_currency,
        src.ref_cd,
        src.ref_id,
        src.transaction_status,
        src.created,
        src.last_updated,
        src.last_updated_by,
        src.source_id,
        src.is_reversal,
        src.reversed_date,
        src.deal_no
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_normal_bus_trn_cash_cnd
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_normal_bus_trn_cash_cnd';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE/*+ PARALLEL(16) */ INTO gbm.bus_transaction tgt
      USING (SELECT
          src.*,
          portfolio.no AS sup_no,
          security.no AS sub_no,
          deal.no AS deal_no,
          c_btr_ref_cd AS ref_cd,
          c_etl_id AS last_updated_by,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l050_normal_bus_trn_cash src
        JOIN gbm.item_index portfolio
          ON portfolio.ref_id = src.portfolio_ref_id
          AND portfolio.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index security
          ON security.ref_id = src.security_ref_id
          AND security.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd
        WHERE src.transaction_status = c_btr_status_cancelled) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.transaction_status = src.transaction_status,
        tgt.last_updated = src.last_updated,
        tgt.last_updated_by = src.last_updated_by,
        tgt.is_reversal = src.is_reversal,
        tgt.reversed_date = src.reversed_date
      WHERE tgt.transaction_status = c_btr_status_correct;
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    load_normal_bus_trn_cash;
    load_normal_bus_trn_cash_cnd;
    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_business_transaction_item
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_business_transaction_item';
    PROCEDURE load_normal_bus_trn_item
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_normal_bus_trn_item';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE/*+ PARALLEL(16) */ INTO gbm.bus_trn_item tgt
      USING (SELECT
          src.*,
          wrto_fund.no AS wrto_fund_no,
          item.no AS item_id,
          deal.no AS deal_no,
          btr.no AS bus_trn_no,
          c_bti_ref_cd AS ref_cd,
          c_source_id AS source_id,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l061_bus_transaction_item src
        JOIN gbm.item_index wrto_fund
          ON wrto_fund.ref_id = src.wrto_fund_ref_id
          AND wrto_fund.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index item
          ON item.ref_id = src.item_ref_id
          AND item.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd
        JOIN gbm.bus_transaction btr
          ON btr.ref_id = src.bus_trn_ref_id
          AND btr.ref_cd = c_btr_ref_cd
          AND btr.transaction_status != c_btr_status_cancelled) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.item_type_cd = src.item_type_cd
      WHERE tgt.item_type_cd != src.item_type_cd
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.bus_trn_no,
        tgt.no,
        tgt.deal_no,
        tgt.start_datetime,
        tgt.item_id,
        tgt.item_type_cd,
        tgt.wrto_fund_no,
        tgt.quantity,
        tgt.amount,
        tgt.price,
        tgt.created,
        tgt.source_id,
        tgt.ref_id,
        tgt.ref_cd
      )
      VALUES
      (
        src.bus_trn_no,
        gbm.bus_trn_item_no_seq.NEXTVAL,
        src.deal_no,
        src.start_datetime,
        src.item_id,
        src.item_type_cd,
        src.wrto_fund_no,
        src.quantity,
        src.amount,
        src.price,
        src.created,
        src.source_id,
        src.ref_id,
        src.ref_cd
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_normal_bus_trn_item_cnd
    IS
      v_log_no          NUMBER;
      v_proc_name       VARCHAR2(100) := 'load_normal_bus_trn_item_cnd';

      CURSOR c_del_trn IS
        SELECT
          bt.no
        FROM gbm.bus_transaction bt
        JOIN gbm.bus_trn_item bti
          ON bti.bus_trn_no = bt.no
        WHERE bt.ref_cd = c_btr_ref_cd
          AND bt.transaction_status = c_btr_status_cancelled;

      TYPE t_del_trn IS TABLE OF c_del_trn % ROWTYPE
        INDEX BY PLS_INTEGER;
      v_del_trn         t_del_trn;

      v_records_updated NUMBER        := 0;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      OPEN c_del_trn;
      LOOP
        FETCH c_del_trn
        BULK COLLECT INTO v_del_trn
        LIMIT c_bulk_collect_limit;

        FORALL i IN 1 .. v_del_trn.COUNT
          DELETE FROM gbm.bus_trn_item bti
            WHERE bti.bus_trn_no = v_del_trn(i).no;

        v_records_updated := v_records_updated + SQL % ROWCOUNT;

        EXIT WHEN v_del_trn.COUNT = 0;

        COMMIT;

      END LOOP;

      CLOSE c_del_trn;
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        IF c_del_trn % ISOPEN THEN
          CLOSE c_del_trn;
        END IF;
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_reval_bus_trn_item
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_reval_bus_trn_item';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.bus_trn_item tgt
      USING (SELECT
          src.*,
          wrto_fund.no AS wrto_fund_no,
          item.no AS item_id,
          deal.no AS deal_no,
          btr.no AS bus_trn_no,
          c_bti_ref_cd AS ref_cd,
          c_source_id AS source_id,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l051_reval_bus_trn src
        JOIN gbm.item_index wrto_fund
          ON wrto_fund.ref_id = src.portfolio_ref_id
          AND wrto_fund.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index item
          ON item.ref_id = src.security_ref_id
          AND item.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd
        JOIN gbm.bus_transaction btr
          ON btr.ref_id = src.ref_id
          AND btr.ref_cd = c_btr_ref_cd) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.bus_trn_no,
        tgt.no,
        tgt.deal_no,
        tgt.start_datetime,
        tgt.item_id,
        tgt.item_type_cd,
        tgt.wrto_fund_no,
        tgt.quantity,
        tgt.amount,
        tgt.price,
        tgt.created,
        tgt.source_id,
        tgt.ref_id,
        tgt.ref_cd
      )
      VALUES
      (
        src.bus_trn_no,
        gbm.bus_trn_item_no_seq.NEXTVAL,
        src.deal_no,
        src.start_datetime,
        src.item_id,
        src.class_cd,
        src.wrto_fund_no,
        NULL,
        src.amount,
        NULL,
        src.created,
        src.source_id,
        src.ref_id,
        src.ref_cd
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, dbms_utility.format_error_backtrace());
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_acc_int_bus_trn_item
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_acc_int_bus_trn_item';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE/*+ PARALLEL(16) */ INTO gbm.bus_trn_item tgt
      USING (SELECT
          src.*,
          wrto_fund.no AS wrto_fund_no,
          item.no AS item_id,
          deal.no AS deal_no,
          btr.no AS bus_trn_no,
          c_bti_ref_cd AS ref_cd,
          c_source_id AS source_id,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l052_acc_int_bus_trn src
        JOIN gbm.item_index wrto_fund
          ON wrto_fund.ref_id = src.portfolio_ref_id
          AND wrto_fund.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index item
          ON item.ref_id = src.security_ref_id
          AND item.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sup_deal_ref_cd
        JOIN gbm.bus_transaction btr
          ON btr.ref_id = src.ref_id
          AND btr.ref_cd = c_btr_ref_cd) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.bus_trn_no,
        tgt.no,
        tgt.deal_no,
        tgt.start_datetime,
        tgt.item_id,
        tgt.item_type_cd,
        tgt.wrto_fund_no,
        tgt.quantity,
        tgt.amount,
        tgt.price,
        tgt.created,
        tgt.source_id,
        tgt.ref_id,
        tgt.ref_cd
      )
      VALUES
      (
        src.bus_trn_no,
        gbm.bus_trn_item_no_seq.NEXTVAL,
        src.deal_no,
        src.start_datetime,
        src.item_id,
        src.class_cd,
        src.wrto_fund_no,
        NULL,
        src.amount,
        NULL,
        src.created,
        src.source_id,
        src.ref_id,
        src.ref_cd
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    load_normal_bus_trn_item;
    load_normal_bus_trn_item_cnd;
    load_reval_bus_trn_item;
    load_acc_int_bus_trn_item;
    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, dbms_utility.format_error_backtrace());
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_cash_business_trn_item
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_cash_business_trn_item';
    PROCEDURE load_cash_bus_trn_item
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_cash_bus_trn_item';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE/*+ PARALLEL(16) */ INTO gbm.bus_trn_item tgt
      USING (SELECT
          src.*,
          wrto_fund.no AS wrto_fund_no,
          item.no AS item_id,
          deal.no AS deal_no,
          btr.no AS bus_trn_no,
          c_source_id AS source_id,
          c_bti_ref_cd AS ref_cd,
          sysdate AS created,
          sysdate AS last_updated
        FROM stg_hiport.l061_cash_bus_trn_item src
        JOIN gbm.item_index wrto_fund
          ON wrto_fund.ref_id = src.wrto_fund_ref_id
          AND wrto_fund.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index item
          ON item.ref_id = src.item_ref_id
          AND item.ref_cd = c_fi_ref_cd
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd
        JOIN gbm.bus_transaction btr
          ON btr.ref_id = src.bus_trn_ref_id
          AND btr.ref_cd = c_btr_ref_cd
          AND btr.transaction_status != c_btr_status_cancelled) src
      ON (src.ref_id = tgt.ref_id
        AND src.ref_cd = tgt.ref_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.item_type_cd = src.item_type_cd
      WHERE tgt.item_type_cd != src.item_type_cd
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.bus_trn_no,
        tgt.no,
        tgt.deal_no,
        tgt.start_datetime,
        tgt.item_id,
        tgt.item_type_cd,
        tgt.wrto_fund_no,
        tgt.quantity,
        tgt.amount,
        tgt.price,
        tgt.created,
        tgt.source_id,
        tgt.ref_id,
        tgt.ref_cd
      )
      VALUES
      (
        src.bus_trn_no,
        gbm.bus_trn_item_no_seq.NEXTVAL,
        src.deal_no,
        src.start_datetime,
        src.item_id,
        src.item_type_cd,
        src.wrto_fund_no,
        src.quantity,
        src.amount,
        src.price,
        src.created,
        src.source_id,
        src.ref_id,
        src.ref_cd
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_cash_bus_trn_item_cnd
    IS
      v_log_no          NUMBER;
      v_proc_name       VARCHAR2(100) := 'load_cash_bus_trn_item_cnd';

      CURSOR c_del_trn IS
        SELECT
          bt.no
        FROM gbm.bus_transaction bt
        JOIN gbm.bus_trn_item bti
          ON bti.bus_trn_no = bt.no
        WHERE bt.ref_cd = c_btr_ref_cd
          AND bt.transaction_status = c_btr_status_cancelled;

      TYPE t_del_trn IS TABLE OF c_del_trn % ROWTYPE
        INDEX BY PLS_INTEGER;
      v_del_trn         t_del_trn;

      v_records_updated NUMBER        := 0;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      OPEN c_del_trn;
      LOOP
        FETCH c_del_trn
        BULK COLLECT INTO v_del_trn
        LIMIT c_bulk_collect_limit;

        FORALL i IN 1 .. v_del_trn.COUNT
          DELETE FROM gbm.bus_trn_item bti
            WHERE bti.bus_trn_no = v_del_trn(i).no;

        v_records_updated := v_records_updated + SQL % ROWCOUNT;

        EXIT WHEN v_del_trn.COUNT = 0;

        COMMIT;

      END LOOP;

      CLOSE c_del_trn;
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        IF c_del_trn % ISOPEN THEN
          CLOSE c_del_trn;
        END IF;
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    load_cash_bus_trn_item;
    load_cash_bus_trn_item_cnd;
    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_fin_instr_invested_in
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_fin_instr_invested_in';
    PROCEDURE load_fin_instr_invested_in_pos
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_fin_instr_invested_in_pos';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO fin_instrument_invested_in tgt
      USING (SELECT
          src.start_datetime,
          src.end_datetime,
          src.effective_date,
          src.security_ref_id,
          src.portfolio_ref_id,
          CASE
            WHEN gbm.ers_common.in_fi_class(fi.asset_class_cd, 'CSH') = 1 THEN NVL(src.unit_holding_count, src.investment_value)
            ELSE src.unit_holding_count
          END AS unit_holding_count,
          CASE
            WHEN gbm.ers_common.in_fi_class(fi.asset_class_cd, 'CSH') = 1 THEN NVL(src.unit_holding_value, src.investment_value)
            ELSE src.unit_holding_value
          END AS unit_holding_value,
          src.investment_value,
          src.interest_accrued,
          src.is_active,
          src.in_gbm,
          fi.no AS invested_in_no,
          ip.no AS no
        FROM stg_hiport.l09_fin_instrument_invested_in src
        JOIN gbm.item_index ip
          ON ip.ref_id = src.portfolio_ref_id
          AND ip.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index fi
          ON fi.ref_id = src.security_ref_id
          AND fi.ref_cd = c_fi_ref_cd) src
      ON (tgt.start_datetime = src.start_datetime
        AND tgt.invested_in_no = src.invested_in_no
        AND tgt.no = src.no)
      WHEN MATCHED THEN UPDATE SET
        --				tgt.end_datetime = NVL(src.end_datetime, tgt.end_datetime),
        --				tgt.is_active = NVL(src.is_active, tgt.is_active),
        tgt.unit_holding_count = NVL(src.unit_holding_count, tgt.unit_holding_count),
        tgt.unit_holding_value = NVL(src.unit_holding_value, tgt.unit_holding_value),
        tgt.investment_value = NVL(src.investment_value, tgt.investment_value),
        tgt.interest_accrued = NVL(src.interest_accrued, tgt.interest_accrued)
      WHERE
        --				tgt.end_datetime IS NULL
        --				OR tgt.end_datetime != src.end_datetime
        ut.are_numbers_equal(tgt.unit_holding_count, src.unit_holding_count) != dt.c_true
        OR ut.are_numbers_equal(tgt.unit_holding_value, src.unit_holding_value) != dt.c_true
        OR ut.are_numbers_equal(tgt.investment_value, src.investment_value) != dt.c_true
        OR ut.are_numbers_equal(tgt.interest_accrued, src.interest_accrued) != dt.c_true
      --				OR tgt.is_active != src.is_active
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.no,
        tgt.invested_in_no,
        tgt.unit_holding_count,
        tgt.unit_holding_value,
        tgt.investment_value,
        tgt.interest_accrued,
        tgt.effective_date,
        tgt.start_datetime,
        tgt.end_datetime,
        tgt.is_active
      )
      VALUES
      (
        src.no,
        src.invested_in_no,
        src.unit_holding_count,
        src.unit_holding_value,
        src.investment_value,
        src.interest_accrued,
        src.effective_date,
        src.start_datetime,
        src.end_datetime,
        src.is_active
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_fin_instr_invested_in_cnd
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_fin_instr_invested_cnd';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO fin_instrument_invested_in tgt
      USING (SELECT
          src.*,
          fi.no AS invested_in_no,
          ip.no AS no
        FROM stg_hiport.l09_fin_instr_invested_in_cnd src
        JOIN gbm.item_index ip
          ON ip.ref_id = src.portfolio_ref_id
          AND ip.ref_cd = c_ip_ref_cd
        JOIN gbm.item_index fi
          ON fi.ref_id = src.security_ref_id
          AND fi.ref_cd = c_fi_ref_cd) src
      ON (tgt.start_datetime = src.start_datetime
        AND tgt.invested_in_no = src.invested_in_no
        AND tgt.no = src.no)
      WHEN MATCHED THEN UPDATE SET
        tgt.is_active = NVL(src.is_active, tgt.is_active)
      DELETE WHERE tgt.is_active = 0;

      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    load_fin_instr_invested_in_pos;
    load_fin_instr_invested_in_cnd;

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_asset_pricing
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_asset_pricing';
    PROCEDURE load_daily_market_price
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_daily_market_price';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd,
      m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.asset_valuation tgt
      USING (SELECT
          src.*,
          c_valuation_ref_cd AS ref_cd,
          c_source_id AS source_id,
          c_valuation_calculator_id AS calculator_id,
          c_valuation_method_cd AS method_cd,
          c_etl_id AS last_updated_by,
          fi.no AS asset_no
        FROM stg_hiport.l10_asset_valuation src
        JOIN gbm.item_index fi
          ON fi.ref_id = src.asset_ref_id
          AND fi.ref_cd = c_fi_ref_cd) src
      ON (tgt.asset_no = src.asset_no
        AND tgt.start_datetime = src.start_datetime)
      WHEN MATCHED THEN UPDATE SET
        tgt.value_amount = NVL(src.value_amount, tgt.value_amount),
        tgt.end_datetime = src.end_datetime,
        tgt.last_updated = src.last_updated,
        tgt.last_updated_by = src.last_updated_by
      WHERE tgt.value_amount != NVL(src.value_amount, tgt.value_amount)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.asset_no,
        tgt.start_datetime,
        tgt.end_datetime,
        tgt.is_active,
        tgt.valuation_date,
        tgt.value_amount,
        tgt.method_cd,
        tgt.calculator_id,
        tgt.source_id,
        tgt.ref_id,
        tgt.ref_cd,
        tgt.created,
        tgt.last_updated_by,
        tgt.last_updated
      )
      VALUES
      (
        src.asset_no,
        src.start_datetime,
        src.end_datetime,
        src.is_active,
        src.valuation_date,
        src.value_amount,
        src.method_cd,
        src.calculator_id,
        src.source_id,
        src.ref_id,
        src.ref_cd,
        src.created,
        src.last_updated_by,
        src.last_updated
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE update_market_price_end_date
    IS
      v_log_no          logit.run_times.no % TYPE;
      v_proc_name       logit.run_times.procedure_name % TYPE := 'update_market_price_end_date';
      v_records_updated NUMBER                                := 0;

      CURSOR c_asset_valuation IS
        SELECT /*+ PARALLEL(16) */
          ROWID AS r_id,
          LEAD(av.start_datetime) OVER (PARTITION BY av.asset_no ORDER BY av.start_datetime ASC) - 1 AS end_datetime
        FROM gbm.asset_valuation av
        WHERE av.method_cd = c_valuation_method_cd
          AND av.ref_cd = c_valuation_ref_cd;

      TYPE
      t_asset_valuation IS TABLE OF c_asset_valuation % ROWTYPE
        INDEX BY PLS_INTEGER;
      v_asset_valuation 
      t_asset_valuation;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, sysdate);

      OPEN c_asset_valuation;

      LOOP
        FETCH c_asset_valuation
        BULK COLLECT INTO v_asset_valuation
        LIMIT c_bulk_collect_limit;

        EXIT WHEN v_asset_valuation.COUNT = 0;

        FORALL i IN 1 .. v_asset_valuation.COUNT
          UPDATE gbm.asset_valuation tgt
            SET tgt.end_datetime = v_asset_valuation(i).end_datetime
            WHERE tgt.ROWID = v_asset_valuation(i).r_id
            AND ((tgt.end_datetime IS NULL
            AND v_asset_valuation(i).end_datetime IS NOT NULL)
            OR (tgt.end_datetime IS NOT NULL
            AND v_asset_valuation(i).end_datetime IS NULL)
            OR tgt.end_datetime != v_asset_valuation(i).end_datetime);

        v_records_updated := v_records_updated + SQL % ROWCOUNT;
      END LOOP;

      CLOSE c_asset_valuation;

      logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, sysdate);
    EXCEPTION
      WHEN OTHERS THEN
        IF c_asset_valuation % ISOPEN THEN
          CLOSE c_asset_valuation;
        END IF;
        logit.etl_run_times.exception_etl_procedure(v_log_no, sysdate, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd,
    m_fin_month_cd, systimestamp);

    load_daily_market_price;
    update_market_price_end_date;

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_market_risk
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_market_risk';

    PROCEDURE load_instrument_market_risk
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_instrument_market_risk';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.fin_market_risk tgt
      USING (SELECT
          src.*,
          c_etl_id AS last_updated_by,
          fi.no AS asset_no
        FROM stg_hiport.l22_fin_market_risk src
        JOIN gbm.item_index fi
          ON fi.ref_id = src.ref_id
          AND fi.ref_cd = c_fi_ref_cd) src
      ON (tgt.asset_no = src.asset_no
        AND tgt.start_datetime = src.start_datetime)
      WHEN MATCHED THEN UPDATE SET
        tgt.dispersion = NVL(src.dispersion, tgt.dispersion),
        tgt.modified_duration = NVL(src.modified_duration, tgt.modified_duration),
        tgt.macaulay_duration = NVL(src.macaulay_duration, tgt.macaulay_duration)
      WHERE tgt.dispersion != NVL(src.dispersion, tgt.dispersion)
        OR tgt.modified_duration != NVL(src.modified_duration, tgt.modified_duration)
        OR tgt.macaulay_duration != NVL(src.macaulay_duration, tgt.macaulay_duration)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.asset_no,
        tgt.start_datetime,
        tgt.macaulay_duration,
        tgt.modified_duration,
        tgt.dispersion
      )
      VALUES
      (
        src.asset_no,
        src.start_datetime,
        src.macaulay_duration,
        src.modified_duration,
        src.dispersion
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_deal_market_risk
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_deal_market_risk';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      MERGE /*+ PARALLEL(16) */ INTO gbm.deal_market_risk tgt
      USING (SELECT
          src.*,
          c_source_id AS source_id,
          c_etl_id AS last_updated_by,
          sysdate AS last_updated,
          sysdate AS created,
          c_deal_marketrisk_ref_cd AS ref_cd,
          c_risk_method_cd AS method_cd,
          deal.no AS deal_no
        FROM stg_hiport.l23_deal_market_risk src
        JOIN gbm.deal_index deal
          ON deal.ref_id = src.deal_ref_id
          AND deal.ref_cd = c_sub_deal_ref_cd) src
      ON (tgt.deal_no = src.deal_no
        AND tgt.start_date = src.start_datetime)
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.deal_no,
        tgt.convexity,
        tgt.start_date,
        tgt.method_cd,
        tgt.fin_year_cd,
        tgt.fin_month_cd,
        tgt.ref_cd,
        tgt.ref_id,
        tgt.source_id,
        tgt.last_updated,
        tgt.last_updated_by
      )
      VALUES
      (
        src.deal_no,
        src.convexity,
        src.start_datetime,
        src.method_cd,
        src.fin_year_cd,
        src.fin_month_cd,
        src.ref_cd,
        src.ref_id,
        src.source_id,
        src.last_updated,
        src.last_updated_by
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE update_market_risk_end_date
    IS
      v_log_no          logit.run_times.no % TYPE;
      v_proc_name       logit.run_times.procedure_name % TYPE := 'update_market_risk_end_date';
      v_records_updated NUMBER                                := 0;

      CURSOR c_fin_market_risk IS
        SELECT /*+ parallel(16) */
          ROWID AS r_id,
          start_datetime,
          asset_no,
          LEAD(start_datetime) OVER (PARTITION BY asset_no ORDER BY start_datetime ASC) - 1 AS end_datetime
        FROM gbm.fin_market_risk;

      TYPE
      t_fin_market_risk IS TABLE OF c_fin_market_risk % ROWTYPE
        INDEX BY PLS_INTEGER;
      v_fin_market_risk 
      t_fin_market_risk;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, sysdate);
      --This code is not currently required as FIN_MARKET_RISK doesn't have an END_DATETIME
      --Can be uncommented and modifed if this functionality becomes a requirement.
      OPEN c_fin_market_risk;

      LOOP
        FETCH c_fin_market_risk
        BULK COLLECT INTO v_fin_market_risk
        LIMIT c_bulk_collect_limit;

        EXIT WHEN v_fin_market_risk.COUNT = 0;

        FORALL i IN 1 .. v_fin_market_risk.COUNT
          UPDATE gbm.fin_market_risk tgt
            SET tgt.end_datetime = v_fin_market_risk(i).end_datetime
            WHERE tgt.ROWID = v_fin_market_risk(i).r_id
            AND (tgt.end_datetime IS NULL
            OR tgt.end_datetime != v_fin_market_risk(i).end_datetime);

        v_records_updated := v_records_updated + SQL % ROWCOUNT;
      END LOOP;

      CLOSE c_fin_market_risk;

      logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, sysdate);
    EXCEPTION
      WHEN OTHERS THEN
        IF c_fin_market_risk % ISOPEN THEN
          CLOSE c_fin_market_risk;
        END IF;
        logit.etl_run_times.exception_etl_procedure(v_log_no, sysdate, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd,
    m_fin_month_cd, systimestamp);

    load_instrument_market_risk;
    load_deal_market_risk;
    update_market_risk_end_date;

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;
  PROCEDURE load_custom_item_class
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_custom_item_class';
    PROCEDURE load_custom_item_class_domain
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_custom_item_class_domain';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, sysdate);
      MERGE INTO custom.custom_item_class_domain tgt
      USING (SELECT
          domain_cd,
          class_cd,
          class_name,
          class_description
        FROM stg_hiport.l210_custom_item_class_domain) src
      ON (tgt.domain_cd = src.domain_cd
        AND tgt.class_cd = src.class_cd)
      WHEN MATCHED THEN UPDATE SET
        tgt.class_name = NVL(src.class_name, tgt.class_name)
      WHERE ut.are_strings_equal(tgt.class_name, src.class_name) = 0
      WHEN NOT MATCHED THEN INSERT
      (
        tgt.domain_cd,
        tgt.class_cd,
        tgt.class_name,
        tgt.class_description
      )
      VALUES
      (
        src.domain_cd,
        src.class_cd,
        src.class_name,
        src.class_description
      );
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      logit.etl_run_times.end_etl_procedure(v_log_no, sysdate);
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, sysdate, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    FUNCTION load_custom_item_class(p_data_cursor IN sys_refcursor)
      RETURN PLS_INTEGER
    AS
      TYPE t_data IS TABLE OF custom.custom_item_class % ROWTYPE
        INDEX BY PLS_INTEGER;
      v_data            t_data;

      v_records_updated PLS_INTEGER := 0;
    BEGIN
      LOOP
        FETCH p_data_cursor
        BULK COLLECT INTO v_data
        LIMIT c_bulk_collect_limit;

        FORALL i IN 1 .. v_data.COUNT
          INSERT /*+ append parallel */ INTO custom.custom_item_class
          VALUES v_data (i);

        v_records_updated := v_records_updated + SQL % ROWCOUNT;

        COMMIT;

        EXIT WHEN v_data.COUNT = 0;

      END LOOP;

      RETURN v_records_updated;
    END;

    PROCEDURE update_item_cls_end_datetime(p_domain_cd custom.custom_item_class_domain.domain_cd % TYPE)
    IS
      v_log_no          NUMBER;
      v_proc_name       VARCHAR2(100) := 'update_item_cls_end_datetime - Domain ' || p_domain_cd;

      CURSOR c_item_class IS
        SELECT
          r_id,
          end_datetime
        FROM (SELECT
            ROWID AS r_id,
            LEAD(cic.start_datetime) OVER (PARTITION BY item_no, cic.custom_item_class_domain_cd ORDER BY cic.start_datetime ASC) - 1 AS end_datetime
          FROM custom.custom_item_class cic
          WHERE cic.custom_item_class_domain_cd = p_domain_cd)
        WHERE end_datetime IS NOT NULL;

      TYPE t_item_class IS TABLE OF c_item_class % ROWTYPE
        INDEX BY PLS_INTEGER;
      v_item_class      t_item_class;

      v_records_updated NUMBER        := 0;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      OPEN c_item_class;
      LOOP
        FETCH c_item_class
        BULK COLLECT INTO v_item_class
        LIMIT c_bulk_collect_limit;

        FORALL i IN 1 .. v_item_class.COUNT
          UPDATE custom.custom_item_class tgt
            SET tgt.end_datetime = v_item_class(i).end_datetime
            WHERE tgt.ROWID = v_item_class(i).r_id
            AND (tgt.end_datetime IS NULL
            OR tgt.end_datetime != v_item_class(i).end_datetime);

        v_records_updated := v_records_updated + SQL % ROWCOUNT;

        EXIT WHEN v_item_class.COUNT = 0;

        COMMIT;

      END LOOP;

      CLOSE c_item_class;
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        IF c_item_class % ISOPEN THEN
          CLOSE c_item_class;
        END IF;
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_gl_codes
    AS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_gl_codes';

      PROCEDURE load_custom_item_class_gl_code
      IS
        v_log_no          NUMBER;
        v_proc_name       VARCHAR2(100) := 'load_custom_item_class_gl_code';

        c_data            sys_refcursor;
        v_records_updated PLS_INTEGER   := 0;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

        OPEN c_data FOR
        SELECT /*+ parallel(16) */
          start_datetime,
          item_no,
          custom_item_class_domain_cd,
          custom_item_class_cd,
          end_datetime
        FROM (WITH changed_gl_codes AS (SELECT
              MIN(src.start_datetime) AS start_datetime,
              src.security
            FROM stg_hiport.m_fin_instrument_gl_group src
            JOIN gbm.item_index fi
              ON fi.ref_id = src.security
              AND fi.ref_cd = c_fi_ref_cd
            JOIN (SELECT
                  item_no,
                  custom_item_class_domain_cd,
                  MAX(start_datetime) AS start_datetime
                FROM custom.custom_item_class cic
                WHERE cic.custom_item_class_domain_cd = c_gl_domain_cd
                GROUP BY
                  item_no,
                  custom_item_class_domain_cd) max_cic
              ON max_cic.item_no = fi.no
            JOIN custom.custom_item_class cic
              ON cic.custom_item_class_domain_cd = c_gl_domain_cd
              AND cic.item_no = max_cic.item_no
              AND cic.start_datetime = max_cic.start_datetime
              AND src.start_datetime > cic.start_datetime
              AND cic.custom_item_class_cd != src.gl_acc
            WHERE src.gl_acc IS NOT NULL
            GROUP BY
              src.security
              UNION
            SELECT
              MIN(start_datetime) AS start_datetime,
              src.security
            FROM stg_hiport.m_fin_instrument_gl_group src
            WHERE gl_acc IS NOT NULL
              AND NOT EXISTS (SELECT
                  1
                FROM gbm.item_index fi
                JOIN custom.custom_item_class cic
                  ON cic.custom_item_class_domain_cd = c_gl_domain_cd
                  AND cic.item_no = fi.no
                WHERE fi.ref_id = src.security
                  AND fi.ref_cd = c_fi_ref_cd)
            GROUP BY
              src.security)
          SELECT
            gl.start_datetime,
            fi.no AS item_no,
            c_gl_domain_cd AS custom_item_class_domain_cd,
            gl.gl_acc AS custom_item_class_cd,
            NULL AS end_datetime
          FROM changed_gl_codes update_gl
          JOIN stg_hiport.m_fin_instrument_gl_group gl
            ON gl.start_datetime = update_gl.start_datetime
            AND gl.security = update_gl.security
          JOIN gbm.item_index fi
            ON fi.ref_id = gl.security
            AND fi.ref_cd = c_fi_ref_cd
          LEFT JOIN custom.custom_item_class cic
            ON cic.custom_item_class_domain_cd = c_gl_domain_cd
            AND cic.item_no = fi.no
            AND update_gl.start_datetime BETWEEN cic.start_datetime AND NVL(cic.end_datetime, update_gl.start_datetime)
            AND cic.start_datetime != gl.start_datetime);

        v_records_updated := load_custom_item_class(c_data);

        CLOSE c_data;

        logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          IF c_data % ISOPEN THEN
            CLOSE c_data;
          END IF;
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      load_custom_item_class_gl_code;
      update_item_cls_end_datetime(c_gl_domain_cd);

      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_sector_codes
    AS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_sector_codes';

      PROCEDURE load_custom_item_class_sector
      IS
        v_log_no          NUMBER;
        v_proc_name       VARCHAR2(100) := 'load_custom_item_class_sector';

        c_data            sys_refcursor;
        v_records_updated PLS_INTEGER   := 0;
      BEGIN
        logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

        OPEN c_data FOR
        SELECT /*+ parallel(16) */
          start_datetime,
          item_no,
          custom_item_class_domain_cd,
          custom_item_class_cd,
          end_datetime
        FROM (WITH changed_sector_codes AS (SELECT
              MIN(src.start_datetime) AS start_datetime,
              src.security
            FROM stg_hiport.m_fin_instrument_sector src
            JOIN gbm.item_index fi
              ON fi.ref_id = src.security
              AND fi.ref_cd = c_fi_ref_cd
            JOIN (SELECT
                  item_no,
                  custom_item_class_domain_cd,
                  MAX(start_datetime) AS start_datetime
                FROM custom.custom_item_class cic
                WHERE cic.custom_item_class_domain_cd = c_sector_domain_cd
                GROUP BY
                  item_no,
                  custom_item_class_domain_cd) max_cic
              ON max_cic.item_no = fi.no
            JOIN custom.custom_item_class cic
              ON cic.custom_item_class_domain_cd = c_sector_domain_cd
              AND cic.item_no = max_cic.item_no
              AND cic.start_datetime = max_cic.start_datetime
              AND src.start_datetime > cic.start_datetime
              AND cic.custom_item_class_cd != src.sector
            WHERE src.sector IS NOT NULL
            GROUP BY
              src.security
              UNION
            SELECT
              MIN(start_datetime) AS start_datetime,
              src.security
            FROM stg_hiport.m_fin_instrument_sector src
            WHERE src.sector IS NOT NULL
              AND NOT EXISTS (SELECT
                  1
                FROM gbm.item_index fi
                JOIN custom.custom_item_class cic
                  ON cic.custom_item_class_domain_cd = c_sector_domain_cd
                  AND cic.item_no = fi.no
                WHERE fi.ref_id = src.security
                  AND fi.ref_cd = c_fi_ref_cd)
            GROUP BY
              src.security)
          SELECT
            sector.start_datetime,
            fi.no AS item_no,
            c_sector_domain_cd AS custom_item_class_domain_cd,
            sector.sector AS custom_item_class_cd,
            NULL AS end_datetime
          FROM changed_sector_codes update_sector
          JOIN stg_hiport.m_fin_instrument_sector sector
            ON sector.start_datetime = update_sector.start_datetime
            AND sector.security = update_sector.security
          JOIN gbm.item_index fi
            ON fi.ref_id = sector.security
            AND fi.ref_cd = c_fi_ref_cd
          LEFT JOIN custom.custom_item_class cic
            ON cic.custom_item_class_domain_cd = c_sector_domain_cd
            AND cic.item_no = fi.no
            AND update_sector.start_datetime BETWEEN cic.start_datetime AND NVL(cic.end_datetime, update_sector.start_datetime)
            AND cic.start_datetime != sector.start_datetime);

        v_records_updated := load_custom_item_class(c_data);

        CLOSE c_data;

        logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
        COMMIT;
        logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
      EXCEPTION
        WHEN OTHERS THEN
          IF c_data % ISOPEN THEN
            CLOSE c_data;
          END IF;
          logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
          ROLLBACK;
          RAISE;
      END;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      load_custom_item_class_sector;
      update_item_cls_end_datetime(c_sector_domain_cd);

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
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    load_custom_item_class_domain;
    load_gl_codes;
    load_sector_codes;

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load_coupon_rates
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_coupon_rates';
    PROCEDURE update_coupon_end_datetime
    IS
      v_log_no          NUMBER;
      v_proc_name       VARCHAR2(100) := 'update_coupon_end_datetime';

      CURSOR c_coupon_rates IS
        SELECT /*+ parallel(16) */
          r_id,
          end_datetime
        FROM (SELECT
            ROWID AS r_id,
            LEAD(fiy.start_datetime) OVER (PARTITION BY fiy.item_no ORDER BY fiy.start_datetime ASC) - 1 AS end_datetime
          FROM gbm.fin_instrument_yield fiy)
        WHERE end_datetime IS NOT NULL;

      TYPE t_coupon_rates IS TABLE OF c_coupon_rates % ROWTYPE
        INDEX BY PLS_INTEGER;
      v_coupon_rates    t_coupon_rates;

      v_records_updated NUMBER        := 0;
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

      OPEN c_coupon_rates;
      LOOP
        FETCH c_coupon_rates
        BULK COLLECT INTO v_coupon_rates
        LIMIT c_bulk_collect_limit;

        FORALL i IN 1 .. v_coupon_rates.COUNT
          UPDATE gbm.fin_instrument_yield tgt
            SET tgt.end_datetime = v_coupon_rates(i).end_datetime
            WHERE tgt.ROWID = v_coupon_rates(i).r_id
            AND (tgt.end_datetime IS NULL
            OR tgt.end_datetime != v_coupon_rates(i).end_datetime);


        v_records_updated := v_records_updated + SQL % ROWCOUNT;

        EXIT WHEN v_coupon_rates.COUNT = 0;

        COMMIT;

      END LOOP;

      CLOSE c_coupon_rates;
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, v_records_updated);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        IF c_coupon_rates % ISOPEN THEN
          CLOSE c_coupon_rates;
        END IF;
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;

    PROCEDURE load_new_coupon_rates
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_new_coupon_rates';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      INSERT /*+ append parallel */ INTO gbm.fin_instrument_yield
        SELECT *
        FROM (WITH new_rates AS (SELECT
              MIN(start_datetime) AS start_datetime,
              src.item_ref_id
            FROM stg_hiport.l13_coupon_rates src
            WHERE src.yield_rate_perc IS NOT NULL
              AND NOT EXISTS (SELECT
                  1
                FROM gbm.item_index fi
                JOIN gbm.fin_instrument_yield fiy
                  ON fiy.item_no = fi.no
                WHERE fi.ref_id = src.item_ref_id
                  AND fi.ref_cd = c_fi_ref_cd)
            GROUP BY
              src.item_ref_id)
          SELECT
            fi.no AS item_no,
            rates.start_datetime,
            NULL AS end_datetime,
            1 AS is_active,
            rates.yield_rate_perc
          FROM new_rates
          JOIN stg_hiport.l13_coupon_rates rates
            ON rates.start_datetime = new_rates.start_datetime
            AND rates.item_ref_id = new_rates.item_ref_id
          JOIN gbm.item_index fi
            ON fi.ref_id = rates.item_ref_id
            AND fi.ref_cd = c_fi_ref_cd);
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
    PROCEDURE load_updated_coupon_rates
    IS
      v_log_no    logit.run_times.no % TYPE;
      v_proc_name logit.run_times.procedure_name % TYPE := 'load_updated_coupon_rates';
    BEGIN
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      INSERT /*+ append parallel */ INTO gbm.fin_instrument_yield
        SELECT *
        FROM (WITH changed_rates AS (SELECT
              MIN(src.start_datetime) AS start_datetime,
              src.item_ref_id
            FROM stg_hiport.l13_coupon_rates src
            JOIN gbm.item_index fi
              ON fi.ref_id = src.item_ref_id
              AND fi.ref_cd = c_fi_ref_cd
            JOIN (SELECT
                  item_no,
                  MAX(fiy.start_datetime) AS start_datetime
                FROM gbm.fin_instrument_yield fiy
                GROUP BY
                  fiy.item_no) max_fiy
              ON max_fiy.item_no = fi.no
            JOIN gbm.fin_instrument_yield fiy
              ON fiy.item_no = max_fiy.item_no
              AND fiy.start_datetime = max_fiy.start_datetime
              AND src.start_datetime > fiy.start_datetime
              AND fiy.yield_rate_perc != src.yield_rate_perc
            WHERE src.yield_rate_perc IS NOT NULL
            GROUP BY
              src.item_ref_id)
          SELECT
            fi.no AS item_no,
            rates.start_datetime,
            NULL AS end_datetime,
            1 AS is_active,
            rates.yield_rate_perc
          FROM changed_rates
          JOIN stg_hiport.l13_coupon_rates rates
            ON rates.start_datetime = changed_rates.start_datetime
            AND rates.item_ref_id = changed_rates.item_ref_id
          JOIN gbm.item_index fi
            ON fi.ref_id = rates.item_ref_id
            AND fi.ref_cd = c_fi_ref_cd
          JOIN gbm.fin_instrument_yield fiy
            ON fiy.item_no = fi.no
            AND changed_rates.start_datetime BETWEEN fiy.start_datetime AND NVL(fiy.end_datetime, changed_rates.start_datetime)
            AND fiy.start_datetime != rates.start_datetime);
      logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
      COMMIT;
      logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
    EXCEPTION
      WHEN OTHERS THEN
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
        ROLLBACK;
        RAISE;
    END;
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    load_new_coupon_rates;
    load_updated_coupon_rates;
    update_coupon_end_datetime;


    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;


  PROCEDURE load_exchange_rates
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load_exchange_rates';

  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    MERGE /*+ parllel(16) */ INTO gbm.currency_exchange tgt
    USING (SELECT
        src.start_datetime,
        src.base_currency_id,
        src.currency_exchange_cd,
        src.rate,
        c_source_id AS id
      FROM stg_hiport.l14_exchange_rates src) src
    ON (tgt.id = src.id
      AND tgt.start_datetime = src.start_datetime
      AND tgt.base_currency_id = src.base_currency_id
      AND tgt.currency_exchange_cd = src.currency_exchange_cd)
    WHEN NOT MATCHED THEN INSERT
    (
      tgt.id,
      tgt.start_datetime,
      tgt.base_currency_id,
      tgt.currency_exchange_cd,
      tgt.rate
    )
    VALUES
    (
      src.id,
      src.start_datetime,
      src.base_currency_id,
      src.currency_exchange_cd,
      src.rate
    )
    WHEN MATCHED THEN UPDATE SET
      tgt.rate = src.rate
    WHERE ut.are_numbers_equal(tgt.rate, src.rate) != dt.c_true;

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  PROCEDURE load
  IS
    v_rt_log_no logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'load';
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_rt_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    load_stakeholder;
    load_item;
    load_deals;
    load_fin_instr_invested_in;
    /* Transactions will be introducied as part of the rollups - Package 3/4
		load_business_transactions;
		load_business_transaction_item;

		load_cash_business_trn;
		load_cash_business_trn_item;
		*/
    load_asset_pricing;

    load_market_risk;
    load_coupon_rates;
    load_exchange_rates;

    load_custom_item_class;
    COMMIT;
    IF (logit.etl_run_times.errors_for_etl(v_rt_log_no, c_etl_id) > 0) THEN
      RAISE_APPLICATION_ERROR(-20000, 'The ETL completed, but there were ' || logit.etl_run_times.errors_for_etl(v_rt_log_no, c_etl_id) || ' loading error(s). Please see the RUN_TIMES table for details.', TRUE);
    END IF;
    logit.etl_run_times.end_etl_procedure(v_rt_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_rt_log_no, sysdate, sqlcode, sqlerrm || ' - Stacktrace - ' || dbms_utility.format_error_backtrace);
      RAISE_APPLICATION_ERROR(-20000, 'The ETL encountered ' || logit.etl_run_times.errors_for_etl(v_rt_log_no, c_etl_id) || ' loading error(s).  Please see the RUN_TIMES table for details.', TRUE);
  END load;

  PROCEDURE rollback
  IS
    v_log_no    logit.run_times.no % TYPE;
    v_proc_name logit.run_times.procedure_name % TYPE := 'rollback';

    PROCEDURE rollback_fin_inst_invested_in
    IS
      v_log_no          NUMBER;
      v_proc_name       VARCHAR2(30) := 'rollback_fin_inst_invested_in';

      CURSOR c_data IS
        SELECT /*+ parallel(16) */
          fiii.ROWID AS r_id
        FROM stg_hiport.l09_fin_instrument_invested_in src
        JOIN gbm.item_index fi
          ON fi.ref_id = src.security_ref_id
          AND fi.ref_cd = c_fi_ref_cd
        JOIN gbm.item_index ip
          ON ip.ref_id = src.portfolio_ref_id
          AND ip.ref_cd = c_ip_ref_cd
        JOIN gbm.fin_instrument_invested_in fiii
          ON fiii.no = ip.no
          AND fiii.invested_in_no = fi.no
          AND fiii.start_datetime = src.start_datetime
        WHERE src.start_datetime = gbm.load_hiport.get_load_date();

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

        FORALL i IN 1 .. v_data.COUNT
          DELETE FROM gbm.fin_instrument_invested_in tgt
            WHERE tgt.ROWID = v_data(i).r_id;

        v_records_updated := v_records_updated + SQL % ROWCOUNT;

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

    PROCEDURE rollback_transactions
    IS
      v_log_no          NUMBER;
      v_proc_name       VARCHAR2(30) := 'rollback_transactions';

      CURSOR c_data IS
        SELECT
          1
        FROM dual;

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

        --				FORALL i IN 1 .. v_data.COUNT
        --					DELETE FROM gbm.bus_transaction tgt
        --						WHERE tgt.ROWID = v_data(i).r_id;

        v_records_updated := v_records_updated + SQL % ROWCOUNT;

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
  BEGIN
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || ' ' || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    rollback_fin_inst_invested_in;
    /* Transactions will be introducied as part of the rollups - Package 3/4
		rollback_transactions;
		*/

    logit.etl_run_times.recordcount_etl_procedure(v_log_no, SQL % ROWCOUNT);
    COMMIT;
    logit.etl_run_times.end_etl_procedure(v_log_no, systimestamp);
  EXCEPTION
    WHEN OTHERS THEN
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;
END load_hiport;
/