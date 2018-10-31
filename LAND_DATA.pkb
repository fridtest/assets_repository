CREATE OR REPLACE package body LND_ASISA.land_data AS
  /******************************************************************************
     name:       LAND_DATA
     purpose:    Automate Asisa process
     revisions:
     ver        date        author           description
     ---------  ----------  ---------------  ------------------------------------
     1.0        2017-10-05  Ntokozo Ntanzi  1. Original version ASISA ETL
     1.1        2017-12-08  Ntokozo Ntanzi     Add instance name to email subject
     1.2        2018-12-30  Kershen Naidu      Testing GIT 123 abc
  ******************************************************************************/
  m_load_date         date;
  m_fin_period        varchar2(10);
  m_fin_year_cd       gbm.fin_year_type.cd % TYPE;
  m_fin_month_cd      gbm.fin_month_type.cd % TYPE;
  m_month_format      constant VARCHAR2(2)    := 'MM';
  m_year_format       constant VARCHAR2(4)    := 'YYYY';
  m_full_date_format  constant VARCHAR2(10)   := 'YYYYMMDD';
  iFileList           frid_utlfile.TStringList;
  run_date_string     varchar2(10)            := to_char(sysdate, m_full_date_format);
  w_sender            varchar2(200)           := 'FridOps <FridOps@liberty.co.za>';
  w_etl_complete      number                  :=4;
  w_etl_stagging_paused varchar2(10)          :='6.0';
  w_env                 varchar2(20);
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  cursor c_main is
    select fmm.mapping_id,
           Decode(fmm.asisa_fund_manager_names,
                  'AllanGray',
                  'Allan Gray',
                  fmm.asisa_fund_manager_names) fund_Manager_Names,
           asisa_directory
      from lnd_asisa.fund_manager_mapping fmm
     group by fmm.mapping_id, asisa_fund_manager_names, asisa_directory
     order by fmm.mapping_id;
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  cursor c_asisa_fund_man_cur(c_mapping_id lnd_asisa.fund_manager_mapping.mapping_id%type) is
    select *
      from lnd_asisa.fund_manager_mapping fmm
     where fmm.mapping_id = c_mapping_id
     order by fmm.mapping_id;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_validation_catergory(p_validation_catergory in lnd_asisa.asisa_validation_emails.validation_catergory%type) is
    begin
      v_validation_catergory :=p_validation_catergory;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_validation_descriptions(p_validation_descriptions in lnd_asisa.asisa_validation_emails.validation_descriptions%type) is
    begin
      v_validation_descriptions :=p_validation_descriptions;
    end;
 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_iscritical(p_run_validations in lnd_asisa.asisa_validation_emails.iscritical%type) is
    begin
     v_run_validations :=p_run_validations;
    end;
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_run_validations(p_run_validations in lnd_asisa.asisa_validation_emails.run_validations%type) is
    begin
      v_run_validations := p_run_validations;
    end;
 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_send_notifications(p_send_notifications in lnd_asisa.asisa_validation_emails.send_notifications%type) is
    begin
      v_send_notifications :=p_send_notifications;
    end;
 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_message_category_success(p_message_category_success in lnd_asisa.asisa_validation_emails.message_category_success%type) is
    begin
      v_message_category_success := p_message_category_success;
    end;
   -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_message_body_success(p_message_body_success in lnd_asisa.asisa_validation_emails.message_body_success%type) is
    begin
      v_message_body_success := p_message_body_success;
    end;
   -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_message_category_failure(p_message_category_failure in lnd_asisa.asisa_validation_emails.message_category_failure%type) is
    begin
      v_message_category_failure :=p_message_category_failure;
    end;
   -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_message_body_failure(p_message_body_failure in lnd_asisa.asisa_validation_emails.message_body_failure%type) is
    begin
      v_message_body_failure :=p_message_body_failure;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_cc_addresses(p_cc_addresses in lnd_asisa.asisa_validation_emails.cc_addresses%type) is
    begin
      v_cc_addresses := p_cc_addresses;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_bcc_addresses(p_bcc_addresses in lnd_asisa.asisa_validation_emails.bcc_addresses%type) is
    begin
      v_bcc_addresses :=p_bcc_addresses;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_severity_desc(p_severity_desc in lnd_asisa.asisa_validation_emails.severity_desc%type) is
    begin
      v_severity_desc := p_severity_desc;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_log_exception_records(p_log_exception_records in lnd_asisa.asisa_validation_emails.log_exception_records%type) is
    begin
      v_log_exception_records := p_log_exception_records;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_display_on_reports(p_display_on_reports in lnd_asisa.asisa_validation_emails.display_on_reports%type) is
    begin
      v_display_on_reports := p_display_on_reports;
    end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_severity_no(p_severity_no lnd_asisa.asisa_validation_emails.serverity_no%type) is
    begin
      v_severity_no :=p_severity_no;
    end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function  get_severity_no return number is
    begin
      return v_severity_no;
    end;
 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_pause_continue_ind(p_pause_continue_ind lnd_asisa.asisa_validation_emails.pause_continue_ind%type) is
     begin
      v_pause_continue_ind :=p_pause_continue_ind;
    end;
 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function  get_pause_continue_ind return varchar2 is
    begin
      return v_pause_continue_ind;
    end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_continue_ind(p_continue_ind lnd_asisa.asisa_validation_emails.contunue_ind%type) is
    begin
      v_continue_ind :=p_continue_ind;
    end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_continue_ind return varchar2 is
    begin
      return v_continue_ind;
    end;
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_validation_catergory     return varchar2 is
    begin
      return v_validation_catergory;
    end;
 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_validation_descriptions  return varchar2
  is
    begin
      return v_validation_descriptions;
    end;
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_iscritical               return varchar2
    is
    begin
      return v_iscritical;
    end;
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_run_validations          return varchar2
    is
    begin
     return v_run_validations;
    end;
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_send_notifications       return varchar2
    is
    begin
     return v_send_notifications;
    end;
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_message_category_success return varchar2
    is
    begin
     return v_message_category_success;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_message_body_success     return varchar2
    is
    begin
      return get_message_body_success;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_message_category_failure return varchar2
    is
    begin
     return get_message_category_failure;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_message_body_failure     return varchar2
    is
    begin
      return get_message_body_failure;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_cc_addresses             return varchar2
    is
    begin
     return  v_cc_addresses;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_bcc_addresses            return varchar2
    is
    begin
     return v_bcc_addresses;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_severity_desc          return varchar2
    is
    begin
      return v_severity_desc;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_log_exception_records  return varchar2
    is
    begin
      return v_log_exception_records;
    end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_display_on_reports return varchar2
    is
    begin
      return v_display_on_reports;
    end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_validation_cat_no(p_cat_no number) is
    begin
     v_validation_cat :=p_cat_no;
    end;
 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_validation_sub_no(p_validation_sub_no varchar2) is
    begin
     v_validation_sub_no := p_validation_sub_no;
    end;
 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   function get_validation_sub_no return varchar2 is
     begin
       return v_validation_sub_no;
     end;
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function  get_validation_cat_no return number is
    begin
      return v_validation_cat;
    end;
 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_mapping_id(p_mapping_id in lnd_asisa.fund_manager_mapping.mapping_id%type) is
  begin
    v_mapping_id := p_mapping_id;
  end;
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_asisa_fund_manager_names(p_asisa_fund_manager_names in lnd_asisa.fund_manager_mapping.asisa_fund_manager_names%type) is
  begin
    v_asisa_fund_manager_names := p_asisa_fund_manager_names;
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_asisa_directory(p_asisa_directory in lnd_asisa.fund_manager_mapping.asisa_directory%type) is
  begin
    v_asisa_directory := p_asisa_directory;
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_file_type_no(p_file_type_no in lnd_asisa.fund_manager_mapping.file_type_no%type) is
  begin
    v_file_type_no := p_file_type_no;
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_search_string_mask(p_search_string_mask in lnd_asisa.fund_manager_mapping.search_string_mask%type) is
  begin
    v_search_string_mask := p_search_string_mask;
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_external_table_name(p_external_table_name in lnd_asisa.fund_manager_mapping.external_table_name%type) is
  begin
    v_external_table_name := p_external_table_name;
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_template_column(p_template_column in lnd_asisa.fund_manager_mapping.template_column%type) is
  begin
    v_template_column := p_template_column;
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_asisa_landing_tables(p_asisa_landing_tables in lnd_asisa.fund_manager_mapping.asisa_landing_tables%type) is
  begin
    v_asisa_landing_tables := p_asisa_landing_tables;
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_asisa_lnd_table_cols(p_asisa_lnd_table_cols in lnd_asisa.fund_manager_mapping.asisa_lnd_table_cols%type) is
  begin
    v_asisa_lnd_table_cols := p_asisa_lnd_table_cols;
  end;
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_file_name(p_file_name varchar2) is
  begin
    v_file_name := p_file_name;
  end;
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_file_name return varchar2 is
  begin
    return v_file_name;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_mapping_id return number is
  begin
    return v_mapping_id;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_asisa_fund_manager_names return varchar2 is
  begin
    return v_asisa_fund_manager_names;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_asisa_directory return varchar2 is
  begin
    return v_asisa_directory;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_file_type_no return number is
  begin
    return v_file_type_no;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_search_string_mask return varchar2 is
  begin
    return v_search_string_mask;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_external_table_name return varchar2 is
  begin
    return v_external_table_name;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_template_column return varchar2 is
  begin
    return v_template_column;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_asisa_landing_tables return varchar2 is
  begin
    return v_asisa_landing_tables;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_asisa_lnd_table_cols return varchar2 is
  begin
    return v_asisa_lnd_table_cols;
  end;
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_file_id(p_file_id number) IS
  begin
    c_file_id := p_file_id;
  end;
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_file_id return number is
  begin
    return c_file_id;
  end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_valuation_date(p_evaludation_date lnd_asisa.land_instruments.valuationdate%type) is
    begin
      v_valudation_date := p_evaludation_date;
    end;

---------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_assetmanagercode(p_assetmanagercode lnd_asisa.asisa_exception_report.assetmanagercode%type) is
    begin
      v_assetmanagercode := p_assetmanagercode;
    end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_valuation_date return date is
    begin
      return v_valudation_date;
    end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_source_id(p_source_id lnd_asisa.asisa_exception_report.source_id%type) is
    begin
      v_source_id :=p_source_id;
    end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------
  function  get_source_id return varchar2
    is
    begin
      return v_source_id;
    end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_assetmanagercode return varchar2 is
    begin
      return v_assetmanagercode;
    end;
 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function Load_Template_file_attributes return number is
    F      utl_file.file_type;
    v_line varchar2(32767);
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.Load_Template_file_attributes';
    v_log_no      number;
  begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    f := frid_utlfile.fopen(lnd_asisa.land_data.v_asisa_directory,lnd_asisa.land_data.v_file_name,'r',32767);

      if utl_file.is_open(f) then
         utl_file.get_line(f,v_line,32767);
      end if;

    delete from lnd_asisa.asisa_file_template_attributes;
    insert into lnd_asisa.asisa_file_template_attributes
      select rownum as pos,
             Decode(regexp_substr(Lower(v_line), '[^,]+', 1, level),
                    'appreciationdepreciationfutures','appreciationdepreciationfuture',
                    'instrumentshorttermratingagency','instrumentshorttermratingagenc',
                    'issuershorttermratingagency'    ,'issuershorttermratingagenc',
                    'assetmarketpricecum'            ,'assetmarketprice',
                    regexp_substr(Lower(v_line), '[^,]+', 1, level)) col_description
        from dual
      connect by regexp_substr(Lower(v_line), '[^,]+', 1, level) is not null;

    utl_file.fclose(f);
    return sql%rowcount;

  exception
    when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
     return 0;
  end;
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure load_exception_data(p_validation_type_no      in lnd_asisa.asisa_exception_report.validation_type_no%type,
                                p_validation_type_desc    in lnd_asisa.asisa_exception_report.validation_type_desc%type,
                                p_validation_no           in lnd_asisa.asisa_exception_report.validation_no%type,
                                p_validation_sub_no       in lnd_asisa.asisa_exception_report.validation_sub_no%type,
                                p_validation_short_desc   in lnd_asisa.asisa_exception_report.validation_short_desc%type,
                                p_exception_severity_no   in lnd_asisa.asisa_exception_report.exception_severity_no%type,
                                p_exception_severity_desc in lnd_asisa.asisa_exception_report.exception_severity_desc%type,
                                p_exception_description   in lnd_asisa.asisa_exception_report.exception_description%type,
                                p_result_action           in lnd_asisa.asisa_exception_report.result_action%type,
                                p_record_count            in number default null) IS

   v_proc_name   logit.run_times.procedure_name%type := 'land_all.load_exception_data';
   v_log_no      number;

  begin
   logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

   if lnd_asisa.land_data.v_mapping_id is not null and
       lnd_asisa.land_data.get_load_date is not null and
       p_validation_type_no is not null and
       lnd_asisa.land_data.v_file_type_no is not null and
       lnd_asisa.land_data.v_file_name is not null and
       p_validation_sub_no is not null then

      delete
        from lnd_asisa.asisa_exception_report aer
       where aer.mapping_id         = lnd_asisa.land_data.v_mapping_id
         and aer.validation_type_no = p_validation_type_no
         and aer.file_type_no       = lnd_asisa.land_data.v_file_type_no
         and aer.file_name          = lnd_asisa.land_data.v_file_name
         and aer.validation_sub_no  = p_validation_sub_no;

      insert into lnd_asisa.asisa_exception_report
        (mapping_id,
         source_id,
         assetmanagercode,
         validation_date,
         run_period,
         validation_type_no,
         file_type_no,
         file_name,
         validation_type_desc,
         fund_manager_name,
         file_type_desc,
         validation_no,
         validation_sub_no,
         validation_short_desc,
         exception_severity_no,
         exception_severity_desc,
         exception_description,
         result_action,
         record_count,
         evaluation_date)
      values
        (lnd_asisa.land_data.v_mapping_id,
         lnd_asisa.land_data.get_source_id,
         lnd_asisa.land_data.get_assetmanagercode,
         lnd_asisa.land_data.get_load_date,
         lnd_asisa.land_data.get_load_date,
         p_validation_type_no,
         lnd_asisa.land_data.v_file_type_no,
         lnd_asisa.land_data.v_file_name,
         p_validation_type_desc,
         lnd_asisa.land_data.v_asisa_fund_manager_names,
         lnd_asisa.land_data.v_search_string_mask,
         p_validation_no,
         p_validation_sub_no,
         p_validation_short_desc,
         p_exception_severity_no,
         p_exception_severity_desc,
         p_exception_description,
         p_result_action,
         p_record_count,
         nvl(lnd_asisa.land_data.get_valuation_date,lnd_asisa.land_data.get_load_date));
    
    end if;
  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure delete_exception_data is
  begin
    delete from lnd_asisa.asisa_exception_report r
    where mapping_id = lnd_asisa.land_data.v_mapping_id
       and file_name  = lnd_asisa.land_data.v_file_name
       and r.validation_no <> 4; --warning error
  exception
    when others then
     null;
  end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function file_headers_count(p_file_cols_count in number) return number IS
  w_template_columns_ctn number := 0;
  w_missing_column_list  clob;
    cursor missing_file_columns is
      select column_id, lower(column_name) column_name
        from dba_tab_columns dtc
       where dtc.table_name =substr(lnd_asisa.land_data.v_external_table_name, 11)
         and dtc.owner = trim(substr(lnd_asisa.land_data.v_external_table_name,0,instr(lnd_asisa.land_data.v_external_table_name,'.') - 1))
         and lower(trim(column_name)) not in (select lower(trim(regexp_replace(lower(tfa.column_name),'[^0-9A-Za-z]','')))
                                                from lnd_asisa.asisa_file_template_attributes tfa)
       order by column_id;

    v_proc_name   logit.run_times.procedure_name%type := 'land_all.file_headers_count';
    v_log_no      number;

  begin

    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    select Count(*)
      into w_template_columns_ctn
      from dba_tab_columns
     where table_name =substr(lnd_asisa.land_data.v_external_table_name, 11)
       and owner =trim(substr(lnd_asisa.land_data.v_external_table_name,0,instr(lnd_asisa.land_data.v_external_table_name, '.') - 1))
     order by column_id;
    if nvl(w_template_columns_ctn, -99) = Nvl(p_file_cols_count, 0) then
      return 0;
    else

    lnd_asisa.land_data.set_validation_sub_no('1.1');
    initialize_exception_params;

    w_missing_column_list := lnd_asisa.land_data.get_validation_descriptions;

    for rec in missing_file_columns LOOP
      w_missing_column_list := w_missing_column_list || ' "'||rec.column_name || '" in positon  '||(rec.column_id)||' > ';
    end loop;

    load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                        p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                        p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                        p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                        p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                        p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                        p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                        p_exception_description   => w_missing_column_list,
                        p_result_action           => lnd_asisa.land_data.get_pause_continue_ind);
     return 1;
     end if;

  exception
    when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
     return 1;
  end;
---------------------------------------------------------------------------------------------------------------------------------------------------------
 function val_col_names_and_positions return number is
    cursor c_file_dba_columns IS
      select fta.col_pos file_column_position,
             trim(lower(fta.column_name)) incorrect_file_column_name,
             lower(dtc.column_name) correct_file_column_name,
             instr(lower(dtc.column_name),Trim(regexp_replace(lower(fta.column_name),'[^0-9A-Za-z]',''))) r1,
             instr(trim(regexp_replace(lower(fta.column_name),'[^0-9A-Za-z]','')),lower(dtc.column_name)) r2
        from dba_tab_columns                          dtc,
             lnd_asisa.asisa_file_template_attributes fta
       where dtc.table_name =substr(lnd_asisa.land_data.v_external_table_name, 11)
         and dtc.owner = trim(substr(lnd_asisa.land_data.v_external_table_name,0,instr(lnd_asisa.land_data.v_external_table_name,'.') - 1))
         and fta.col_pos = dtc.column_id
         and lower(Trim(fta.column_name)) not like '%portfolioassetcurrency%'
         and Trim(lower(dtc.column_name)) <> Trim(regexp_replace(lower(fta.column_name), '[^0-9A-Za-z]', ''))
       order by dtc.column_id asc;

    w_header_count_fail    number := 0;
    w_col                  number := 0;
    w_pos_fail             number := 0;
    w_string_pos_fail      CLOB;
    w_string_header_fail   CLOB;
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.val_col_names_and_positions';
    v_log_no    number;

  begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    w_string_pos_fail    := '';
    w_string_header_fail := '';
    for rec in c_file_dba_columns loop
      select Count(*)
        into w_col
        from dba_tab_columns dtc
       where dtc.table_name =substr(lnd_asisa.land_data.v_external_table_name, 11)
         and dtc.owner = trim(substr(lnd_asisa.land_data.v_external_table_name,0,instr(lnd_asisa.land_data.v_external_table_name,'.') - 1))
         and lower(trim(dtc.column_name)) not like '%portfolioassetcurrency%'
         and Trim(lower(dtc.column_name)) =Trim(regexp_replace(lower(rec.incorrect_file_column_name),'[^0-9A-Za-z]',''));

      if w_col > 0 then

        w_pos_fail          := w_pos_fail + 1;
        w_string_pos_fail   := w_string_pos_fail||' Incorrect column header in position  '||(rec.file_column_position)||' Column header name : "'||rec.incorrect_file_column_name ||'" the Correct Column  header name should be  "'||rec.correct_file_column_name||'" > ';
      else
        w_header_count_fail  := w_header_count_fail + 1;
        w_string_header_fail := w_string_header_fail||'"'||rec.incorrect_file_column_name ||'"'||' to be corrected to ' ||'"'||rec.correct_file_column_name ||'" >  ';
      end if;
      w_col := 0;
    end loop;

    if nvl(w_header_count_fail, 0) > 0 then

        lnd_asisa.land_data.set_validation_sub_no('1.3');
        initialize_exception_params;

       if nvl(lnd_asisa.land_data.get_run_validations,'N') ='Y' then

       load_exception_data(p_validation_type_no     => lnd_asisa.land_data.get_validation_cat_no,
                           p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                           p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                           p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                           p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                           p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                           p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                           p_exception_description   => w_string_header_fail,
                           p_result_action           => lnd_asisa.land_data.get_pause_continue_ind);
       else

       load_exception_data(p_validation_type_no     => lnd_asisa.land_data.get_validation_cat_no,
                          p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                          p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                          p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions||' >> VALIDAITION IS TURNED OFF',
                          p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                          p_exception_description   => w_string_header_fail,
                          p_result_action           => lnd_asisa.land_data.get_continue_ind);

        w_header_count_fail :=0;

       end if;

    end if;

    if Nvl(w_pos_fail, 0) > 0 then

      lnd_asisa.land_data.set_validation_sub_no('1.2');
      initialize_exception_params;

      if nvl(lnd_asisa.land_data.get_run_validations,'N') ='Y' then
      load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                          p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                          p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                          p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                          p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                          p_exception_description   => w_string_pos_fail,
                          p_result_action           => lnd_asisa.land_data.get_pause_continue_ind);
      else
      load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                          p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                          p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                          p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions||' >>  VALIDAITION IS TURNED OFF',
                          p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                          p_exception_description   => w_string_pos_fail,
                          p_result_action           => lnd_asisa.land_data.get_continue_ind);
       w_pos_fail :=0;

      end if;

    end if;

    if w_pos_fail + w_header_count_fail = 0 then
      return 0;
    end if;

    return 1;
  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
  --------------------------------------------------------------------------------------------------------------------------------------------------------
  Procedure validate_portfolio_columns is
    cursor c_file_dba_columns IS
      select file_col_pos  as file_column_position,
             file_col_name as incorrect_file_column_name,
             dba_col_name  as correct_file_column_name
        from (select fta.col_pos file_col_pos,
                     trim(lower(fta.column_name)) file_col_name,
                     dtc.column_id dba_col_pos,
                     lower(dtc.column_name) dba_col_name,
                     instr(lower(dtc.column_name),Trim(regexp_replace(lower(fta.column_name),'[^0-9A-Za-z]',''))) r1,instr(trim(regexp_replace(lower(fta.column_name),'[^0-9A-Za-z]','')), lower(dtc.column_name)) r2
                from dba_tab_columns                          dtc,
                     lnd_asisa.asisa_file_template_attributes fta
               where dtc.table_name =
                     substr(lnd_asisa.land_data.v_external_table_name, 11)
                 and dtc.owner = trim(substr(lnd_asisa.land_data.v_external_table_name,0,instr(lnd_asisa.land_data.v_external_table_name,'.') - 1))
                 and fta.col_pos = dtc.column_id
                 and lower(trim(fta.column_name)) like '%portfolioassetcurrency%'
               order by dtc.column_id asc) rec
       where (rec.r1 = 0 and rec.r2 = 0)
         and rownum = 1;

    w_exception_description   clob;
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.validate_portfolio_columns';
    v_log_no    NUMBER;

  begin

    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    for rec in c_file_dba_columns loop
      w_exception_description := 'Column ' ||rec.incorrect_file_column_name ||' renamed to "' ||rec.correct_file_column_name || '"';

      lnd_asisa.land_data.set_validation_sub_no('1.4');
      initialize_exception_params;
      if nvl(lnd_asisa.land_data.get_log_exception_records,'N') ='Y' then
       load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                           p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                           p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                           p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                           p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                           p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                           p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                           p_exception_description   => w_exception_description,
                           p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                           p_record_count            => 1);
      end if;
    end loop;

  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure report_non_mandatory_data is
    w_column               varchar2(200);
    w_sql                  clob;
    w_missing_fields       clob;
    w_missing_ctn          number(10);
    w_count_sql            varchar2(200);
    w_records              number(10) :=0;
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.report_non_mondatory_data';
    v_log_no    NUMBER;
  begin

    lnd_asisa.land_data.set_validation_sub_no('2.1');
    initialize_exception_params;

   logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    w_missing_fields := '';

    for rec in (select Lower(dtc.column_id) dba_pos,
                       Lower(trim(regexp_replace(dtc.column_name,'[^0-9A-Za-z]',''))) dba_column,
                       (fta.col_pos + 3) file_pos,
                       fta.column_name,
                       dtc.data_type,
                       Length(Lower(trim(regexp_replace(dtc.column_name,'[^0-9A-Za-z]','')))) column_lenght,
                       rec.max_length
                  from dba_tab_columns dtc,
                       lnd_asisa.asisa_file_template_attributes fta,
                       (select max(string_length) max_length
                          from (select length(Lower(trim(regexp_replace(dtc.column_name,'[^0-9A-Za-z]','')))) string_length
                                  from dba_tab_columns                          dtc,
                                       lnd_asisa.asisa_file_template_attributes fta
                                 where dtc.table_name =substr(lnd_asisa.land_data.v_asisa_landing_tables,11)
                                   and dtc.owner =trim(substr(lnd_asisa.land_data.v_asisa_landing_tables,0,instr(lnd_asisa.land_data.v_asisa_landing_tables,'.') - 1))
                                   and lower(trim(dtc.column_name)) =lower(trim(regexp_replace(fta.column_name,'[^0-9A-Za-z]',''))))) rec
                 where dtc.table_name =substr(lnd_asisa.land_data.v_asisa_landing_tables, 11)
                   and dtc.owner = trim(substr(lnd_asisa.land_data.v_asisa_landing_tables,0,instr(lnd_asisa.land_data.v_asisa_landing_tables,'.') - 1))
                   and lower(trim(dtc.column_name)) =lower(trim(regexp_replace(fta.column_name,'[^0-9A-Za-z]','')))
                 order by dtc.column_id) loop

      w_column    := rec.dba_column;
      w_sql       := 'select ' || w_column || ' from ' || lnd_asisa.land_data.v_external_table_name;
      w_count_sql := 'select count(1) from ' || lnd_asisa.land_data.v_external_table_name || ' where ' || w_column || ' is null';

      execute immediate w_count_sql
        into w_missing_ctn;

      if nvl(w_missing_ctn,0) >0 then
         w_missing_fields := w_missing_fields || ' ' || rpad(w_column,rec.column_lenght + abs(rec.column_lenght - rec.max_length),' ') ||'- Number of rows not filled in :' ||w_missing_ctn || '  ';
         w_records := w_records +1;
      end if;
    end loop;

    if nvl(w_records,0) > 0 then

     if nvl(lnd_asisa.land_data.get_log_exception_records,'N') ='Y' then
      load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                          p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                          p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                          p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                          p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                          p_exception_description   => w_missing_fields,
                          p_result_action           => lnd_asisa.land_data.get_pause_continue_ind);
     end if;
    end if;

  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function validate_invalid_dates return number is
    w_column               varchar2(200);
    w_sql                  clob;
    cur                    sys_refcursor;
    w_value                varchar2(200);
    w_incorrect_format     clob;
    w_ctn                  number := 0;
    w_dates                date;
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.validate_invalid_dates';
    v_log_no                number;
  begin

    lnd_asisa.land_data.set_validation_sub_no('2.3');
    initialize_exception_params;

    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    w_incorrect_format := '';

    for rec IN (select Lower(dtc.column_id) dba_pos,
                       lower(dtc.column_name) dba_column,
                       (fta.col_pos) file_pos,
                       fta.column_name,
                       dtc.data_type
                  from dba_tab_columns                          dtc,
                       lnd_asisa.asisa_file_template_attributes fta
                 where dtc.table_name =substr(lnd_asisa.land_data.v_asisa_landing_tables, 11)
                   and dtc.owner = trim(substr(lnd_asisa.land_data.v_asisa_landing_tables,0,instr(lnd_asisa.land_data.v_asisa_landing_tables,'.') - 1))
                   and lower(trim(dtc.column_name)) =lower(trim(regexp_replace(fta.column_name,'[^0-9A-Za-z]','')))
                   and dtc.data_type like '%DATE%'
                 order by dtc.column_id)
      loop
      w_column := rec.column_name;
      w_sql    := 'select ' || w_column || ' from ' ||lnd_asisa.land_data.v_external_table_name;
      open cur for w_sql;
      loop
        fetch cur
          into w_value;
        exit when cur%notfound;
        begin
          w_dates := to_date(w_value,'YYYY/MM/DD');
        exception
          when others then
            w_ctn              := w_ctn + 1;
            w_incorrect_format := w_incorrect_format || '' || w_ctn || '. ' ||w_column ||' ';
        end;
      end loop;
    end loop;

    if nvl(w_ctn, 0) > 0 then

      load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                          p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                          p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                          p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                          p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                          p_exception_description   => w_incorrect_format,
                          p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                          p_record_count            => w_ctn);
      return 1;
    else
      return 0;
    end if;
  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
      return 1;
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function validate_invalid_numbers return number is
    w_column               varchar2(200);
    w_sql                  clob;
    cur                    sys_refcursor;
    w_value                varchar2(200);
    w_number               number;
    w_incorrect_format     clob;
    w_ctn                  number := 0;
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.validate_invalid_numbers';
    v_log_no    NUMBER;

  begin
    lnd_asisa.land_data.set_validation_sub_no('2.2');
    initialize_exception_params;

    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    w_incorrect_format := '';

    for rec IN (select Lower(dtc.column_id) dba_pos,
                       lower(dtc.column_name) dba_column,
                       (fta.col_pos) file_pos,
                       fta.column_name,
                       dtc.data_type
                  from dba_tab_columns dtc,
                       lnd_asisa.asisa_file_template_attributes fta
                 where dtc.table_name =substr(lnd_asisa.land_data.v_asisa_landing_tables, 11)
                   and dtc.owner = trim(substr(lnd_asisa.land_data.v_asisa_landing_tables,0,instr(lnd_asisa.land_data.v_asisa_landing_tables,'.') - 1))
                   and lower(trim(dtc.column_name)) =lower(trim(regexp_replace(fta.column_name,'[^0-9A-Za-z]','')))
                   and dtc.data_type like '%NUMBER%'
                 order by dtc.column_id) loop

      w_column := rec.column_name;
      w_sql    := 'select ' || w_column || ' from ' ||lnd_asisa.land_data.v_external_table_name;
      open cur for w_sql;
      loop
        fetch cur
          into w_value;
        exit when cur%notfound;
        begin
          w_number := to_number(nvl(w_value, 0));
        exception
          when value_error then
            w_ctn              := w_ctn + 1;
            w_incorrect_format := w_incorrect_format || '' || w_ctn || '. ' ||w_column ||' ';
        end;
      end loop;
    end loop;

    if nvl(w_ctn, 0) > 0 then

      if nvl(lnd_asisa.land_data.get_run_validations,'N') ='Y' then
      load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                          p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                          p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                          p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                          p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                          p_exception_description   => w_incorrect_format,
                          p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                          p_record_count            => w_ctn);

      return 1;
      else
            load_exception_data(p_validation_type_no => lnd_asisa.land_data.get_validation_cat_no,
                          p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                          p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                          p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions||' : '||' >> VALIDAITION IS TURNED OFF',
                          p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                          p_exception_description   => w_incorrect_format,
                          p_result_action           => lnd_asisa.land_data.get_continue_ind,
                          p_record_count            => w_ctn);
        return 0;
      end if;

    else
      return 0;
    end if;

  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
      return 1;
  end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure Load_asisa_file_summary is
  w_evaluation_date       date;
  w_sql                   clob;
  w_table                 varchar2(200) := lnd_asisa.land_data.get_asisa_landing_tables;
  f_shortfilename         varchar2(200);
  w_long_filename         VARCHAR2(200) := lnd_asisa.land_data.get_file_name;
  w_rowcount              number(10) :=0;
  w_rowcount_sql          clob;
  w_total_market_value    number :=0;
  w_run_intervals         number(10);
  w_previous_run_date     date;
  w_next_run_Date         date;
  prev_records_ctn        varchar2(200);
  v_proc_name   logit.run_times.procedure_name%type := 'land_all.load_asisa_file_summary';
  v_log_no    NUMBER;
begin

  logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
  w_sql := 'select max(valuationdate)
          from ' || w_table ||' where fileid =lnd_asisa.land_data.get_file_id';

  execute immediate w_sql into w_evaluation_date;

  select regexp_replace(substr(w_long_filename,0,instr(w_long_filename,substr(regexp_replace(w_long_filename,'[^0-9]',''),0,1) ) -1),'[^0-9A-Za-z]','')
  into f_shortfilename from dual;

  delete
    from lnd_asisa.file_asisa_upload_summary s
   where mapping_id    = lnd_asisa.land_data.v_mapping_id
     and to_date(s.evaluation_date,'DD.MM.YYYY') = to_date(w_evaluation_date,'DD.MM.YYYY')
     and file_name     = lnd_asisa.land_data.get_file_name;

  w_rowcount_sql := 'select count(*) from '||lnd_asisa.land_data.get_asisa_landing_tables||' where fileid ='||lnd_asisa.land_data.get_file_id;
  execute immediate w_rowcount_sql into w_rowcount;

  if lnd_asisa.land_data.get_file_type_no =2 then

     select sum(rec.totalmarketvalue)
       into w_total_market_value
       from (select distinct portfolioidcode,totalmarketvalue
       from lnd_asisa.land_holdings ld
       where ld.fileid =lnd_asisa.land_data.get_file_id) rec;

  end if;

  select fmm.run_interval
    into w_run_intervals
    from lnd_asisa.fund_manager_mapping fmm
   where fmm.mapping_id   = lnd_asisa.land_data.get_mapping_id
     and fmm.file_type_no = lnd_asisa.land_data.get_file_type_no;

   w_previous_run_date := add_months(w_evaluation_date,w_run_intervals*-1);
   w_next_run_Date     := add_months(w_evaluation_date,w_run_intervals);

  select count(*)
    into prev_records_ctn
    from lnd_asisa.file_asisa_upload_summary s
   where mapping_id        = lnd_asisa.land_data.v_mapping_id
     and to_date(s.evaluation_date,'DD.MM.YYYY') = to_date(w_previous_run_date,'DD.MM.YYYY')
     and s.short_fname     = f_shortfilename
     and s.file_type       = lnd_asisa.land_data.get_file_type_no;

  if nvl(prev_records_ctn,0) =0 then
  for rec in (select max(evaluation_date) prev_evaluation_date
     from lnd_asisa.file_asisa_upload_summary us
    where us.file_id  <>  lnd_asisa.land_data.get_file_id
      and us.short_fname = f_shortfilename
      and us.file_type   = lnd_asisa.land_data.get_file_type_no)
   loop
    w_previous_run_date := rec.prev_evaluation_date;
   end loop;
  end if;

  insert into lnd_asisa.file_asisa_upload_summary
    (seq_no,
     mapping_id,
     evaluation_date,
     file_id,
     file_type,
     file_name,
     short_fname,
     records_counts,
     totalmarket_value,
     current_run_date,
     next_run_date,
     previous_run_date,
     run_date,
     current_no_of_rows,
     prev_no_of_rows,
     row_change_percentage,
     current_fund_value,
     prev_fund_value,
     fund_value_change_perc
     )
  values
    (lnd_asisa.file_upload_summary_seq.nextval,
     lnd_asisa.land_data.v_mapping_id,
     w_evaluation_date,
     lnd_asisa.land_data.get_file_id,
     lnd_asisa.land_data.get_file_type_no,
     lnd_asisa.land_data.get_file_name,
     f_shortfilename,
     w_rowcount,
     w_total_market_value,
     w_evaluation_date,
     w_next_run_Date,
     w_previous_run_date,
     sysdate,
     w_rowcount,
     0,
     0,
     w_total_market_value,
     0,
     0);

 exception
   when others then
    logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function load_landing_data return number is
    v_insert          varchar2(32767);
    w_file_id         number;
    v_delete          varchar2(3000);
    type ref_cur is ref cursor;
    cur_var           ref_cur;
    dsql              varchar2(2000);
    w_valudation_date date;
    w_ext_valuation   varchar2(2000);
    w_invalid_number  number := null;
    w_invalid_dates   number := null;
    v_proc_name       logit.run_times.procedure_name%type := 'land_all.load_landing_data';
    v_log_no          number;
  begin

    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    w_invalid_dates  := validate_invalid_dates;
    w_invalid_number := validate_invalid_numbers;
    report_non_mandatory_data;

    if nvl(w_invalid_number, -99) = 0 and nvl(w_invalid_dates, -99) = 0 then

      dsql := 'select distinct valuationdate from ' ||lnd_asisa.land_data.v_external_table_name ||' where valuationdate is not null';
      open cur_var for dsql;
      loop
        fetch cur_var
          into w_ext_valuation;
        exit when cur_var%notfound;
        w_valudation_date := trunc(to_date(w_ext_valuation, 'YYYY/MM/DD'));

        v_delete := 'delete from ' ||lnd_asisa.land_data.v_asisa_landing_tables ||' where trunc(to_date(valuationdate,''YYYY/MM/DD'')) =trunc(to_date(''' ||
                             w_valudation_date || ''',''YYYY/MM/DD''))  and filename =''' || lnd_asisa.land_data.v_file_name || '''';

        execute immediate v_delete;
      end loop;

      w_file_id := lnd_asisa.file_seq_no.nextval;
      set_file_id(w_file_id);

      v_insert := 'insert  into ' ||lnd_asisa.land_data.v_asisa_landing_tables || '  select ' ||w_file_id || ',''' || lnd_asisa.land_data.v_file_name ||''',''' || sysdate || ''',' ||
                  lnd_asisa.land_data.v_asisa_lnd_table_cols || ' from  ' ||lnd_asisa.land_data.v_external_table_name;
      execute immediate v_insert;
    else
      set_file_id(-99);
    end if;
    return w_file_id;

  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
  ---------------------------------------------------------------------------------------------------------------------------------------------------
  function start_validations return number is
    w_layout_check           number := 0;
    w_header_check_names     number := 0;
    w_file_cols              number;
    v_proc_name              logit.run_times.procedure_name%type := 'land_all.start_validations';
    v_log_no                 number;
  begin

    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    w_file_cols          := load_template_file_attributes;
    validate_portfolio_columns;
    w_layout_check       := file_headers_count(p_file_cols_count => w_file_cols);
    w_header_check_names := val_col_names_and_positions;

    if (w_layout_check + w_header_check_names = 0) then
        delete_exception_data;
        return 0;
    else
      return 1;
    end if;

    exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
      return 1;
  end;
-------------------------------------------------------------------------------------------------------------------------------------------------
  procedure populate_file_valuation_date is
    w_sql            varchar2(200);
    w_char_date      varchar2(200);
    w_valuation_date date;
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.populate_file_valuation_date';
    v_log_no          NUMBER;
    begin
     logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
     w_sql := 'select max(valuationdate) from '||lnd_asisa.land_data.v_external_table_name;
     execute immediate w_sql into w_char_date;
     w_valuation_date :=to_date(w_char_date,'YYYY/MM/DD');
     set_valuation_date(w_valuation_date);

     exception
       when others then
         logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
    end;
------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure populate_assetmanagercode is
    w_sql                 varchar2(200);
    w_assetmanagercode    varchar2(200);
    w_source_id           varchar2(40);
    source_id_ctn         number;
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.populate_assetmanagercode';
    v_log_no          number;
    begin

     logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
     w_sql := 'select assetmanagercode from '||lnd_asisa.land_data.v_external_table_name||' where rownum =1';

     execute immediate w_sql into w_assetmanagercode;

      insert into lnd_asisa.processed_files(mapping_id,file_name,evaluation_date,load_date)
      values (lnd_asisa.land_data.get_mapping_id,lnd_asisa.land_data.get_file_name,lnd_asisa.land_data.get_valuation_date,sysdate);

      if w_assetmanagercode is not null then
        lnd_asisa.land_data.set_assetmanagercode(w_assetmanagercode);

        select count(*)
          into source_id_ctn
          from stg_asisa.map_asisa_source
         where asset_manager_code =lnd_asisa.land_data.get_assetmanagercode
         and rownum =1;

         if nvl(source_id_ctn,0) =1 then
           select source_id
             into w_source_id
            from stg_asisa.map_asisa_source
           where asset_manager_code =lnd_asisa.land_data.get_assetmanagercode;
           lnd_asisa.land_data.set_source_id(w_source_id);
           else
             lnd_asisa.land_data.set_source_id(null);
         end if;
         else
           lnd_asisa.land_data.set_assetmanagercode(null);
      end if;

     exception
       when others then
         logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
    end;
  --------------------------------------------------------------------------------------------------------------------------------------------------------
 function create_dynamic_ext_tables return number AS
    w_sql             clob;
    w_ext_asisa_count number(10) := 0;
    rows_inserted     number(10) := 0;
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.create_dynamic_ext_tables';
    v_log_no          number;
  begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    
    set_valuation_date(null);
    set_source_id(null);
    set_assetmanagercode(null);
    
    select count(*)
      into w_ext_asisa_count
      from dba_objects o
     where o.object_type = 'TABLE'
       and o.object_name =
           substr(lnd_asisa.land_data.v_external_table_name, 11)
       and o.owner =
           Trim(substr(lnd_asisa.land_data.v_external_table_name,0,instr(lnd_asisa.land_data.v_external_table_name, '.') - 1));
    if w_ext_asisa_count > 0 then
      execute immediate 'drop  table  ' ||
                        lnd_asisa.land_data.v_external_table_name;
    end if;

    w_sql := 'CREATE  table  ' || lnd_asisa.land_data.v_external_table_name || '
        (  ' || lnd_asisa.land_data.v_template_column || ')
      organization external
      (
        type ORACLE_LOADER
        default  directory  ASISA_DIR
        access parameters
        (
          RECORDS  DELIMITED  BY newline BADFILE ''asisa_file.err''  NODISCARDFILE  NOLOGFILE  skip 1 FIELDS  TERMINATED BY  '',''  OPTIONALLY ENCLOSED  BY ''"'' LRTRIM  REJECT ROWS WITH ALL NULL FIELDS
        )
        location (' || lnd_asisa.land_data.v_asisa_directory ||':'''|| lnd_asisa.land_data.v_file_name ||''')
      )
      reject limit 1';
    
    execute immediate w_sql;
    execute immediate 'Select  count(*) from  '||lnd_asisa.land_data.v_external_table_name
      into rows_inserted;
    
    insert into lnd_asisa.created_asisa_objects
    values(lnd_asisa.land_data.v_external_table_name);
    
    return rows_inserted;
  exception
    when others then
    log_other_exception_params('5.0',sqlerrm);
    logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, lnd_asisa.land_data.v_file_name||' : '||sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
    return 0;
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure set_load_date(p_load_date DATE) AS
  begin
    m_load_date    := p_load_date;
    m_fin_period   := to_char(p_load_date, m_full_date_format);
    m_fin_month_cd := to_char(p_load_date, m_month_format);
    m_fin_year_cd  := to_char(p_load_date, m_year_format);
  end;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function get_load_date return date as
  begin
    return m_load_date;
  end;
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function data_detailed_error(p_drill_qry custom.process_validations.drillthrough_qry%type)
    return clob is
    cur_var          sys_refcursor;
    w_file           varchar2(2000);
    w_fileid         varchar2(200);
    w_error_details  clob;
    v_proc_name      logit.run_times.procedure_name%type := 'land_all.data_detailed_error';
    v_log_no         number;
  begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    open cur_var for p_drill_qry;
    loop
      fetch cur_var
        into w_fileid,w_file;
      exit when cur_var%notfound;
      w_error_details := w_error_details || ' ' || w_fileid || ' : ' ||w_file||',';
    end loop;
    return w_error_details;
    exception
     when others  then
       logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function data_validation return number is
    w_exception_sev_no   number;
    w_drill_gry          clob;
    w_detailed_error     clob := null;
    w_error_counts       number :=0;
    w_enabled            number :=0;
    v_proc_name          logit.run_times.procedure_name%type := 'land_all.data_detailed_error';
    v_log_no             number;
  begin
      logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
      custom.process_validator.validate_process(c_etl_id,sysdate,null,false,lnd_asisa.land_data.c_file_id);
        for rec in (select version,
                           record_count,
                           error_code validation_no,
                           error_message
                      from logit.run_times rt
                     where version = lnd_asisa.land_data.c_file_id
                       and rt.record_count > 0
                       and rt.error_code is not null
                       and exists(select 1 from custom.process_validations where process_id = c_etl_id and no=error_code))
         loop

          select pv.is_critical,
                 pv.drillthrough_qry,
                 enabled
            into w_exception_sev_no,
                 w_drill_gry,
                 w_enabled
            from custom.process_validations pv
           where process_id = c_etl_id
             and no         = rec.validation_no;

          w_detailed_error := rec.error_message;

          if rec.validation_no in (356) then
            w_detailed_error := data_detailed_error(w_drill_gry);
            w_detailed_error := rec.error_message || ' ' ||w_detailed_error;
          end if;

          lnd_asisa.land_data.set_validation_sub_no(to_char(rec.validation_no));
          initialize_exception_params;

         if nvl(lnd_asisa.land_data.get_run_validations,'N') ='Y' then

          load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                              p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                              p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                              p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                              p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                              p_exception_description   => w_detailed_error,
                              p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                              p_record_count            => rec.record_count);

          if nvl(w_exception_sev_no,0) =1 and nvl(w_enabled,0) =1 then
            w_error_counts :=w_error_counts +1;
            lnd_asisa.land_data.remove_landing_failed_data;
            exit;
          end if;
        else

          load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                              p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                              p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                              p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions ||' '|| ' >> VALIDAITION IS TURNED OFF',
                              p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                              p_exception_description   => w_detailed_error,
                              p_result_action           => lnd_asisa.land_data.get_continue_ind,
                              p_record_count            => rec.record_count);
        end if;

      end loop;

    return w_error_counts;
     exception
      when others then
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
       return 1;
  end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  procedure remove_landing_failed_data is
    v_remove_sql varchar2(3000);
    v_proc_name   logit.run_times.procedure_name%type := 'land_all.remove_landing_failed_data';
    v_log_no    NUMBER;

  begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    v_remove_sql := 'Delete from ' ||lnd_asisa.land_data.v_asisa_landing_tables ||' where fileid =' || get_file_id ||' and  filename =''' || lnd_asisa.land_data.v_file_name || '''';
    execute immediate v_remove_sql;
    exception
      when others then
       logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 function calculate_rows_variance(p_current_row number, p_previous_row number) return number is
   w_lower_bound             number ;
   w_upper_bound             number;
   w_variance                number(10,2);
   w_exception_description   varchar2(2000);
   w_original_var            number(10,2);
   v_proc_name   logit.run_times.procedure_name%type := 'land_all.Calculate_rows_variance';
   v_log_no    NUMBER;

   begin

   logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    for rr in(select fmm.count_check_percentage
                from lnd_asisa.fund_manager_mapping fmm
               where fmm.mapping_id   = lnd_asisa.land_data.get_mapping_id
                 and fmm.file_type_no = lnd_asisa.land_data.get_file_type_no)
      loop
        select substr(rr.count_check_percentage,0,instr(rr.count_check_percentage,',')-1) lower_bound
         into w_lower_bound
         from dual;

       select substr(rr.count_check_percentage,instr(rr.count_check_percentage,',')+1,Length(rr.count_check_percentage)) upper_bound
         into w_upper_bound
         from dual;

        if nvl(p_previous_row,0) >0 then
          w_original_var :=(((p_current_row -p_previous_row)/p_previous_row) * 100);
          w_variance :=abs(((p_current_row -p_previous_row)/p_previous_row) * 100);
        end if;


        if nvl(w_variance,0) < nvl(w_lower_bound,0) then

         lnd_asisa.land_data.set_validation_sub_no(to_char('2.4'));
         initialize_exception_params;

         w_exception_description   :=lnd_asisa.land_data.get_validation_descriptions||' : '||w_variance||'% < '||w_lower_bound||'%';
         if nvl(lnd_asisa.land_data.get_log_exception_records,'N') ='Y' then
          load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                              p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                              p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                              p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                              p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                              p_exception_description   => w_exception_description,
                              p_result_action           => lnd_asisa.land_data.get_continue_ind,
                              p_record_count            => 1);

         update lnd_asisa.file_asisa_upload_summary su
            set su.row_change_percentage   = w_original_var
          where su.file_id = lnd_asisa.land_data.get_file_id;

         return 0;
         end if;

        elsif nvl(w_variance,0) >= nvl(w_lower_bound,0) and nvl(w_variance,0) <= nvl(w_upper_bound,0) then
         lnd_asisa.land_data.set_validation_sub_no(to_char('2.6'));
         initialize_exception_params;
         w_exception_description   :=lnd_asisa.land_data.get_validation_descriptions||' : '||w_variance||'% between '||w_lower_bound||'% and '||w_upper_bound||'%';

        if nvl(lnd_asisa.land_data.get_log_exception_records,'N') ='Y' then

        load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                            p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                            p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                            p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                            p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                            p_exception_description   => w_exception_description,
                            p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                            p_record_count            => 1);

           update lnd_asisa.file_asisa_upload_summary su
            set su.row_change_percentage   = w_original_var
         where su.file_id = lnd_asisa.land_data.get_file_id;
         return 0;

         end if;

        elsif nvl(w_variance,0) > nvl(w_upper_bound,0)  then

           lnd_asisa.land_data.set_validation_sub_no(to_char('2.4'));
           initialize_exception_params;

           w_exception_description   :=lnd_asisa.land_data.get_validation_descriptions||' : '||w_variance||' > '||w_upper_bound||'%';
          if nvl(lnd_asisa.land_data.get_run_validations,'N') ='Y' then

              load_exception_data(p_validation_type_no  => lnd_asisa.land_data.get_validation_cat_no,
                              p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                              p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                              p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                              p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                              p_exception_description   => w_exception_description,
                              p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                              p_record_count            => 1);

               update lnd_asisa.file_asisa_upload_summary su
                  set su.row_change_percentage   = w_original_var
                where su.file_id = lnd_asisa.land_data.get_file_id;

                return 1;
          else
               load_exception_data(p_validation_type_no  => lnd_asisa.land_data.get_validation_cat_no,
                              p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                              p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                              p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                              p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions||' : '||' >> VALIDAITION IS TURNED OFF',
                              p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                              p_exception_description   => w_exception_description,
                              p_result_action           => lnd_asisa.land_data.get_continue_ind,
                              p_record_count            => 1);

               update lnd_asisa.file_asisa_upload_summary su
                  set su.row_change_percentage   = w_original_var
                where su.file_id = lnd_asisa.land_data.get_file_id;
            return 0;
         end if;
        end if;
      end loop;

   return 0;
   exception
     when others then
       logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
       return 1;
   end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function calculate_marketvalue_variance(p_current_row number, p_previous_row number) return number is
   w_lower_bound             number ;
   w_upper_bound             number;
   w_variance                number(10,2);
   w_original_var            number(10,2);
   w_exception_description   varchar2(2000);
   v_proc_name   logit.run_times.procedure_name%type := 'land_all.calculate_marketvalue_variance';
   v_log_no                  number;
   begin

   logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    for rr in(select fmm.fund_val_check_percentage
                from lnd_asisa.fund_manager_mapping fmm
               where fmm.mapping_id   = lnd_asisa.land_data.get_mapping_id
                 and fmm.file_type_no = lnd_asisa.land_data.get_file_type_no)
      loop

        select substr(rr.fund_val_check_percentage,0,instr(rr.fund_val_check_percentage,',')-1) lower_bound
         into w_lower_bound
         from dual;

       select substr(rr.fund_val_check_percentage,instr(rr.fund_val_check_percentage,',')+1,Length(rr.fund_val_check_percentage)) upper_bound
         into w_upper_bound
         from dual;

       if nvl(p_previous_row,0) >0 then

        w_original_var :=(((p_current_row -p_previous_row)/p_previous_row) * 100);
        w_variance :=abs((((p_current_row -p_previous_row)/p_previous_row) * 100));
       end if;

        if nvl(w_variance,0) < nvl(w_lower_bound,0) then

         lnd_asisa.land_data.set_validation_sub_no(to_char('2.5'));
         initialize_exception_params;

         w_exception_description   :=lnd_asisa.land_data.get_validation_descriptions||' : '||w_original_var||' < '||w_lower_bound||'%';

        load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                            p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                            p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                            p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                            p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                            p_exception_description   => w_exception_description,
                            p_result_action           => lnd_asisa.land_data.get_continue_ind,
                            p_record_count            => 1);


        update lnd_asisa.file_asisa_upload_summary su
           set su.fund_value_change_perc   = w_original_var
         where su.file_id = lnd_asisa.land_data.get_file_id;

         return 0;

        elsif nvl(w_variance,0) >= nvl(w_lower_bound,0) and nvl(w_variance,0) <= nvl(w_upper_bound,0) then

         lnd_asisa.land_data.set_validation_sub_no(to_char('2.7'));
         initialize_exception_params;

         w_exception_description   :=  lnd_asisa.land_data.get_validation_descriptions||' : '||w_original_var||' between '||w_lower_bound||'% and '||w_upper_bound||'%';
        if lnd_asisa.land_data.get_log_exception_records ='Y' then
         load_exception_data(p_validation_type_no     => lnd_asisa.land_data.get_validation_cat_no,
                            p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                            p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                            p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                            p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                            p_exception_description   => w_exception_description,
                            p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                            p_record_count            => 1);

            update lnd_asisa.file_asisa_upload_summary su
               set su.fund_value_change_perc   = w_original_var
             where su.file_id = lnd_asisa.land_data.get_file_id;

         end if;
         return 0;
        elsif nvl(w_variance,0) >= nvl(w_upper_bound,0)  then

          lnd_asisa.land_data.set_validation_sub_no(to_char('2.5'));
          initialize_exception_params;

          w_exception_description   := lnd_asisa.land_data.get_validation_descriptions||' : '||w_original_var||' > '||w_upper_bound||'%';

       if nvl(lnd_asisa.land_data.get_run_validations,'N') ='Y' then
        load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                            p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                            p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                            p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                            p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                            p_exception_description   => w_exception_description,
                            p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                            p_record_count            => 1);

            update lnd_asisa.file_asisa_upload_summary su
               set su.fund_value_change_perc   = w_original_var
             where su.file_id = lnd_asisa.land_data.get_file_id;
         return 1;
         else
         load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                            p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                            p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                            p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                            p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions||' : '||' VALIDATION TURNED OFF',
                            p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                            p_exception_description   => w_exception_description,
                            p_result_action           => lnd_asisa.land_data.get_continue_ind,
                            p_record_count            => 1);

            update lnd_asisa.file_asisa_upload_summary su
               set su.fund_value_change_perc   = w_original_var
             where su.file_id = lnd_asisa.land_data.get_file_id;

             return 0;
         end if;
        end if;
      end loop;
   return 0;
   exception
     when others then
       logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
       return 1;
   end;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  function perfom_consistency_checks return number is
    w_rowcount_variance  number :=0;
    w_marketval_variance number :=0;
    v_proc_name          logit.run_times.procedure_name%type := 'land_all.perfom_consistency_checks';
    v_log_no             number;
    begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    for r1 in(select s1.previous_run_date,
             s1.short_fname,
             s1.file_name,
             s1.records_counts,
             s1.totalmarket_value
        from lnd_asisa.file_asisa_upload_summary s1
       where s1.file_id  =lnd_asisa.land_data.get_file_id)
       loop
         for r2 in (select s2.records_counts,
                           s2.totalmarket_value
                      from lnd_asisa.file_asisa_upload_summary s2
                     where s2.short_fname     = r1.short_fname
                       and s2.evaluation_date = r1.previous_run_date
                   )
         loop
           update lnd_asisa.file_asisa_upload_summary su
              set prev_no_of_rows = nvl(r2.records_counts,0)
           where su.file_id = lnd_asisa.land_data.get_file_id;

           w_rowcount_variance  :=calculate_rows_variance(r1.records_counts,r2.records_counts);

            if lnd_asisa.land_data.get_file_type_no =2 then
               w_marketval_variance := calculate_marketvalue_variance(r1.totalmarket_value,r2.totalmarket_value);

               update lnd_asisa.file_asisa_upload_summary su
                  set su.prev_fund_value =nvl(r2.totalmarket_value,0)
                where su.file_id = lnd_asisa.land_data.get_file_id;

            end if;
         end loop;
       end loop;

    if (nvl(w_rowcount_variance,0)+ nvl(w_marketval_variance,0)) >0 then

         update lnd_asisa.file_asisa_upload_summary su
           set su.result_action =lnd_asisa.land_data.get_pause_continue_ind
         where su.file_id =lnd_asisa.land_data.get_file_id;
         remove_landing_failed_data;
        return 1;
      else
       update lnd_asisa.file_asisa_upload_summary su
          set su.result_action =lnd_asisa.land_data.get_continue_ind
         where su.file_id =lnd_asisa.land_data.get_file_id;
         return 0;
    end if;
    exception
      when others then
        logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
        return 1;
    end;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function get_file_type_desc(p_id lnd_asisa.fund_manager_mapping.mapping_id%type,
                            p_type lnd_asisa.fund_manager_mapping.file_type_no%type) return varchar2 is
  w_file_type_desc  lnd_asisa.fund_manager_mapping.search_string_mask%type;
  v_proc_name       logit.run_times.procedure_name%type := 'land_all.get_file_type_desc';
  v_log_no          number;
  begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    for rec in (select fmm.search_string_mask
                  from lnd_asisa.fund_manager_mapping fmm
                 where fmm.mapping_id =p_id
                   and fmm.file_type_no =p_type)
     loop
      w_file_type_desc := rec.search_string_mask;
     end loop;
     return w_file_type_desc;
     exception
       when others then
         logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
         return null;
  end;
  
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure get_enviroment is
   v_proc_name   logit.run_times.procedure_name%type := 'land_all.get_enviroment';
   v_log_no      number;
   begin
     logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
     for rec in (select v.INSTANCE_NAME INSTANCE_NAME
                  from v$instance v 
                  where rownum =1)
      loop
       w_env := upper(rec.INSTANCE_NAME); 
      end loop; 
                
     exception when others then
       logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);  
   end;  
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure Generate_pass_notifications is
  l_clob              clob;
  l_attach_text       clob;
  l_attach_text_h     clob;
  w_recipients        lnd_asisa.asisa_email_recipients.recipients_addresses%type;
  w_subject           varchar2(2000) :='Consistency Report - No Reply';
  w_summary_found     number :=0;
  v_proc_name         logit.run_times.procedure_name%type := 'land_all.Generate_pass_notifications';

cursor c1 is
select previous_run_date,
       current_run_date,
       evaluation_date,
       file_type,
       file_name,
       prev_no_of_rows,
       current_no_of_rows,
       row_change_percentage,
       prev_fund_value,
       current_fund_value,
       fund_value_change_perc,
       result_action
  from lnd_asisa.file_asisa_upload_summary s
 where s.mapping_id      = lnd_asisa.land_data.get_mapping_id
   and s.evaluation_date = lnd_asisa.land_data.get_valuation_date
   order by s.file_type;
   
   v_log_no    number;
begin

 logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
 l_attach_text_h :='PREVIOUS STAGING DATE,CURRENT STAGING DATE,FUND MANAGER NAME,EVALUATION_DATE,FILE_TYPE,FILE_TYPE_DESC,FILE_NAME,ROWS IN PREVIOUS STAGING,ROWS IN CURRENT STAGING,ROW_CHANGE_PERCENTAGE,FUND VALUE PREVIOUS STAGING,FUND VALUE CURRENT STAGING,CHANGE PERCENTAGE,RESULT ACTION';

 for r_asisa_exception_report in c1
  loop
    l_attach_text :=
    r_asisa_exception_report.previous_run_date        ||','||
    r_asisa_exception_report.current_run_date         ||','||
    lnd_asisa.land_data.get_asisa_fund_manager_names  ||','||
    r_asisa_exception_report.evaluation_date          ||','||
    r_asisa_exception_report.file_type                ||','||
    get_file_type_desc(lnd_asisa.land_data.get_mapping_id,r_asisa_exception_report.file_type)||','||
    r_asisa_exception_report.file_name                ||','||
    r_asisa_exception_report.prev_no_of_rows          ||','||
    r_asisa_exception_report.current_no_of_rows       ||','||
    r_asisa_exception_report.row_change_percentage    ||','||
    r_asisa_exception_report.prev_fund_value          ||','||
    r_asisa_exception_report.current_fund_value       ||','||
    r_asisa_exception_report.fund_value_change_perc   ||','||
    r_asisa_exception_report.result_action            ||','||chr(13);

    l_clob := l_clob|| l_attach_text;
    w_summary_found := w_summary_found + 1;

  end loop;
  if nvl(w_summary_found,0) >0 then

   l_clob := l_attach_text_h ||chr(13) || l_clob;
   
   for x in(select distinct mapping_id,send_notifications,
                    message_body_success
               from lnd_asisa.asisa_validation_emails e
              where mapping_id=lnd_asisa.land_data.get_mapping_id
                 and rownum =1) -- you need to add new cat for sending a report
     loop
       for records in (select distinct recipients_addresses
                         from lnd_asisa.asisa_email_recipients er
                        where mapping_id = lnd_asisa.land_data.get_mapping_id
                          and er.active ='Y' 
                          and er.validation_category_no =3)
      loop
      w_recipients :=records.recipients_addresses;
      end loop;
    
     if nvl(x.send_notifications,'N') ='Y' then
     custom.send_email_att.send_mails(p_sender     => w_sender,
                                   p_recipients    => w_recipients,
                                   p_subject       => w_env ||' - '||upper(lnd_asisa.land_data.get_asisa_fund_manager_names)||' : '||w_subject,
                                   p_message       => x.message_body_success,
                                   p_attachment    => substr(l_clob,0,30000),
                                   p_att_inline    => false,
                                   p_att_filename  => lnd_asisa.land_data.get_asisa_fund_manager_names||' Consistency Report.csv');
      end if;
      w_recipients :=null;
     end loop;
 end if;

exception
 when others then
 logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure email_failed_files is
  l_clob              clob;
  l_attach_text       clob;
  l_attach_text_h     clob;
  w_message_body      clob;
  w_subject           clob;
  v_proc_name         logit.run_times.procedure_name%type := 'land_all.email_failed_files';

 cursor c1(p_mapping_id lnd_asisa.asisa_exception_report.mapping_id%type,
           p_validation_type lnd_asisa.asisa_exception_report.validation_type_no%type) is
 select r.mapping_id,
        r.fund_manager_name,
        r.validation_date,
        r.validation_sub_no,
        case
        when r.validation_sub_no ='1.5'
        then null
        else r.evaluation_date
        end evaluation_date,
        r.validation_type_no,
        r.validation_type_desc,
        r.file_type_no,
        r.file_type_desc,
        r.file_name,
        r.exception_severity_desc,
        r.validation_short_desc,
        substr(r.exception_description,0,3000) exception_description,
        r.validation_no,
        r.record_count,
        r.result_action,
        ve.display_on_reports
   from lnd_asisa.asisa_exception_report r,lnd_asisa.asisa_validation_emails ve
  where r.mapping_id          = p_mapping_id
    and nvl(ve.mapping_id,r.mapping_id) = r.mapping_id
    and ve.validation_number  = r.validation_sub_no
    and r.validation_type_no  = p_validation_type
    and ve.display_on_reports ='Y'
    order by validation_type_no,file_type_no;

    v_log_no                number;
    w_category_subject      clob;
    w_recipients            varchar2(2000);
    w_validation_catergory  varchar2(200);
    

begin
  logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

  for i in (select distinct er.mapping_id,
                   er.validation_type_no,
                   er.validation_type_desc
              from lnd_asisa.asisa_exception_report er
             where er.mapping_id = lnd_asisa.land_data.get_mapping_id)
   loop
   l_attach_text_h :='MAPPING ID,FUND MANAGER NAME,VALIDATION DATE,VALUATION_DATE,VALIDATION TYPE DESCRIPTION,FILE TYPE DESC'||','||'FILE NAME'||','||'EXCEPTION SEVERITY DESC,VALIDATION NUMBER,RECORD COUNT,RESULT ACTION,VALIDATION SHORT DESC,FULL ERROR DESCRIPTION';
 for r_asisa_exception_report in c1(i.mapping_id,i.validation_type_no)
  loop
    
    l_attach_text :=
    r_asisa_exception_report.mapping_id              ||','||
    r_asisa_exception_report.fund_manager_name       ||','||
    r_asisa_exception_report.validation_date         ||','||
    r_asisa_exception_report.evaluation_date         ||','||
    r_asisa_exception_report.validation_type_desc    ||','||
    r_asisa_exception_report.file_type_desc          ||','||
    r_asisa_exception_report.file_name               ||','||
    r_asisa_exception_report.exception_severity_desc ||','||
    r_asisa_exception_report.validation_sub_no       ||','||
    r_asisa_exception_report.record_count            ||','||
    r_asisa_exception_report.result_action           ||','||
    r_asisa_exception_report.validation_short_desc   ||','||
    substr(r_asisa_exception_report.exception_description,0,400)   ||UTL_TCP.crlf;
    l_clob := l_clob|| l_attach_text;
   end loop; 
    
   l_clob := l_attach_text_h ||UTL_TCP.crlf || l_clob;

   for x in (select distinct recipients_addresses
                  from lnd_asisa.asisa_email_recipients ve
                 where ve.mapping_id = lnd_asisa.land_data.get_mapping_id
                   and recipients_addresses is not null
                   and ve.validation_category_no in (i.validation_type_no))
     loop
     w_recipients :=x.recipients_addresses;
    end loop;
    
    select distinct validation_catergory,
           message_category_failure,
           message_body_failure
      into w_validation_catergory,
           w_category_subject,
           w_message_body
      from lnd_asisa.asisa_validation_emails ve
     where nvl(ve.mapping_id,lnd_asisa.land_data.get_mapping_id) = lnd_asisa.land_data.get_mapping_id
       and ve.cat_no     = i.validation_type_no
       and rownum        = 1;
     
     w_subject := upper(lnd_asisa.land_data.get_asisa_fund_manager_names)||' : '||w_category_subject;
     
     custom.send_email_att.send_mails(p_sender       => w_sender,
                                   p_recipients   => w_recipients,
                                   p_subject      => w_env ||' - '|| w_subject,
                                   p_message      => w_message_body,
                                   p_attachment   => substr(l_clob,0,30000),
                                   p_att_inline   => false,
                                   p_att_filename => lnd_asisa.land_data.get_asisa_fund_manager_names||' '||w_validation_catergory||' Exception Report.csv');
                    
      
      l_clob              := null;
      l_attach_text_h     := null;
      w_subject           := null;
      w_category_subject  := null;
      w_recipients        := null;
   end loop;

exception
 when others then
 logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   
 procedure notification is
   v_proc_name   logit.run_times.procedure_name%type := 'land_all.notification';
   v_log_no      number;
  begin
  logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
  get_enviroment;
  email_failed_files;
  Generate_pass_notifications;
  Successful_ETL;
  lnd_asisa.land_data.set_valuation_date(null);
  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
      null;
  end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure initialize_exception_params is
   v_proc_name   logit.run_times.procedure_name%type := 'land_all.initialize_exception_params';
   v_log_no      number;

   cursor c_parms(p_validation_sub_no lnd_asisa.asisa_validation_emails.validation_number%type) is
   select  distinct e.*
    from lnd_asisa.asisa_validation_emails e
   where e.mapping_id =lnd_asisa.land_data.get_mapping_id
     and e.validation_number =p_validation_sub_no
     and rownum =1;

   begin
     logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name||m_fin_period, m_fin_year_cd,m_fin_month_cd,systimestamp);
     for p in c_parms(lnd_asisa.land_data.get_validation_sub_no)
      loop
      lnd_asisa.land_data.set_validation_cat_no(p.cat_no);
      lnd_asisa.land_data.set_validation_sub_no(p.validation_number);
      lnd_asisa.land_data.set_validation_catergory(p.validation_catergory);
      lnd_asisa.land_data.set_validation_descriptions(p.validation_descriptions);
      lnd_asisa.land_data.set_run_validations(p.run_validations);
      lnd_asisa.land_data.set_send_notifications(p.send_notifications);
      lnd_asisa.land_data.set_severity_no(p.serverity_no);
      lnd_asisa.land_data.set_severity_desc(p.severity_desc);
      lnd_asisa.land_data.set_display_on_reports(p.display_on_reports);
      lnd_asisa.land_data.set_log_exception_records(p.log_exception_records);
      lnd_asisa.land_data.set_continue_ind(p.contunue_ind);
      lnd_asisa.land_data.set_message_category_failure(p.message_category_failure);
      lnd_asisa.land_data.set_message_body_failure(p.message_body_failure);
      lnd_asisa.land_data.set_pause_continue_ind(p.pause_continue_ind);
      end loop;
 
   exception
   when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
   end;
------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure log_other_exception_params(p_vali_sub_no in lnd_asisa.asisa_validation_emails.validation_number%type,p_detailed_error  in varchar2) is
   v_proc_name   logit.run_times.procedure_name%type := 'land_all.log_other_exception_params';
   v_log_no      number;

   cursor c_parms(p_vali_sub_no lnd_asisa.asisa_validation_emails.validation_number%type) is
   select  distinct e.*
    from lnd_asisa.asisa_validation_emails e
   where upper(e.fund_manager_name) ='ALL'
     and e.validation_number =p_vali_sub_no
     and rownum =1;

   begin
     logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name||m_fin_period, m_fin_year_cd,m_fin_month_cd,systimestamp);
     
     for p in c_parms(p_vali_sub_no)
      loop
      lnd_asisa.land_data.set_validation_cat_no(p.cat_no);
      lnd_asisa.land_data.set_validation_sub_no(p.validation_number);
      lnd_asisa.land_data.set_validation_catergory(p.validation_catergory);
      lnd_asisa.land_data.set_validation_descriptions(p.validation_descriptions);
      lnd_asisa.land_data.set_run_validations(p.run_validations);
      lnd_asisa.land_data.set_send_notifications(p.send_notifications);
      lnd_asisa.land_data.set_severity_no(p.serverity_no);
      lnd_asisa.land_data.set_severity_desc(p.severity_desc);
      lnd_asisa.land_data.set_display_on_reports(p.display_on_reports);
      lnd_asisa.land_data.set_log_exception_records(p.log_exception_records);
      lnd_asisa.land_data.set_continue_ind(p.contunue_ind);
      lnd_asisa.land_data.set_message_category_failure(p.message_category_failure);
      lnd_asisa.land_data.set_message_body_failure(p.message_body_failure);
      lnd_asisa.land_data.set_pause_continue_ind(p.pause_continue_ind);
      
      end loop;
      
      if nvl(lnd_asisa.land_data.get_run_validations,'N') ='Y' then 
       load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                           p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                           p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                           p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                           p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                           p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions,
                           p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                           p_exception_description   => lnd_asisa.land_data.v_message_body_failure||' '||p_detailed_error,
                           p_result_action           => lnd_asisa.land_data.v_pause_continue_ind,
                           p_record_count            => 1);               
      end if;  
   
   exception
   when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
   end;   
------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure delete_unsused_objects is
  v_sql         clob;
  v_log_no      varchar2(2000);
  v_proc_name   logit.run_times.procedure_name%type := 'land_all.delete_unsused_objects';
  begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name||m_fin_period, m_fin_year_cd,m_fin_month_cd,systimestamp);

    for ix in (select distinct object_name
                 from lnd_asisa.created_asisa_objects)
    loop
      v_sql :='drop table '||ix.object_name;
       execute immediate v_sql;
    end loop;

    exception
      when others then
       logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure Successful_ETL is
  v_log_no                varchar2(2000);
  v_proc_name             logit.run_times.procedure_name%type := 'land_all.Successful_ETL';
  v_pause_count           number;
  w_recipients            lnd_asisa.asisa_email_recipients.recipients_addresses%type;
  w_subject               varchar2(2000);
  w_check_point           number(10) :=0;
 begin

  logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name||m_fin_period, m_fin_year_cd,m_fin_month_cd,systimestamp);

 select count(*)
  into w_check_point
 from lnd_asisa.processed_files
where mapping_id =lnd_asisa.land_data.get_mapping_id
  and file_name is not null;

if nvl(w_check_point,0) >0 then

  select count(*)
   into v_pause_count
  from lnd_asisa.asisa_exception_report er
  where er.mapping_id =lnd_asisa.land_data.get_mapping_id
    and lower(er.result_action) ='pause';

  if nvl(v_pause_count,0) = 0 then

    for x in (select distinct recipients_addresses
                  from lnd_asisa.asisa_email_recipients ve
                 where ve.mapping_id = lnd_asisa.land_data.get_mapping_id
                   and recipients_addresses is not null
                   and ve.validation_category_no in (w_etl_complete)
             )
     loop
     w_recipients :=w_recipients ||';'||x.recipients_addresses;
     end loop;

    for rec in (select message_category_success,
                       message_body_success
                  from lnd_asisa.asisa_validation_emails ve
                 where ve.cat_no =w_etl_complete) loop

     w_subject := upper(lnd_asisa.land_data.get_asisa_fund_manager_names)||' : '||rec.message_category_success;

     custom.send_email_att.send_mails(p_sender    => w_sender,
                                   p_recipients   => substr(w_recipients,2),
                                   p_subject      => w_env ||' - '|| w_subject,
                                   p_message      => lnd_asisa.land_data.get_asisa_fund_manager_names||' '||rec.message_body_success,
                                   p_attachment   => null,
                                   p_att_inline   => false,
                                   p_att_filename => null);
     end loop;
     
  end if;
end if;
 exception
   when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
 end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure Staging_stopped_notification is
  v_log_no                varchar2(2000);
  v_proc_name             logit.run_times.procedure_name%type := 'land_all.Staging_stopped_notification';
  w_recipients            lnd_asisa.asisa_email_recipients.recipients_addresses%type;
  w_subject               varchar2(2000);
  w_mapping_id            number :=w_etl_stagging_paused;
 begin

  logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name||m_fin_period, m_fin_year_cd,m_fin_month_cd,systimestamp);

    for x in (select distinct recipients_addresses
                  from lnd_asisa.asisa_email_recipients ve
                  where ve.mapping_id = w_mapping_id
                   and recipients_addresses is not null
                   and ve.validation_category_no in (w_etl_stagging_paused)
             )
     loop
     w_recipients :=w_recipients ||';'||x.recipients_addresses;
     end loop;

    for rec in (select message_category_success,
                       message_body_success
                  from lnd_asisa.asisa_validation_emails ve
                 where ve.cat_no =w_etl_stagging_paused) loop

     w_subject := rec.message_category_success;

     custom.send_email_att.send_mails(p_sender    => w_sender,
                                   p_recipients   => substr(w_recipients,2),
                                   p_subject      => w_env ||' - '||w_subject,
                                   p_message      => rec.message_body_success,
                                   p_attachment   => null,
                                   p_att_inline   => false,
                                   p_att_filename => null);
     end loop;

 exception
   when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
 end; 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 function check_file_extension return number is
    v_proc_name     logit.run_times.procedure_name%type :='land_all.check_file_extension';
    v_log_no        number;
    v_file_suffix   varchar2(10);
    w_error         varchar2(200) :='The correct file extension/Type should be CSV(Comma delimited).';
  begin

     logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

     lnd_asisa.land_data.set_validation_sub_no('1.5');
     initialize_exception_params;

     v_file_suffix := substr(lnd_asisa.land_data.v_file_name,(instr(lnd_asisa.land_data.v_file_name,'.',-1,1) +1),length(lnd_asisa.land_data.v_file_name));

    if nvl(upper(v_file_suffix),'@@@') !='CSV' then
      delete
        from lnd_asisa.asisa_exception_report er
       where er.mapping_id         = lnd_asisa.land_data.get_mapping_id
         and er.validation_sub_no  = lnd_asisa.land_data.get_validation_sub_no
         and er.validation_type_no = lnd_asisa.land_data.get_validation_cat_no
         and trim(substr(er.file_name,0,instr(er.file_name,'.')-1)) = trim(substr(lnd_asisa.land_data.v_file_name,0,instr(lnd_asisa.land_data.v_file_name,'.')-1));

     for indx in(select message_body_failure
               from lnd_asisa.asisa_validation_emails e
               where e.mapping_id        = lnd_asisa.land_data.get_mapping_id
                 and e.validation_number = lnd_asisa.land_data.get_validation_sub_no
                 and e.cat_no            = lnd_asisa.land_data.get_validation_cat_no)
      loop

      load_exception_data(p_validation_type_no      => lnd_asisa.land_data.get_validation_cat_no,
                          p_validation_type_desc    => lnd_asisa.land_data.get_validation_catergory,
                          p_exception_severity_no   => lnd_asisa.land_data.get_severity_no,
                          p_validation_no           => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_sub_no       => lnd_asisa.land_data.get_validation_sub_no,
                          p_validation_short_desc   => lnd_asisa.land_data.get_validation_descriptions||'"'||v_file_suffix||'"',
                          p_exception_severity_desc => lnd_asisa.land_data.get_severity_desc,
                          p_exception_description   => w_error,
                          p_result_action           => lnd_asisa.land_data.get_pause_continue_ind,
                          p_record_count            =>1);
      return 1;
     end loop;
    else

     delete
        from lnd_asisa.asisa_exception_report er
       where er.mapping_id         = lnd_asisa.land_data.get_mapping_id
         and er.validation_sub_no  = lnd_asisa.land_data.get_validation_sub_no
         and er.validation_type_no = lnd_asisa.land_data.get_validation_cat_no
         and trim(substr(er.file_name,0,instr(er.file_name,'.')-1)) =trim(substr(lnd_asisa.land_data.v_file_name,0,instr(lnd_asisa.land_data.v_file_name,'.')-1));
 
      return 0;
    end if;

  exception
    when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp,sqlcode,sqlerrm ||' - Stacktrace: ' ||dbms_utility.format_error_backtrace);
     return 1;
  end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure delete_manager_run_info is
    v_log_no               logit.run_times.no%type;
    v_proc_name            logit.run_times.procedure_name%type := 'land_all.delete_manager_run_info';
  begin
   logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp); 
    
    delete
      from lnd_asisa.processed_files; 
     exception
    when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp,sqlcode,sqlerrm ||' - Stacktrace: ' ||dbms_utility.format_error_backtrace);
  end;
-------------------------------------------------------------------------------------------------------------------------------------------------
 procedure land_all is
    v_log_no               logit.run_times.no%type;
    v_proc_name            logit.run_times.procedure_name%type := 'land_all';
    v_src_dir              varchar2(250) := null;
    v_src_sub_dir          varchar2(250) := null;
    w_val_results          number(10)    := 0;
    w_data_results         number        := 0;
    w_consistencey         number        := 0;
    w_external_table_load  number :=0;
  begin
    set_load_date;
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);

    delete from lnd_asisa.created_asisa_objects;
    
    delete
      from lnd_asisa.asisa_exception_report er;
    
    for man_rec in c_main loop
      lnd_asisa.land_data.set_mapping_id(man_rec.mapping_id);
      delete_manager_run_info;
      for rec in c_asisa_fund_man_cur(lnd_asisa.land_data.get_mapping_id)
        loop
         lnd_asisa.land_data.set_asisa_fund_manager_names(rec.asisa_fund_manager_names);
         lnd_asisa.land_data.set_search_string_mask(rec.search_string_mask);
         lnd_asisa.land_data.set_asisa_directory(rec.asisa_directory);
         lnd_asisa.land_data.set_external_table_name(rec.external_table_name);
         lnd_asisa.land_data.set_asisa_landing_tables(rec.asisa_landing_tables);
         lnd_asisa.land_data.set_file_type_no(rec.file_type_no);
         lnd_asisa.land_data.set_asisa_lnd_table_cols(rec.asisa_lnd_table_cols);
         lnd_asisa.land_data.set_template_column(rec.template_column);

        ifilelist := frid_utlfile.getospathlist(fridweb.utlfile.getospath_for_oradir(lnd_asisa.land_data.v_asisa_directory),'%' ||lnd_asisa.land_data.v_search_string_mask||'%.'||'%'); -- get full list of files

        if iFileList.Count > 0 then
          v_src_dir     := fridweb.utlfile.getospath_for_oradir(lnd_asisa.land_data.v_asisa_directory) || '/' ||run_date_string;
          v_src_sub_dir := v_src_dir || '/' ||lnd_asisa.land_data.v_search_string_mask || 's';
          fridweb.utlfile.createosfolder(v_src_dir); --  create archive main folder
          fridweb.utlfile.createosfolder(v_src_sub_dir);
        
        for i in 1 .. iFileList.count
        loop
          lnd_asisa.land_data.set_file_name(iFileList(i));
        if nvl(check_file_extension,0) =0 then
           
           w_external_table_load :=create_dynamic_ext_tables;
          if nvl(w_external_table_load,0) >0 then
            populate_file_valuation_date;
            populate_assetmanagercode;
            w_val_results := start_validations;
            if w_val_results = 0 then  --Structure validation
               c_file_id := load_landing_data;
              if nvl(lnd_asisa.land_data.get_file_id,0) > 0 then
                 w_data_results := data_validation;
                if w_data_results = 0 then -- data validation
                   Load_asisa_file_summary;
                   w_consistencey := perfom_consistency_checks;
                   if w_consistencey =0 then
                    fridweb.utlfile.copyfiles(fridweb.utlfile.getospath_for_oradir(rec.asisa_directory),v_src_sub_dir,iFileList(i),null,true,true,true);
                    fridweb.utlfile.deletefile(frid_utlfile.getOSPath_for_OraDir(rec.asisa_directory)  ||'/'|| iFileList(i));
                    else
                     null;
                    end if;
                  else
                   null;
                  end if; --End Data Validation
                else
                   null;
               end if;
              else
              null;
            end if;       --End Structure Validation
          else
                          --No Data Loaded on External Table
           null;
          end if;         --External Table Loading
         end if;          --Checking File Extensions
        end loop;
       end if;
      end loop;
      notification;
      --Clean record for before starting new fund manager
    end loop;
  delete_unsused_objects;
  exception
    when others then
      logit.etl_run_times.exception_etl_procedure(v_log_no,systimestamp,sqlcode,sqlerrm ||' - Stacktrace: ' ||dbms_utility.format_error_backtrace);
  end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure land_asisa is
   v_log_no       logit.run_times.no%type;
   v_proc_name    logit.run_times.procedure_name%type := 'lnd_asisa.land_asisa';
  begin
    logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
    lnd_asisa.land_data.land_all;
  exception
    when others then
    logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
    rollback;
    raise;
  end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure stage_asisa is
  v_log_no        logit.run_times.no%type;
  v_proc_name     logit.run_times.procedure_name%type :='lnd_asisa.stage_asisa';
  w_error_count   number :=0;
  w_count         number :=1;
  w_string        varchar2(2000) :='Staging has been paused, one or more file(s) has Failed validations, Please check the exception reports for the following fund Manager(s)'||chr(10);
  w_recipients    varchar2(200);
  w_subject       varchar2(200);
 begin
  
  logit.etl_run_times.start_etl_procedure(v_log_no, c_etl_id, c_version, v_proc_name || m_fin_period, m_fin_year_cd, m_fin_month_cd, systimestamp);
  
  select count(*) 
    into w_error_count 
   from lnd_asisa.asisa_exception_report er  
  where upper(er.result_action) ='PAUSE';
  
  if nvl(w_error_count,0) = 0 then
     gbm.load_asisa.load_from_be;
  else
   for rec in (select distinct fund_manager_name 
                 from lnd_asisa.asisa_exception_report er 
                where upper(er.result_action) ='PAUSE')
     loop
        w_string :=w_string || w_count ||'. '||rec.fund_manager_name||' '||chr(10);
        w_count  := w_count +1;
     end loop; 
   
    for x in (select distinct recipients_addresses,description
                  from lnd_asisa.asisa_email_recipients ve
                 where recipients_addresses is not null
                   and ve.validation_category_no in (w_etl_stagging_paused))
     loop
     w_recipients :=x.recipients_addresses;
     w_subject    :=x.description;
     end loop;
     
     custom.send_email_att.send_mails(p_sender    => w_sender,
                                   p_recipients   => w_recipients,
                                   p_subject      => w_env ||' - '||w_subject,
                                   p_message      => w_string,
                                   p_attachment   => null,
                                   p_att_inline   => false,
                                   p_att_filename => null);
       
  end if;
 exception
   when others then
     logit.etl_run_times.exception_etl_procedure(v_log_no, systimestamp, sqlcode, sqlerrm || ' - Stacktrace: ' || dbms_utility.format_error_backtrace);
  end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------
end;
/