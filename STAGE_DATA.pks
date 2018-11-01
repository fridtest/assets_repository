CREATE OR REPLACE PACKAGE STG_SILICA.stage_data 
	AS
  c_version CONSTANT logit.run_times.version % TYPE := 1;
  c_etl_id CONSTANT logit.run_times.stream_name % TYPE := UPPER('STAGE_SILICA');

	PROCEDURE set_load_date(p_load_date DATE DEFAULT TRUNC(sysdate));

	FUNCTION get_load_date
		RETURN DATE;

	PROCEDURE stage_all;

	PROCEDURE update_source_mappings;
END stage_data;

/