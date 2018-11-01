--sqlplus  custom/custom@imiuat
--set echo on 

select * from dual;
select * from v$instance;
select * from dba_objects where rownum <10;
@script1.sql
@script2.sql
@LAND_DATA.pkb

echo "Compiling Silica Packages"

@STAGE_DATA.pkb
@STAGE_DATA.pks

exit
