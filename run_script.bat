--sqlplus  custom/custom@imiuat
--set echo on 

select * from dual;
select * from v$instance;
select * from dba_objects where rownum <10;

pause
