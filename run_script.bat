--sqlplus  custom/custom@imiuat
--set echo on 

select * from dual;
select * from v$instance;
select * from dba_object where rownum <10;

pause
