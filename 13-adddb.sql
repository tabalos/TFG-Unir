set linesize 200
set pagesize 999
SET HEAD OFF
SET VER OFF
set feedback off


--prompt ##################################################################################################
--prompt For non multitenant databases use unique database name and instance name
--prompt For multitenant - use PDB name as database name and CDB name as a instance name
--prompt ##################################################################################################
prompt

-- accept DBNAME prompt "Enter database name " 
SET TERMOUT OFF
column f_st new_value DBNAME
select name f_st from sys.v_$database;

SET TERMOUT ON
prompt ##################################################################################################
prompt  Database Name: &DBNAME
prompt ##################################################################################################
prompt

accept INST_NUM default 1 prompt "Enter number of instances [default 1] "


SET TERMOUT OFF

spool instance.sql
-- select 'accept HOST' || rownum || ' prompt "Enter host name for instance number ' || rownum || ' "' from all_source where rownum <= &INST_NUM;
-- select 'accept INST' || rownum || ' default ' || decode('&INST_NUM',1,'&DBNAME', '&DBNAME' || rownum) || ' prompt "Enter instance name (or CDB for 12c) for instance number ' 
--        || rownum || ' [ default ' || decode('&INST_NUM',1,'&DBNAME', '&DBNAME' || rownum) ||' ] "' from all_source where rownum <= &INST_NUM;
select 'column host' || rownum || ' new_value HOST' || rownum from dual connect by level<= &INST_NUM;
select 'column inst' || rownum || ' new_value INST' || rownum from dual connect by level<= &INST_NUM;
select 'select regexp_substr(host_name,''[^.]+'',1,1) host' || rownum || ', instance_name inst' || rownum || ' from sys.gv_$instance where inst_id=' || rownum || ';' from dual connect by level<= &INST_NUM;

spool off

@instance.sql

SET TERMOUT ON

accept PORT default 1521 prompt "Enter listener port number [default 1521] "
-- accept SASHPASS prompt "Enter SASH password on target database "

set term off
--- set term off
-- column f_st new_value v_dblink
-- select substr('&DBNAME' || '_' || replace('&HOST1','.','_'),1,30) f_st from dual;


spool setup.sql
select 'exec sash_repo.add_db(''&' || 'HOST' || rownum || ''', ' || &PORT || ', ''sash'', ''&DBNAME'', ''&' || 'INST' || rownum || ''',' || rownum || ', null);' from all_source where rownum <= &INST_NUM;
select 'exec sash_pkg.configure_db;' from dual;
select 'exec sash_pkg.set_dbid;' from dual;
select 'exec sash_repo.setup_jobs' from dual;
select 'exec sash_repo.start_collecting_jobs' from dual;
select 'exec sash_repo.purge_tables' from dual;
spool off
set term on

spool setup.log
@setup.sql

prompt
prompt ##################################################################################################
prompt  Database added. 
prompt ##################################################################################################
prompt 

prompt  List of databases

set head on
col dbname format a14
col host format a30
col current_db format a10
select s.dbname, s.dbid, s.host, s.inst_num, case when t.dbid is null then ' ' else '*' end current_db from sash_targets s, sash_target t where s.dbid = t.dbid(+)
and s.inst_num = t.inst_num(+) order by dbid, inst_num;
prompt

spool off

