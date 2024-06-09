set linesize 200
set pagesize 999
SET HEAD OFF
SET VER OFF
set feedback off

spool exit.sql
select 'exit' from dual where SYS_CONTEXT ('USERENV', 'SESSION_USER') != upper('SYS');
spool off
@exit.sql

prompt "------------------------------------------------------------------------------------"
prompt  Creating repository owner and job kill function using SYS user                     
prompt "------------------------------------------------------------------------------------"

spool sys_objects.log
@03-repo_user.sql
@04-repo_sys_procedure.sql
spool off

WHENEVER SQLERROR CONTINUE NONE 

undef ENTER_SASH_TNS
col sash_tns noprint new_value SASH_TNS
accept ENTER_SASH_TNS prompt "Enter TNS alias to connect to database - required for 12c plugable DB [leave it empty to use SID]? "
select case when nvl('&&ENTER_SASH_TNS','x') = 'x' then '' else '@' || nvl('&&ENTER_SASH_TNS','') end  sash_tns from dual;

connect sash/sash&SASH_TNS
set term off
spool exit.sql
select 'exit' from dual where SYS_CONTEXT ('USERENV', 'SESSION_USER') != upper('sash');
spool off
@exit.sql

set term on
prompt "------------------------------------------------------------------------------------"
prompt  Installing SASH objects into SASH schema                                     
prompt "------------------------------------------------------------------------------------"
set term off

@05-repo_helpers.sql
@06-repo_schema.sql
@07-repo_triggers.sql
@08-repo_views.sql
@09-sash_repo.sql
@10-sash_pkg.sql
@11-sash_xplan.sql
@12-sash_awr_views.sql
set term on

prompt "------------------------------------------------------------------------------------"
prompt  Instalation completed. Starting SASH configuration process                         
prompt  Press Control-C if you do not want to configure target database at that time.
prompt "------------------------------------------------------------------------------------"

@13-adddb.sql

exit 
