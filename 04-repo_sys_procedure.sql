-- (c) Marcin Przepiorowski 2010
-- v2.1 Changes: Initial release
-- v2.2 Changes:
-- v2.3 Changes: using SASH_USER instead of hardcoded
-- v2.4

create or replace procedure kill_sash_session as
    -- File Revision $Rev$
	vsql varchar2(100);
	begin
	for a in (select '''' || sid || ',' || serial# || '''' ss from v$session where sid in (select sid from dba_jobs_running jr, dba_jobs j where j.job = jr.job and what like '%sash_pkg%')) loop
		dbms_output.put_line(a.ss);
		vsql:='alter system kill session ' || a.ss ;
		dbms_output.put_line(vsql);
		insert into sash.sash_log (action, message,result) values ('kill_sash_session','killing job ' || vsql, 'I');		
		execute immediate vsql ;
	end loop;
end;
/

grant execute on kill_sash_session to sash;
