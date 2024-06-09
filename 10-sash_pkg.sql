-- (c) Kyle Hailey 2007
-- (c) Marcin Przepiorowski 2010
-- v2.0 Package deployed on target database, database link pointed to repository 
-- v2.1 Changes: - Deployed on repository database not on target, 
--               - Data collection via DB link pointed to target, 
--               - Bug fixing in get_sqlids
-- v2.2 Changes: - splited between 10g and above and 9i
--               - gathered instance statistic 
--               - sql information improved
--               - RAC support
--               - multi db support
-- v2.3 Changes  - full RAC and multi DB support
--               - gathering metrics
--               - logging
-- v2.4 Changes  - new collection procedures for AWR

spool sash_pkg.log
prompt Creating SASH_PKG package

create sequence sashseq cache 1000;

--
-- BEGIN CREATE TARGET PACKAGE SASH_PKG
--
CREATE OR REPLACE PACKAGE sash_pkg AS
    procedure configure_db;
    procedure get_all ;
    procedure get_stats ;
    procedure get_one(v_sql_id varchar2);
    procedure get_objs(l_dbid number)  ;
    procedure get_latch;
    procedure get_users;
    procedure get_params  ;
    procedure get_sqltxt(l_dbid number) ;
    procedure get_sqlstats(l_hist_samp_id number, l_dbid number)  ;
    procedure get_sqlid(l_dbid number, v_sql_id varchar2) ;
    procedure get_sqlplans(l_hist_samp_id number, l_dbid number) ;
    procedure get_extents;
    procedure get_event_names  ;
    procedure collect_other(v_sleep number, loops number);
    procedure collect_ash (v_sleep number, loops number, vinstance number) ;
    function get_dbid return number ;
    function get_version  return varchar2 ;
    procedure set_dbid;
--    procedure set_dbid ( v_dbid number)  ;
    procedure collect_metric(v_hist_samp_id number) ;
    procedure get_metrics ;
    procedure collect_iostat(v_hist_samp_id number) ;
    procedure collect_histogram(v_hist_samp_id number);
    procedure collect_osstat(v_hist_samp_id number);
    procedure collect_systime(v_hist_samp_id number);
END sash_pkg;
/
show errors

CREATE OR REPLACE PACKAGE BODY sash_pkg AS

PROCEDURE get_data_files is
    l_dbid number;
    sql_stat varchar2(4000);
  TYPE SashcurTyp IS REF CURSOR;
  sash_cur   SashcurTyp;
    sash_rec sash_data_files%rowtype;

 begin
     l_dbid:=get_dbid;
     sql_stat:= 'select /*+DRIVING_SITE(f) */ :1, file_name, file_id, tablespace_name from sys.dba_data_files' ||
     ' f where file_id not in (select file_id from sash_data_files where dbid = :2)';
     execute immediate 'MERGE INTO sash_data_files l USING sys.dba_data_files' || ' r ON (l.file_id = r.file_id and l.dbid = :1)
                        WHEN MATCHED THEN UPDATE SET l.file_name = r.file_name WHERE l.dbid = :2
                        WHEN NOT MATCHED THEN INSERT (dbid, file_name, file_id, tablespace_name) VALUES (:3, r.file_name, r.file_id, r.tablespace_name)'
     using l_dbid,l_dbid,l_dbid;
    exception
        when DUP_VAL_ON_INDEX then
            sash_repo.log_message('GET_DATA_FILES', 'Already configured ?','W');
end get_data_files;


procedure configure_db is

begin
    sash_repo.log_message('configure_db', 'get_event_names' ,'I');
    sash_pkg.get_event_names;
    sash_repo.log_message('configure_db', 'get_users' ,'I');
    sash_pkg.get_users;
    sash_repo.log_message('configure_db', 'get_params' ,'I');
    sash_pkg.get_params;
    sash_repo.log_message('configure_db', 'get_data_files' ,'I');
    sash_pkg.get_data_files;
    sash_repo.log_message('configure_db', 'get_metrics' ,'I');
    sash_pkg.get_metrics;
    commit;
exception
    when others then
        sash_repo.log_message('configure_db', SUBSTR(SQLERRM, 1 , 1000) ,'E');
        RAISE_APPLICATION_ERROR(-20100,'SASH configure_db error ' || SUBSTR(SQLERRM, 1 , 1000));
end configure_db;


FUNCTION get_version return varchar2 is
    l_ver sash_targets.version%type;
    begin
      -- execute immediate 'select version from sash_targets where lower(db_link) = lower('''||v_dblink||''')' into l_ver;
      execute immediate 'select version from sys.v_$instance' into l_ver;
      return l_ver;
end get_version;

FUNCTION is_pdb return varchar2 is
    l_ispdb varchar2(1) := 'N';
begin
	if (to_number(sys_context('USERENV','CON_ID')) > 0) then
		l_ispdb := 'Y';
	end if;
    return l_ispdb;
end is_pdb;


FUNCTION get_dbid return number is
	l_ver  sash_targets.version%type;
    l_dbid number;
    begin

    l_ver:=get_version;
      --  l_dblink := replace(v_dblink,'-','_');
      --execute immediate 'select dbid  from sys.v_$database@'||v_dblink into l_dbid;
	 -- execute immediate 'select dbid  from sash_targets where db_link = :1' into l_dbid using l_dblink;

	  if (substr(l_ver,1,2) > '11' and is_pdb='Y') then
	  	execute immediate 'select con_dbid from sys.v_$database' into l_dbid;
	  else
	    execute immediate 'select dbid from sys.v_$database' into l_dbid;
	  end if;
      return l_dbid;
end get_dbid;


PROCEDURE get_users is
    l_dbid number;
    v_command varchar2(4000);
    begin
	  l_dbid:=get_dbid;
	  v_command := 'insert into sash_users
               (dbid, username, user_id)
               select ' || l_dbid || ',username,user_id from sys.dba_users'||
               ' u where user_id not in (select user_id from sash_users where dbid = ' || l_dbid || ')';

      -- execute immediate 'insert into sash_users
      --          (dbid, username, user_id)
      --          select ' || l_dbid || ',username,user_id from dba_users'||
      --          ' u where user_id not in (select user_id from sash_users where dbid = ' || l_dbid || ')';
      -- dbms_output.put_line('Executo: ' || v_command);
      execute immediate v_command;
    exception
        when DUP_VAL_ON_INDEX then
            sash_repo.log_message('GET_USERS', 'Already configured ?','W');
end get_users;

PROCEDURE get_latch is
 l_dbid number;
 begin
   	l_dbid:=get_dbid;
    execute immediate 'insert into sash_latch (dbid, latch#, name) select ' || l_dbid || ',latch#, name from sys.v_$latch';
end get_latch;

procedure get_stats is
 l_dbid number;
 begin
    l_dbid:=get_dbid;
    execute immediate 'insert into sash_stats select ' || l_dbid || ', STATISTIC#, name, 0 from sys.v_$sysstat';
end get_stats;


PROCEDURE get_params is
   l_dbid number;
   begin
     l_dbid:=get_dbid;
     execute immediate 'insert into sash_params ( dbid, name, value) select ' || l_dbid || ',name,value from sys.v_$parameter';
    exception
        when DUP_VAL_ON_INDEX then
            sash_repo.log_message('GET_PARAMS', 'Already configured ?','W');
end get_params;

PROCEDURE get_metrics is
   l_dbid number;
   l_ver sash_targets.version%type;
   begin
    begin
     l_dbid:=get_dbid;

     l_ver:=get_version;

	 /* Original
	 execute immediate 'insert into sash_sysmetric_names select distinct ' || l_dbid || ',METRIC_ID,METRIC_NAME,METRIC_UNIT from sys.v_$sysmetric_history@'||v_dblink ||
     ' where metric_name in (
        ''User Transaction Per Sec'',
        ''Physical Reads Per Sec'',
        ''Physical Reads Per Txn'',
        ''Physical Writes Per Sec'',
        ''Redo Generated Per Sec'',
        ''Redo Generated Per Txn'',
        ''Redo Writes Per Sec'',
        ''Logons Per Sec'',
        ''User Calls Per Sec'',
        ''User Commits Per Sec'',
        ''Logical Reads Per Txn'',
        ''Logical Reads Per Sec'',
        ''Total Parse Count Per Txn'',
        ''Network Traffic Volume Per Sec'',
        ''Enqueue Requests Per Txn'',
        ''DB Block Changes Per Txn'',
        ''Current Open Cursors Count'',
        ''SQL Service Response Time'',
        ''Response Time Per Txn'',
        ''Executions Per Sec'',
        ''Average Synchronous Single-Block Read Latency'',
        ''I/O Megabytes per Second'',
        ''I/O Requests per Second'',
        ''Average Active Sessions''
     )';
	 */

    if (substr(l_ver,1,2) > '11' and is_pdb='Y') then
	  execute immediate 'insert into sash_sysmetric_names select distinct ' || l_dbid || ',METRIC_ID,METRIC_NAME,METRIC_UNIT from sys.v_$con_sysmetric_history' ||
     ' where metric_name in (
		''Average Active Sessions'',
		''Average Synchronous Single-Block Read Latency'',
		''CPU Usage Per Sec'',
		''Current Open Cursors Count'',
		''Current OS Load'',
		''DB Block Changes Per Txn'',
		''Enqueue Requests Per Txn'',
		''Executions Per Sec'',
		''GC CR Block Received Per Second'',
		''GC Current Block Received Per Second'',
		''Global Cache Average CR Get Time'',
		''Global Cache Average Current Get Time'',
		''Hard Parse Count Per Sec'',
		''Host CPU Utilization (%)'',
		''I/O Megabytes per Second'',
		''I/O Requests per Second'',
		''Logical Reads Per Sec'',
		''Logical Reads Per Txn'',
		''Logons Per Sec'',
		''Network Traffic Volume Per Sec'',
		''Open Cursors Per Sec'',
		''Physical Read Total Bytes Per Sec'',
		''Physical Read Total IO Requests Per Sec'',
		''Physical Reads Per Sec'',
		''Physical Reads Per Txn'',
		''Physical Write Total Bytes Per Sec'',
		''Physical Write Total IO Requests Per Sec'',
		''Physical Writes Per Sec'',
		''Process Limit %'',
		''Redo Generated Per Sec'',
		''Redo Generated Per Txn'',
		''Redo Writes Per Sec'',
		''Response Time Per Txn'',
		''SQL Service Response Time'',
		''Total Parse Count Per Txn'',
		''Total Table Scans Per Sec'',
		''User Calls Per Sec'',
		''User Commits Per Sec'',
		''User Rollbacks Per Sec'',
		''User Transaction Per Sec''
     )';

	else

	  execute immediate 'insert into sash_sysmetric_names select distinct ' || l_dbid || ',METRIC_ID,METRIC_NAME,METRIC_UNIT from sys.v_$sysmetric_history' ||
     ' where metric_name in (
		''Average Active Sessions'',
		''Average Synchronous Single-Block Read Latency'',
		''CPU Usage Per Sec'',
		''Current Open Cursors Count'',
		''Current OS Load'',
		''DB Block Changes Per Txn'',
		''Enqueue Requests Per Txn'',
		''Executions Per Sec'',
		''GC CR Block Received Per Second'',
		''GC Current Block Received Per Second'',
		''Global Cache Average CR Get Time'',
		''Global Cache Average Current Get Time'',
		''Hard Parse Count Per Sec'',
		''Host CPU Utilization (%)'',
		''I/O Megabytes per Second'',
		''I/O Requests per Second'',
		''Logical Reads Per Sec'',
		''Logical Reads Per Txn'',
		''Logons Per Sec'',
		''Network Traffic Volume Per Sec'',
		''Open Cursors Per Sec'',
		''Physical Read Total Bytes Per Sec'',
		''Physical Read Total IO Requests Per Sec'',
		''Physical Reads Per Sec'',
		''Physical Reads Per Txn'',
		''Physical Write Total Bytes Per Sec'',
		''Physical Write Total IO Requests Per Sec'',
		''Physical Writes Per Sec'',
		''Process Limit %'',
		''Redo Generated Per Sec'',
		''Redo Generated Per Txn'',
		''Redo Writes Per Sec'',
		''Response Time Per Txn'',
		''SQL Service Response Time'',
		''Total Parse Count Per Txn'',
		''Total Table Scans Per Sec'',
		''User Calls Per Sec'',
		''User Commits Per Sec'',
		''User Rollbacks Per Sec'',
		''User Transaction Per Sec''
     )';
	end if;
    exception
        when DUP_VAL_ON_INDEX then
            sash_repo.log_message('GET_METRICS', 'Already configured ?','W');
    end;

end get_metrics;


PROCEDURE set_dbid is
   l_dbid number;
   l_inst number;
   cnt number;
   begin

     execute immediate 'select dbid, inst_num from sash_targets where rownum<2' into l_dbid, l_inst;
     select count(*) into cnt from
         sash_target;
     if cnt = 0 then
         insert into
            sash_target_static ( dbid, inst_num )
            values (l_dbid, l_inst);
     else
         update sash_target_static set dbid = l_dbid, inst_num = l_inst;
     end if;
end set_dbid;


PROCEDURE get_extents is
    l_dbid number;

 begin
    l_dbid:=get_dbid;

    execute immediate 'insert into sash_extents ( dbid, segment_name, partition_name, segment_type, tablespace_name, 	extent_id, file_id, block_id, bytes, blocks, relative_fno)
             select '|| l_dbid ||', segment_name, partition_name, segment_type, tablespace_name, 	extent_id, file_id, block_id, bytes, blocks, relative_fno from sys.dba_extents';
     exception
            when OTHERS then
                    sash_repo.log_message('GET_EXTENTS error', '','E');
    RAISE_APPLICATION_ERROR(-20115, 'SASH get_extents error ' || SUBSTR(SQLERRM, 1 , 1000));
end get_extents;

PROCEDURE get_event_names is
          l_dbid number;

       begin
          l_dbid:=get_dbid;
          execute immediate 'insert into sash_event_names ( dbid, event#, event_id, parameter1, parameter2, parameter3, wait_class_id, name, wait_class ) select distinct '||
          											  l_dbid ||', event#, event_id, parameter1, parameter2, parameter3, wait_class_id, name, wait_class from sys.v_$event_name';

         exception
            when DUP_VAL_ON_INDEX then
                    sash_repo.log_message('GET_EVENT_NAMES', 'Already configured ?','W');
end get_event_names;


PROCEDURE get_objs(l_dbid number) is
type sash_objs_type is table of sash_objs%rowtype;
sash_objsrec  sash_objs_type := sash_objs_type();
type ctype is ref cursor;
C_SASHOBJS ctype;
sql_stat varchar2(4000);


begin
    sql_stat:='select :1, o.object_id, o.owner, o.object_name, o.subobject_name, o.object_type
               from sys.dba_objects o, 
                    (select current_obj# object_id
               		 from ( select count(*) cnt, CURRENT_OBJ#
               		 		from sash
               		 		where current_obj# > 0
               		 		  and sample_time > (sysdate - 1/24)
               		 		group by current_obj#
               		 		order by cnt desc )
               		 where rownum < 100) ash
               where o.object_id=ash.object_id
                 and ash.object_id not in (select object_id from sash_objs where dbid = :2 
               )';

    open c_sashobjs for sql_stat using l_dbid, l_dbid;
    fetch c_sashobjs bulk collect into sash_objsrec;
    forall i in 1 .. sash_objsrec.count
             insert into sash_objs values sash_objsrec(i);
    close c_sashobjs;
exception
    when DUP_VAL_ON_INDEX then null;
    when others then
        sash_repo.log_message('get_objs', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20101, 'SASH get_objs error ' || SUBSTR(SQLERRM, 1 , 1000));
end get_objs;

PROCEDURE get_sqlplans(l_hist_samp_id number, l_dbid number) is
type sash_sqlrec_type is table of sash_sqlplans%rowtype;
sash_sqlrec  sash_sqlrec_type := sash_sqlrec_type();
type ctype is ref cursor;
c_sqlplans ctype;
sql_stat varchar2(4000);

begin
	
    sql_stat:='select SQL_ID, PLAN_HASH_VALUE, REMARKSDESC, OPERATION, OPTIONS, OBJECT_NODE, OBJECT_OWNER, OBJECT_NAME, OBJECT_INSTANCE, OBJECT_TYPE, OPTIMIZER, SEARCH_COLUMNS, ID, PARENT_ID, DEPTH, POSITION, COST, CARDINALITY, BYTES, OTHER_TAG, PARTITION_START, PARTITION_STOP, PARTITION_ID, OTHER, DISTRIBUTION, CPU_COST, IO_COST, TEMP_SPACE, ACCESS_PREDICATES, FILTER_PREDICATES, DBID, INST_ID
               from (select /*+DRIVING_SITE(sql) */
                          sql.sql_id,
                          sql.plan_hash_value,
                          ''REMARKS'' remarksdesc ,
                          sql.OPERATION,
                          sql.OPTIONS,
                          sql.OBJECT_NODE,
                          sql.OBJECT_OWNER,
                          sql.OBJECT_NAME,
                          0 OBJECT_INSTANCE,
                          sql.OBJECT_TYPE,
                          sql.OPTIMIZER,
                          sql.SEARCH_COLUMNS,
                          sql.ID,
                          sql.PARENT_ID,
                          sql.depth,
                          sql.POSITION,
                          sql.COST,
                          sql.CARDINALITY,
                          sql.BYTES,
                          sql.OTHER_TAG,
                          sql.PARTITION_START,
                          sql.PARTITION_STOP,
                          sql.PARTITION_ID,
                          sql.OTHER,
                          sql.DISTRIBUTION,
                          sql.CPU_COST,
                          sql.IO_COST,
                          sql.TEMP_SPACE,
                          sql.ACCESS_PREDICATES,
                          sql.FILTER_PREDICATES,
                          :1 DBID,
			              sql.inst_id,
			              row_number() over (partition by sql.sql_id, sql.plan_hash_value, sql.id order by sql.inst_id) rn
                    from sys.gv_$sql_plan sql, sash_hour_sqlid sqlids
                    where sql.sql_id= sqlids.sql_id
                      and sql.plan_hash_value = sqlids.sql_plan_hash_value
                    and not exists (select 1 from sash_sqlplans sqlplans where sqlplans.plan_hash_value = sqlids.sql_plan_hash_value
                                             and sqlplans.sql_id = sqlids.sql_id )
                    ) where rn=1';
    open c_sqlplans for sql_stat using l_dbid;
    fetch c_sqlplans bulk collect into sash_sqlrec;
    forall i in 1 .. sash_sqlrec.count
             insert into sash_sqlplans values sash_sqlrec(i);
    close c_sqlplans;
exception
    when others then
        sash_repo.log_message('get_sqlplans', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20102, 'SASH get_sqlplans error ' || SUBSTR(SQLERRM, 1 , 1000));
end get_sqlplans;

PROCEDURE get_sqlstats(l_hist_samp_id number, l_dbid number) is
type sash_sqlstats_type is table of sash_sqlstats%rowtype;
sash_sqlstats_rec sash_sqlstats_type;
type ctype is ref cursor;
c ctype;
sql_stat varchar2(4000);
v_lastall number;
l_ver varchar2(8);
l_oldsnap number;

begin
        begin
            select max(snap_id) into l_oldsnap from sash_sqlstats m where m.dbid = l_dbid;
                exception when NO_DATA_FOUND then
                        l_oldsnap:=1;
        end;


        l_ver:=substr(sash_pkg.get_version,0,4);

        if (l_ver = '10.2') then
           sql_stat:='select /*+driving_site(sql) */  :1, :2, inst_id,
                   sql_id,  plan_hash_value, parse_calls, disk_reads,
                          direct_writes, buffer_gets, rows_processed, serializable_aborts,
                       fetches, executions, end_of_fetch_count, loads, version_count,
                       invalidations,  px_servers_executions,  cpu_time, elapsed_time,
                       avg_hard_parse_time, application_wait_time, concurrency_wait_time,
                       cluster_wait_time, user_io_wait_time, plsql_exec_time, java_exec_time,
                       sorts, sharable_mem, total_sharable_mem, 0, 0, 0, 0,0,0,0,0,0 ,
                        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
                       from sys.gv_$sqlstats sql
                       where (sql.sql_id, sql.plan_hash_value) in ( select sql_id, SQL_PLAN_HASH_VALUE from sash_hour_sqlid t)';
        elsif (l_ver = '11.1') then
           sql_stat:='select /*+driving_site(sql) */  :1, :2, inst_id,
                   sql_id,  plan_hash_value, parse_calls, disk_reads,
                          direct_writes, buffer_gets, rows_processed, serializable_aborts,
                       fetches, executions, end_of_fetch_count, loads, version_count,
                       invalidations,  px_servers_executions,  cpu_time, elapsed_time,
                       avg_hard_parse_time, application_wait_time, concurrency_wait_time,
                       cluster_wait_time, user_io_wait_time, plsql_exec_time, java_exec_time,
                       sorts, sharable_mem, total_sharable_mem, typecheck_mem, io_interconnect_bytes,
                       io_disk_bytes, 0,0,0,0, exact_matching_signature, force_matching_signature ,
                        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
                       from sys.gv_$sqlstats sql
                       where (sql.sql_id, sql.plan_hash_value) in ( select sql_id, SQL_PLAN_HASH_VALUE from sash_hour_sqlid t)';
        elsif ((l_ver = '11.2') or (l_ver >= '12.1')) then
           sql_stat:='select /*+driving_site(sql) */  :1, :2, inst_id,
               sql_id,  plan_hash_value, parse_calls, disk_reads,
               direct_writes, buffer_gets, rows_processed, serializable_aborts,
               fetches, executions, end_of_fetch_count, loads, version_count,
               invalidations,  px_servers_executions,  cpu_time, elapsed_time,
               avg_hard_parse_time, application_wait_time, concurrency_wait_time,
               cluster_wait_time, user_io_wait_time, plsql_exec_time, java_exec_time,
               sorts, sharable_mem, total_sharable_mem, typecheck_mem, io_interconnect_bytes,
               0, physical_read_requests,  physical_read_bytes, physical_write_requests,
               physical_write_bytes, exact_matching_signature, force_matching_signature ,
               0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
               from sys.gv_$sqlstats sql
                where (sql.sql_id, sql.plan_hash_value) in ( select sql_id, SQL_PLAN_HASH_VALUE from sash_hour_sqlid t)';
        end if;
        --where last_active_time > sysdate - :4/24';
        --open c for sql_stat using l_hist_samp_id, l_dbid, v_inst_num, v_lastall ;
        open c for sql_stat using l_hist_samp_id, l_dbid;
        fetch c bulk collect into sash_sqlstats_rec;
        forall i in 1..sash_sqlstats_rec.count
            insert into sash_sqlstats values sash_sqlstats_rec(i);
        close c;

         update sash_sqlstats s set
        (fetches_delta, end_of_fetch_count_delta, sorts_delta, executions_delta, px_servers_execs_delta,
         loads_delta, invalidations_delta, parse_calls_delta, disk_reads_delta, buffer_gets_delta, rows_processed_delta,
         cpu_time_delta, elapsed_time_delta, iowait_delta, clwait_delta, apwait_delta, ccwait_delta, direct_writes_delta,
         plsexec_time_delta, javexec_time_delta, io_interconnect_bytes_delta, io_disk_bytes_delta, physical_read_requests_delta, physical_read_bytes_delta,
         physical_write_requests_delta, physical_write_bytes_delta) =
        (select
         s.fetches - old.fetches, s.end_of_fetch_count - old.end_of_fetch_count, s.sorts - old.sorts, s.executions - old.executions, s.px_servers_executions - old.px_servers_executions,
         s.loads - old.loads, s.invalidations - old.invalidations, s.parse_calls - old.parse_calls, s.disk_reads - old.disk_reads, s.buffer_gets - old.buffer_gets, s.rows_processed - old.rows_processed,
         s.cpu_time - old.cpu_time, s.elapsed_time - old.elapsed_time, s.user_io_wait_time - old.user_io_wait_time, s.cluster_wait_time - old.cluster_wait_time, s.application_wait_time - old.application_wait_time, s.concurrency_wait_time - old.concurrency_wait_time, s.direct_writes - old.direct_writes,
         s.plsql_exec_time - old.plsql_exec_time, s.java_exec_time - old.java_exec_time, s.io_interconnect_bytes - old.io_interconnect_bytes, s.io_disk_bytes - old.io_disk_bytes,
         s.physical_read_requests - old.physical_read_requests, s.physical_read_bytes - old.physical_read_bytes,
         s.physical_write_requests - old.physical_write_requests, s.physical_write_bytes - old.physical_write_bytes
         from sash_sqlstats old where
         old.snap_id = l_oldsnap and old.sql_id = s.sql_id and old.plan_hash_value = s.plan_hash_value and old.dbid = s.dbid and old.instance_number = s.instance_number)
         where snap_id = l_hist_samp_id;
exception
    when others then
        sash_repo.log_message('get_sqlstats', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20103, 'SASH get_sqlstats error ' || SUBSTR(SQLERRM, 1 , 1000));
end get_sqlstats;


PROCEDURE get_sqlid(l_dbid number, v_sql_id varchar2) is
begin
         insert into sash_hour_sqlid select sql_id, sql_plan_hash_value from sash where l_dbid = dbid and sql_id = v_sql_id;
end get_sqlid;

PROCEDURE get_sqlids(l_dbid number) is
          v_sqlid  number;
          v_sqllimit number:=0;
          v_lastall number;
          v_numinstances number;

       begin
       	 select count(*) into v_numinstances from sash_targets;	-- Per agafar v_sqllimit * v_numinstances
         begin
            select to_number(value) into v_sqllimit from sash_configuration where param='SQL LIMIT';
            dbms_output.put_line('v limit ' || v_sqllimit);
         exception when NO_DATA_FOUND then
            v_sqllimit:=30;
         end;

		 v_sqllimit := v_sqllimit * v_numinstances;  -- 15/09/2023 XR: Afegim agafar v_sqllimit * num instancies

         begin
            select to_number(value) into v_lastall from sash_configuration where param='STATFREQ';
            dbms_output.put_line('v_lastall ' || v_lastall);
            exception when NO_DATA_FOUND then
              v_lastall:=15;
         end;
	     dbms_output.put_line('v_lastall ' || v_lastall || ' - v_limit ' || v_sqllimit);
         insert into sash_hour_sqlid 
                          select distinct sql_id, sql_plan_hash_value            -- SQLs que están en ASH
                          from (select count(*) cnt, sql_id, sql_plan_hash_value
	                            from sash
	                            where l_dbid = dbid
	                               and sql_id != '0'
	                               --and sql_plan_hash_value != '0'
	                               and sample_time > sysdate - v_lastall/1440
	                               --and sample_time > sysdate - 1/24
	 			                   -- and inst_id = v_inst_num
	                            group by sql_id, sql_plan_hash_value
	                            order by cnt desc )
	                      where rownum <= v_sqllimit
	                      union
	                      select distinct sql_id, plan_hash_value                -- SQLs con más ELAPSED_TIME por ejecución
	                      from (select sql_id, plan_hash_value
	                            from sys.gv_$sqlstats
	                            order by (elapsed_time/decode(executions,0,1)) desc)
	                      where rownum <= v_sqllimit
	                      union
	                      select distinct sql_id, plan_hash_value                -- SQLs con más CPU_TIME por ejecución
	                      from (select sql_id, plan_hash_value
	                            from sys.gv_$sqlstats
	                            order by (cpu_time/decode(executions,0,1)) desc)
	                      where rownum <= v_sqllimit
	                      union
	                      select distinct sql_id, plan_hash_value                -- SQLs con más PARSE_TIME por ejecución
	                      from (select sql_id, plan_hash_value
	                            from sys.gv_$sqlstats
	                            order by avg_hard_parse_time desc)
	                      where rownum <= v_sqllimit
	                      union
	                      select distinct sql_id, plan_hash_value                -- SQLs con más SHARABLE_MEM por ejecución 
	                      from (select sql_id, plan_hash_value
	                            from sys.gv_$sqlstats
	                            order by SHARABLE_MEM desc)
	                      where rownum <= v_sqllimit
	                      union
	                      select distinct sql_id, plan_hash_value                -- SQLs con más VERSION_COUNT por ejecución 
	                      from (select sql_id, plan_hash_value
	                            from sys.gv_$sqlstats
	                            order by VERSION_COUNT desc)
	                      where rownum <= v_sqllimit
                        ;
exception
    when others then
        sash_repo.log_message('get_sqlids', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20104, 'SASH get_sqlids error ' || SUBSTR(SQLERRM, 1 , 1000));
end get_sqlids;


PROCEDURE get_sqltxt(l_dbid number)  is
sql_stat varchar2(4000);

begin

    sql_stat:='insert into sash_sqltxt
               select DBID, SQL_ID, SQL_FULLTEXT, COMMAND_TYPE
               from (
	                select /*+DRIVING_SITE(sqlt) */  :1 as DBID, SQL_ID, SQL_FULLTEXT, COMMAND_TYPE, row_number() over (partition by sql_id order by inst_id) rn
	                -- from sys.v_$sqlstats sqlt
	                from sys.gv_$sql sqlt
	                where sqlt.sql_id in
	                (select distinct sql_id from sash_hour_sqlid t
	                 where not exists (select 1 from sash_sqltxt psql where t.sql_id = psql.sql_id and psql.dbid = :2))
	                ) where rn=1';
    execute immediate sql_stat using l_dbid, l_dbid;
exception
    when others then
        sash_repo.log_message('get_sqltxt', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20105, 'SASH get_sqltxt error ' || SUBSTR(SQLERRM, 1 , 1000));
end get_sqltxt;


PROCEDURE collect_ash(v_sleep number, loops number, vinstance number) is
          sash_rec sash%rowtype;
          TYPE SashcurTyp IS REF CURSOR;
          sash_cur   SashcurTyp;
          l_dbid number;
          cur_sashseq   number := 0;
          sql_stat varchar2(4000);
          no_host EXCEPTION;
      PRAGMA EXCEPTION_INIT(no_host, -12543);

          begin

            l_dbid:=get_dbid;
            sql_stat := 'select a.*, 1 sample_id, null terminal, null inst_id from sys.sashnow a';

            for i in 1..loops loop
              select  sashseq.nextval into cur_sashseq from dual;

              insert /*+ append */ into sash
				   (DBID,
                    SAMPLE_TIME,
                    SESSION_ID,
                    SESSION_STATE,
                    SESSION_SERIAL#,
				    OSUSER,
                    SESSION_TYPE  ,
                    USER_ID,
                    COMMAND,
                    MACHINE,
                    PORT,
                    SQL_ADDRESS,
                    SQL_PLAN_HASH_VALUE,
                    SQL_CHILD_NUMBER,
                    SQL_ID,
                    SQL_OPCODE  /* aka SQL_OPCODE */,
                    SQL_EXEC_START,
                    SQL_EXEC_ID,
                    PLSQL_ENTRY_OBJECT_ID,
                    PLSQL_ENTRY_SUBPROGRAM_ID,
                    PLSQL_OBJECT_ID,
                    PLSQL_SUBPROGRAM_ID,
                    EVENT# ,
                    SEQ#        /* xksuse.ksuseseq */,
                    P1          /* xksuse.ksusep1  */,
                    P2          /* xksuse.ksusep2  */,
                    P3          /* xksuse.ksusep3  */,
                    WAIT_TIME   /* xksuse.ksusetim */,
                    TIME_WAITED   /* xksuse.ksusewtm */,
                    CURRENT_OBJ#,
                    CURRENT_FILE#,
                    CURRENT_BLOCK#,
                    CURRENT_ROW#,
                    PROGRAM,
                    MODULE,
                    MODULE_HASH,  /* ASH collects string */
                    ACTION,
                    ACTION_HASH,   /* ASH collects string */
                    LOGON_TIME,
                    ksuseblocker,
                    SERVICE_NAME,
                    FIXED_TABLE_SEQUENCE, /* FIXED_TABLE_SEQUENCE */
                    QC,
                    BLOCKING_INSTANCE,
                    BLOCKING_SESSION,
                    FINAL_BLOCKING_INSTANCE,
                    FINAL_BLOCKING_SESSION,
                    SAMPLE_ID,
                    inst_id)
              select a.DBID,
                     a.sample_time,
                     a.SESSION_ID,
                     a.SESSION_STATE,
                     a.SESSION_SERIAL#,
					 a.OSUSER,
                     a.SESSION_TYPE  ,
                     a.USER_ID,
                     a.COMMAND,
                     a.MACHINE,
                     a.PORT,
                     a.SQL_ADDRESS,
                     a.SQL_PLAN_HASH_VALUE,
                     a.SQL_CHILD_NUMBER,
                     a.SQL_ID ,
                     a.SQL_OPCODE  /* aka SQL_OPCODE */,
                     a.SQL_EXEC_START,
                     a.SQL_EXEC_ID,
                     a.PLSQL_ENTRY_OBJECT_ID,
                     a.PLSQL_ENTRY_SUBPROGRAM_ID,
                     a.PLSQL_OBJECT_ID,
                     a.PLSQL_SUBPROGRAM_ID,
                     a.EVENT# ,
                     a.SEQ#        /* xksuse.ksuseseq */,
                     a.P1          /* xksuse.ksusep1  */,
                     a.P2          /* xksuse.ksusep2  */,
                     a.P3          /* xksuse.ksusep3  */,
                     a.WAIT_TIME   /* xksuse.ksusetim */,
                     a.TIME_WAITED   /* xksuse.ksusewtm */,
                     a.CURRENT_OBJ#,
                     a.CURRENT_FILE#,
                     a.CURRENT_BLOCK#,
                     a.CURRENT_ROW#,
                     a.PROGRAM,
                     a.MODULE,
                     a.MODULE_HASH,  /* ASH collects string */
                     a.action,
                     a.ACTION_HASH,   /* ASH collects string */
                     a.LOGON_TIME,
                     a.ksuseblocker,
                     a.SERVICE_NAME,
                     a.FIXED_TABLE_SEQUENCE, /* FIXED_TABLE_SEQUENCE */
                     a.QC,
                     a.BLOCKING_INSTANCE,
                     a.BLOCKING_SESSION,
                     a.FINAL_BLOCKING_INSTANCE,
                     a.FINAL_BLOCKING_SESSION,
                     cur_sashseq,
                     -- vinstance
                     a.inst_id
              from sys.sashnow a;
              commit;
              dbms_lock.sleep(v_sleep);
            end loop;
exception
    when no_host then
    sash_repo.log_message('collect_ash', 'can access database ' || SUBSTR(SQLERRM, 1 , 800),'W');
    when others then
        sash_repo.log_message('collect_ash', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20106, 'SASH collect error ' || SUBSTR(SQLERRM, 1 , 1000));
end collect_ash;

procedure collect_io_event(v_hist_samp_id number) is
type sash_io_system_event_type is table of sash_io_system_event%rowtype;
io_event_rec sash_io_system_event_type;
sql_stat varchar2(4000);
TYPE SashcurTyp IS REF CURSOR;
sash_cur   SashcurTyp;
l_dbid number;

begin
    l_dbid:=get_dbid;
    sql_stat := 'select :1,inst_id,:2,sysdate,total_waits,total_timeouts,time_waited,average_wait,time_waited_micro,event_id
                 from sys.gv_$system_event where event in (''log file sync'',''log file parallel write'',''db file scattered read'',''db file sequential read'',''direct path read''
                ,''direct path read temp'',''direct write'',''direct write temp'')';
    open sash_cur FOR sql_stat using l_dbid, v_hist_samp_id;
    fetch sash_cur bulk collect into io_event_rec;
    forall i in 1..io_event_rec.count
        insert into sash_io_system_event values io_event_rec(i);
    --commit;
    close sash_cur;
exception
    when others then
        sash_repo.log_message('collect_io_event', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20107, 'SASH collect_io_event error ' || SUBSTR(SQLERRM, 1 , 1000));
end collect_io_event;

procedure collect_metric(v_hist_samp_id number) is
type sash_sysmetric_history_type is table of sash_sysmetric_history%rowtype;
session_rec sash_sysmetric_history_type;
sql_stat varchar2(4000);
TYPE SashcurTyp IS REF CURSOR;
sash_cur   SashcurTyp;
l_dbid number;
l_time date;
l_ver sash_targets.version%type;

begin
    l_dbid:=get_dbid;
    l_ver:=get_version;

    select nvl(max(BEGIN_TIME),sysdate-30) into l_time from sash_sysmetric_history where dbid = l_dbid;
    dbms_output.put_line(l_dbid || ' ' || l_time);


    if (substr(l_ver,1,2) > '11' and is_pdb='Y') then
    	sql_stat := 'select  :1, inst_id, :2, BEGIN_TIME, INTSIZE_CSEC, GROUP_ID, METRIC_ID, VALUE from sys.gv_$CON_SYSMETRIC_HISTORY ss where begin_time > :3 and INTSIZE_CSEC > 2000 and metric_id in (select METRIC_ID from sash_sysmetric_names)';
    else
    	sql_stat := 'select  :1, inst_id, :2, BEGIN_TIME, INTSIZE_CSEC, GROUP_ID, METRIC_ID, VALUE from sys.gv_$SYSMETRIC_HISTORY ss where begin_time > :3 and INTSIZE_CSEC > 2000 and metric_id in (select METRIC_ID from sash_sysmetric_names)';
	end if;
    dbms_output.put_line(sql_stat || ' ' || l_time);
    open sash_cur FOR sql_stat using l_dbid, v_hist_samp_id, l_time;
    fetch sash_cur bulk collect into session_rec;
    forall i in 1..session_rec.count
        insert into sash_sysmetric_history values session_rec(i);
    --commit;
    close sash_cur;
exception
    when others then
        sash_repo.log_message('collect_metric', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20108, 'SASH collect_metric error ' || SUBSTR(SQLERRM, 1 , 1000));
end collect_metric;


procedure collect_histogram(v_hist_samp_id number) is
type sash_event_histogram_type is table of sash_event_histogram%rowtype;
session_rec sash_event_histogram_type;
sql_stat varchar2(4000);
TYPE SashcurTyp IS REF CURSOR;
sash_cur   SashcurTyp;
l_dbid number;
l_time date;

begin
    l_dbid:=get_dbid;
    sql_stat := 'select  :1, :2, inst_id, EVENT#, WAIT_TIME_MILLI, WAIT_COUNT from sys.gv_$event_histogram';
    open sash_cur FOR sql_stat using v_hist_samp_id, l_dbid;
    fetch sash_cur bulk collect into session_rec;
    forall i in 1..session_rec.count
        insert into sash_event_histogram values session_rec(i);
    --commit;
    close sash_cur;
exception
    when others then
        sash_repo.log_message('collect_histogram', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20108, 'SASH collect_histogram error ' || SUBSTR(SQLERRM, 1 , 1000));
end collect_histogram;


procedure collect_iostat(v_hist_samp_id number) is
	type sash_iofuncstats_type is table of sash_iofuncstats%rowtype;
	session_rec sash_iofuncstats_type;
	sql_stat varchar2(4000);
	TYPE SashcurTyp IS REF CURSOR;
	sash_cur   SashcurTyp;
	l_dbid number;

begin
    l_dbid:=get_dbid;
    sql_stat := 'select  :1, inst_id, :2, FUNCTION_ID, FUNCTION_NAME, SMALL_READ_MEGABYTES, SMALL_WRITE_MEGABYTES, LARGE_READ_MEGABYTES, LARGE_WRITE_MEGABYTES, SMALL_READ_REQS,
                 SMALL_WRITE_REQS, LARGE_READ_REQS, LARGE_WRITE_REQS, NUMBER_OF_WAITS , WAIT_TIME from sys.v_$iostat_function ss';
    open sash_cur FOR sql_stat using l_dbid, v_hist_samp_id;
    fetch sash_cur bulk collect into session_rec;
    forall i in 1..session_rec.count
        insert into sash_iofuncstats values session_rec(i);
    commit;
    close sash_cur;
exception
    when others then
        sash_repo.log_message('collect_iostat', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20109, 'SASH collect_iostat error ' || SUBSTR(SQLERRM, 1 , 1000));
end collect_iostat;

procedure collect_osstat(v_hist_samp_id number) is
	type sash_osstat_type is table of sash_osstat%rowtype;
	session_rec sash_osstat_type;
	sql_stat varchar2(4000);
	TYPE SashcurTyp IS REF CURSOR;
	sash_cur   SashcurTyp;
	l_dbid number;

begin
    l_dbid:=get_dbid;
    sql_stat := 'select :1, :2, inst_id, OSSTAT_ID, VALUE from sys.gv_$osstat';
    open sash_cur FOR sql_stat using v_hist_samp_id, l_dbid;
    fetch sash_cur bulk collect into session_rec;
    forall i in 1..session_rec.count
        insert into sash_osstat values session_rec(i);
    commit;
    close sash_cur;
exception
    when others then
        sash_repo.log_message('collect_osstat', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20109, 'SASH collect_osstat error ' || SUBSTR(SQLERRM, 1 , 1000));
end;

procedure collect_systime(v_hist_samp_id number) is
	type sash_sys_time_model_type is table of sash_sys_time_model%rowtype;
	session_rec sash_sys_time_model_type;
	sql_stat varchar2(4000);
	TYPE SashcurTyp IS REF CURSOR;
	sash_cur   SashcurTyp;
	l_dbid number;

begin
    l_dbid:=get_dbid;
    sql_stat := 'select :1, :2, inst_id, STAT_ID, VALUE from sys.gv_$sys_time_model';
    open sash_cur FOR sql_stat using v_hist_samp_id, l_dbid;
    fetch sash_cur bulk collect into session_rec;
    forall i in 1..session_rec.count
        insert into sash_sys_time_model values session_rec(i);
    commit;
    close sash_cur;
exception
    when others then
        sash_repo.log_message('collect_sys_time', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20109, 'SASH collect_sys_time error ' || SUBSTR(SQLERRM, 1 , 1000));
end;

procedure collect_stats is
type sash_instance_stats_type is table of sash_instance_stats%rowtype;
session_rec sash_instance_stats_type;
sql_stat varchar2(4000);
TYPE SashcurTyp IS REF CURSOR;
sash_cur   SashcurTyp;
l_dbid number;

begin
    l_dbid:=get_dbid;
    sql_stat := 'select /*+DRIVING_SITE(ss) */ ' || l_dbid || ' , inst_id, sysdate, statistic#, value from sys.gv_$sysstat ss where statistic# in (select sash_s.statistic# from sash_stats sash_s where collect = 1)';
    open sash_cur FOR sql_stat;
    fetch sash_cur bulk collect into session_rec;
    forall i in 1..session_rec.count
        insert into sash_instance_stats values session_rec(i);
    commit;
    close sash_cur;
exception
    when others then
        sash_repo.log_message('collect_stats', SUBSTR(SQLERRM, 1 , 1000),'E');
        RAISE_APPLICATION_ERROR(-20110, 'SASH collect_stats error ' || SUBSTR(SQLERRM, 1 , 1000));
end collect_stats;

procedure collect_other(v_sleep number, loops number) is
type sash_instance_stats_type is table of sash_instance_stats%rowtype;
session_rec sash_instance_stats_type;
sql_stat varchar2(4000);
TYPE SashcurTyp IS REF CURSOR;
sash_cur   SashcurTyp;
l_dbid number;

begin
    for l in 1..loops loop
        collect_stats;
        collect_io_event(1);
        commit;
        dbms_lock.sleep(v_sleep);
    end loop;
end collect_other;


PROCEDURE get_one(v_sql_id varchar2) is
   l_hist_samp_id	number;
   l_dbid number;
   v_startup_time date;
begin
   select hist_id_seq.currval into l_hist_samp_id from dual;
   l_dbid:=get_dbid;
   get_sqlid(l_dbid,v_sql_id);
   get_sqltxt(l_dbid);
   get_sqlstats(l_hist_samp_id, l_dbid);
   get_sqlplans(l_hist_samp_id, l_dbid);
   -- select startup_time into v_startup_time from sys.v_$instance;
   -- insert into sash_hist_sample (hist_sample_id, dbid, instance_number, hist_date, startup_time) values (l_hist_samp_id, l_dbid, v_inst_num, sysdate, v_startup_time);

   insert into sash_hist_sample (hist_sample_id, dbid, instance_number, hist_date, startup_time)
	   select l_hist_samp_id, l_dbid, i.inst_id, sysdate, i.startup_time
	   from sys.gv_$instance i;

   commit;
end get_one;

PROCEDURE get_all is
 l_hist_samp_id	number;
 l_dbid number;
 l_ver varchar2(8);
 v_startup_time date;
 v_last_date    date;
begin
   select hist_id_seq.nextval into l_hist_samp_id from dual;
   l_ver:=substr(sash_pkg.get_version,0,2);
   l_dbid:=get_dbid;
   get_users;
   get_data_files;
   get_sqlids(l_dbid);
   get_sqltxt(l_dbid);
   get_sqlstats(l_hist_samp_id, l_dbid);
   get_sqlplans(l_hist_samp_id, l_dbid);
   get_objs(l_dbid);
   collect_metric(l_hist_samp_id);


   insert into sash_hist_sample (hist_sample_id, dbid, instance_number, last_date, hist_date, startup_time)
	   select l_hist_samp_id, l_dbid, i.inst_id, greatest(nvl(h.hist_date, i.startup_time), i.startup_time), sysdate, i.startup_time
	   from sys.gv_$instance i,
	        (select instance_number as inst_id, max(hist_date) as hist_date from sash_hist_sample where dbid=l_dbid group by instance_number) h
	   where i.inst_id = h.inst_id(+);

   commit;
end get_all;

END sash_pkg;
/


show errors
spool off
