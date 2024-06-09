-- (c) Kyle Hailey 2007
-- (c) Marcin Przepiorowski 2010
-- v2.1 Changes: add password and tablespace prompt, add new privileges to sash user on repository
-- v2.2 Changes: add schema owner as a variable, display more information
-- v2.3
-- v2.4
 
 set ver off

-- prompt Are you connected as the SYS user? 
-- accept toto prompt "If you are not the SYS user hit Control-C , else Return : "

--		accept SASH_USER default sash prompt "Enter user name (schema owner) [or enter to accept username sash] ? " 
--		accept SASH_PASS default sash prompt "Enter user password ? "
--		accept SASH_TS default users prompt "Enter SASH user default tablespace [or enter to accept USERS tablespace] ? "
--		prompt SASH default tablespace is: &SASH_TS
--		
--		prompt "------------------------------------------------------------------------------------"
--		prompt Existing &SASH_USER user will be deleted.
--		accept toto prompt "If you are not sure hit Control-C , else Return : "
--		prompt "------------------------------------------------------------------------------------"
--		
--		drop user &SASH_USER cascade;
--		
--		prompt New &SASH_USER user will be created.
--		
--		WHENEVER SQLERROR EXIT 
--		create user &SASH_USER identified by &SASH_PASS default tablespace &SASH_TS;
--		alter user &SASH_USER quota unlimited on &SASH_TS;

grant connect, resource to sash;

grant ANALYZE ANY                 to sash;
grant CREATE TABLE                to sash;
grant ALTER SESSION               to sash;
grant CREATE SEQUENCE             to sash;
grant CREATE DATABASE LINK        to sash;
grant UNLIMITED TABLESPACE        to sash;
grant CREATE PUBLIC DATABASE LINK to sash;
grant create view                 to sash;
grant create public synonym       to sash;
grant execute on dbms_lock        to sash;
grant Create job                  to sash;
grant manage scheduler            to sash;
