#!/bin/bash
DB=$1
# NOTE psql returns a space in front of the results so all compares have a space in front e.g " gba"
if [ "$PG_PORT" == "" ]; then
  export PG_PORT=5432
fi
if [ "$DB" == "" ]; then
  export DB=gba
fi

RESULT=`psql -q -h localhost -p $PG_PORT -U postgres -d postgres --tuples-only --command "SELECT rolname FROM pg_roles WHERE rolname = 'gba_user';"`
if [ " gba_user" == "$RESULT" ]; then
  echo INFO: Role gba_user exists so no need to create again
else
  createuser -h localhost -p $PG_PORT -U postgres --no-login --no-superuser --no-createdb --no-createrole gba_user
  echo INFO: Created role gba_user
fi

RESULT=`psql -q -h localhost -p $PG_PORT -U postgres -d postgres --tuples-only --command "SELECT rolname FROM pg_roles WHERE rolname = 'gba_viewer';"`
if [ " gba_viewer" == "$RESULT" ]; then
  echo INFO: Role gba_viewer exists so no need to create again
else
  createuser -h localhost -p $PG_PORT -U postgres --no-login --no-superuser --no-createdb --no-createrole gba_viewer
  echo INFO: Created role gba_viewer
fi

RESULT=`psql -q -h localhost -p $PG_PORT -U postgres -d postgres --tuples-only --command "SELECT rolname FROM pg_roles WHERE rolname = 'gba';"`
if [ " gba" == "$RESULT" ]; then
  echo INFO: User gba exists so no need to create again
else
  echo CREATE ROLE gba LOGIN PASSWORD \'gba2011\' CREATEDB INHERIT NOCREATEROLE IN ROLE GBA_USER | psql -q -h localhost -p $PG_PORT -U postgres 
  echo INFO: Created user gba
fi

RESULT=`psql -q -h localhost -p $PG_PORT -U postgres -d postgres --tuples-only --command "SELECT rolname FROM pg_roles WHERE rolname = 'proxy_gba';"`
if [ " proxy_gba" == "$RESULT" ]; then
  echo INFO: User proxy_gba exists so no need to create again
else
  echo CREATE ROLE proxy_gba LOGIN PASSWORD \'gba2011\' CREATEDB INHERIT NOCREATEROLE IN ROLE GBA_USER | psql -q -h localhost -p $PG_PORT -U postgres 
  echo INFO: Created user proxy_gba
fi

RESULT=`psql -q -h localhost -p $PG_PORT -U postgres -d postgres --tuples-only --command "SELECT spcname FROM pg_tablespace WHERE spcname = 'gba';"`
if [ " gba" == "$RESULT" ]; then
  echo INFO: Tablespace gba exists so no need to create again
else
  echo Enter the full directory for the GBA tablespace e.g /apps_data/postgres/gba
  read GBA_TBS
  if [ ! -e $GBA_TBS ]; then
    mkdir -p $GBA_TBS
  fi
  sudo chown postgres $GBA_TBS
  echo CREATE TABLESPACE gba OWNER gba LOCATION \'${GBA_TBS}\' | psql -h localhost -p $PG_PORT -U postgres -d postgres
  echo INFO: Created tablespace gba
fi

RESULT=`psql -q -h localhost -p $PG_PORT -U postgres -d postgres --tuples-only --command "SELECT datname FROM pg_database WHERE datname = '$DB';"`
if [ " $DB" == "$RESULT" ]; then
  echo "Database exists, are you sure you want to erase all the data (YES/NO)?"
  read DROP_DB
  if [ "$DROP_DB" == "YES" ]; then
    dropdb -h localhost -p $PG_PORT -U postgres $DB
    if [ "$?" != "0" ]; then
      echo ERROR: Cannot delete database
      exit
    fi
  else
    echo ERROR: Database deletion cancelled by user input
    exit
  fi
fi

createdb -h localhost -p $PG_PORT -U postgres --template postgres --tablespace gba --owner=gba $DB
echo CREATE EXTENSION postgis | psql -h localhost -p $PG_PORT -U postgres -d $DB

if [ "$?" != "0" ]; then
  echo ERROR: CANNOT CREATE THE DATABASE
else
  echo INFO: Creating tables
  psql -h localhost -p $PG_PORT -U postgres -d $DB -f gba-dba-perms.sql > gba-perms.log 2>&1 
  psql -h localhost -p $PG_PORT -U gba -d $DB -f gba-ddl-all.sql > gba-ddl.log 2>&1
  grep FATAL gba-ddl.log gba-perms.log
  grep ERROR gba-ddl.log gba-perms.log
  echo INFO: Tables created if no ERRORS above
fi
