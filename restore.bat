@echo off

if [%1]==[]  (
  echo usage: restore [database] 
  goto end 
)

echo restoring database %1

pg_restore --jobs=10 --host=localhost --port=5432 --dbname=postgres --create --clean --if-exists --username=postgres %1
              
:end

