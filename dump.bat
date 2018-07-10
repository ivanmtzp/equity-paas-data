@echo off

if [%1]==[]  (
  echo usage: dump [database] 
  goto end 
)

echo dumping database %1

IF EXIST %1 (
   rmdir /S /Q %1
   timeout /t 4 /nobreak > NUL   
)

pg_dump --format=directory --file=%1 --jobs=10 --verbose --compress=9 --dbname=%1 --host=localhost --port=5432 --username=postgres
              
:end

