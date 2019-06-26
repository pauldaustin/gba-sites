@echo off
setlocal
cd %~dp0\..
SET VERSION=%1
for /f %%f in ('dir /o:n /b gba-sites-*-bin') do (
  set LATEST_VERSION=%%f
)
SET LATEST_VERSION=%LATEST_VERSION:gba-sites-=%
SET LATEST_VERSION=%LATEST_VERSION:-bin=%
IF [%VERSION%] EQU [] set /p VERSION=Enter Version (%LATEST_VERSION%):
IF [%VERSION%] EQU [] SET VERSION=%LATEST_VERSION%

set INI_DIR=gba-sites-%VERSION%-bin\ini
IF EXIST %INI_DIR% (

  copy %INI_DIR%\*.ini ..\GbaSiteTools.ini
  echo Activated %VERSION%
) ELSE (
  echo Version %VERSION% does not exist
)

if NOT "%2" == "--batch" (
  pause
)
endlocal
