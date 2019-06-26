@echo off
setlocal
set PATH=e:\sw_nt\cygwin\bin\;C:\Apps\cygwin\bin;%PATH%
SET VERSION=%1
IF [%VERSION%] EQU [] set /p VERSION=Enter Version (e.g. 1.0.1-RELEASE): 
IF [%VERSION%] EQU [] SET VERSION=%LATEST_VERSION%
set DIR=%~dp0
set DIR=%DIR:\=/%
set DIR=/cygdrive/%DIR::=%
bash -c "cd `cygpath %DIR%/..`; curl -O https://open.revolsys.com/artifactory/repo/ca/bc/gov/gba/gba-sites/%VERSION%/gba-sites-%VERSION%-bin.zip"
echo Downloaded %VERSION%

if NOT "%1" == "--batch" (
  pause
)
endlocal
