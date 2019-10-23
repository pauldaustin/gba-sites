@echo off
set DIR=%~dp0%
set APPS_DIR=%DIR%\..\..\apps64
set PATH=%APPS_DIR%\PortableGit;%PATH%

SET VERSION=%1
IF [%VERSION%] EQU [] set /p VERSION=Enter Version (e.g. 1.0.1-RELEASE): 
IF [%VERSION%] EQU [] SET VERSION=%LATEST_VERSION%

set DIR=%DIR:\=/%
set DIR=/%DIR::=%
git-bash -c "cd `cygpath %DIR%/..`; curl -O https://open.revolsys.com/artifactory/gbasites-release-local/ca/bc/gov/gba/gba-sites/%VERSION%/gba-sites-%VERSION%-bin.zip"
echo Downloaded %VERSION%
