@echo off
setlocal
set DIR=%~dp0%
set PATH=%DIR%\..\..\apps64\PortableGit\cmd;%PATH%

cd %DIR%\..

if not exist src (
  mkdir src
)

rem ----------------

call :gitUpdate cmd pauldaustin/gba-sites version-scripts

call :gitUpdate src\jeometry jeometry-org/jeometry master

call :gitUpdate src\com.revolsys.open revolsys/com.revolsys.open master

call :gitUpdate src\gba pauldaustin/gba major

call :gitUpdate src\gba-sites pauldaustin/gba-sites master

rem ----------------

call :gitUpdate ..\config revolsys/ca.bc.gov.gba.config master

if NOT "%1" == "--batch" (
  pause
)

endlocal
exit /b

rem -- git update     -----------------
:gitUpdate
setlocal

set DIR=%1
set REPO=%2
set BRANCH=%3

echo Git Update %DIR%
if exist %DIR% (
  git -C %DIR% checkout %BRANCH%
  git -C %DIR% pull
) else (
  git clone https://github.com/%REPO%.git %DIR%
  git -C %DIR% checkout %BRANCH%
)
echo(

endlocal
exit /b
rem -- git update END -----------------
