@echo off
setlocal
set DIR=%~dp0%
set PATH=\apps64\PortableGit\cmd;%PATH%

cd %DIR%\..

if not exist src (
  mkdir src
)
cd src

rem ----------------

call gitUpdate com.revolsys.open revolsys/gba master

call gitUpdate gba pauldaustin/ca.bc.gov.gba major

call gitUpdate gba-sites pauldaustin/gba-sites master

rem ----------------

cd ..\..

echo Git Update GBA Config
git -C config pull
echo(

if NOT "%1" == "--batch" (
  pause
)

exit /b

rem -- git update     -----------------
:git_update
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

