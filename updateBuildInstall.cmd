@echo off
setlocal
set DIR=%~dp0%
set APPS_DIR=%DIR%\..\..\apps64
set PATH=%APPS_DIR%\7-Zip;%PATH%

cd %DIR%

call gitUpdate.cmd --batch

call build.cmd --batch

call cmd\activateVersion.cmd MAJOR-SNAPSHOT --batch

if NOT "%1" == "--batch" (
  pause
)

endlocal
exit /b
