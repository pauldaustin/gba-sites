@echo off
setlocal
set DIR=%~dp0%
set APPS_DIR=%DIR%\..\..\apps64
set PATH=%APPS_DIR%\7-Zip;%PATH%

set BASE_NAME=gba-sites-MAJOR-SNAPSHOT-bin

cd %DIR%

call gitUpdate.cmd --batch

call build.cmd --batch

IF %ERRORLEVEL% == 0 (
  echo Install
  cd ..

  if exist %BASE_NAME% rmdir /s /q %BASE_NAME%
  7z x src\gba-sites\target\%BASE_NAME%.zip -o%BASE_NAME% > %DIR%\log\7z.log
) ELSE (
  echo "*** BUILD FAILED check %LOG% ****"
)

call cmd\activateVersion.cmd MAJOR-SNAPSHOT --batch

if NOT "%1" == "--batch" (
  pause
)

endlocal
exit /b
