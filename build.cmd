@echo off
setlocal
set DIR=%~dp0%
set APPS_DIR=%DIR%\..\..\apps64
set JAVA_HOME=%APPS_DIR%\jdk-11
set PATH=%JAVA_HOME%\bin;%PATH
set PATH=%APPS_DIR%\PortableGit\cmd;%PATH%
set PATH=%APPS_DIR%\7-Zip;%PATH%
set PATH=%APPS_DIR%\maven\bin;%PATH%

set BASE_NAME=gba-sites-MAJOR-SNAPSHOT-bin

cd %DIR%\..

if not exist log mkdir log

call cmd\gitUpdate.cmd --batch

rem -------------------------------------

call :mvnBuild com.revolsys.open

call :mvnBuild gba

call :mvnBuild gba-sites

rem -------------------------------------

IF %ERRORLEVEL% == 0 (
  echo Install

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

rem -- mvnBuild     -----------------
:mvnBuild
setlocal

set DIR=%1

echo Build %DIR%
call mvn -settings %APPS_DIR%\settings.xml -f src\%DIR --log-file %DIR%\log\%DIR.log -B -Dmaven.javadoc.skip=true -Dsources.skip=true -DskipTests clean install
echo(

endlocal
exit /b
rem -- mvnBuild END -----------------
