@echo off
setlocal
set DIR=%~dp0%
set APPS_DIR=%DIR%\..\..\apps64
set JAVA_HOME=%APPS_DIR%\jdk-11
set PATH=%JAVA_HOME%\bin;%PATH%
set PATH=%APPS_DIR%\maven\bin;%PATH%
set PATH=%APPS_DIR%\7-zip;%PATH%

cd %DIR%\..

if not exist log mkdir log

rem -------------------------------------

call :mvnBuild jeometry

call :mvnBuild com.revolsys.open

call :mvnBuild gba

call :mvnBuild gba-sites


call %DIR%\install.cmd MAJOR-SNAPSHOT %1

rem -------------------------------------

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
call mvn -settings %APPS_DIR%\settings.xml -f src\%DIR% --log-file %DIR%\log\%DIR.log -B -Dmaven.javadoc.skip=true -Dsources.skip=true -DskipTests install
echo(

endlocal
exit /b
rem -- mvnBuild END -----------------
