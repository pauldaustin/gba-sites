@echo off
setlocal
set DIR=%~dp0%
set LOG=%DIR%\log\build.log
set JAVA_HOME=..\apps64\jdk-11
set PATH=%JAVA_HOME%\bin;%PATH%;..\apps64\7-Zip;..\apps64\maven\bin
set BASE_NAME=gba-GBA-SNAPSHOT-bin

cd %DIR%

if not exist log mkdir log

call gitUpdate.cmd --batch

echo Build
call mvn -settings U:\maven\settings.xml -f src\com.revolsys.open --log-file %DIR%\log\build-rs.log -B -Dmaven.javadoc.skip=true -Dsources.skip=true -DskipTests clean install
call mvn -settings U:\maven\settings.xml -f src\gba --log-file %DIR%\log\build-gba.log -B -Dmaven.javadoc.skip=true -Dsources.skip=true -DskipTests clean install
echo(

IF %ERRORLEVEL% == 0 (
  echo Install

  if exist %BASE_NAME% rmdir /s /q %BASE_NAME%
  7z x src\gba\target\%BASE_NAME%.zip -o%BASE_NAME% > %DIR%\log\7z.log
) ELSE (
  echo "*** BUILD FAILED check %LOG% ****"
)
call activateVersion.cmd GBA-SNAPSHOT --batch
pause
