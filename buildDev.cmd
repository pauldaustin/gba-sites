@echo off
setlocal
C:
cd C:\Development\GBA
cd com.revolsys.open
call mvn -Dmaven.javadoc.skip=true -Dsources.skip=true -Dmaven.test.skip=true clean install
cd ..\ca.bc.gov.gba
call mvn -Dmaven.javadoc.skip=true -Dsources.skip=true -Dmaven.test.skip=true clean install

cd C:\Apps\gba\versions\
rd /s /q gba-GBA-SNAPSHOT-bin
7z x C:\Development\GBA\ca.bc.gov.gba\target\gba-GBA-SNAPSHOT-bin.zip -Ogba-GBA-SNAPSHOT-bin
pause
