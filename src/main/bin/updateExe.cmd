@echo off
set PATH=%PATH%;C:\Apps\winrun4j\bin
set WR_EXE=C:\Development\ALL\com.revolsys.winrun4j\target\nar\WinRun4j-1.0.0-SNAPSHOT-amd64-Windows-msvc-executable\bin\amd64-Windows-msvc\WinRun4j.exe

copy %WR_EXE% GbaSiteTools.exe
RCEDIT64.exe /C GbaSiteTools.exe
RCEDIT64.exe /I GbaSiteTools.exe GbaSiteTools.ico
RCEDIT64.exe /A GbaSiteTools.exe GbaSiteTools.ico
RCEDIT64.exe /N GbaSiteTools.exe GbaSiteTools.ini
RCEDIT64.exe /S GbaSiteTools.exe GbaSiteToolsSplash.gif
RCEDIT64.exe /L GbaSiteTools.exe
pause

