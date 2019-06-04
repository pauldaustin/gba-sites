@echo off
set PATH=%PATH%;C:\Apps\winrun4j\bin
set WR_EXE=C:\Development\ALL\com.revolsys.winrun4j\target\nar\WinRun4j-1.0.0-SNAPSHOT-amd64-Windows-msvc-executable\bin\amd64-Windows-msvc\WinRun4j.exe

copy %WR_EXE% "BC GIS.exe"
RCEDIT64.exe /C "BC GIS.exe"
RCEDIT64.exe /I "BC GIS.exe" "BC GIS.ico"
RCEDIT64.exe /A "BC GIS.exe" "BC GIS.ico"
RCEDIT64.exe /N "BC GIS.exe" "BC GIS.ini"
RCEDIT64.exe /S "BC GIS.exe" "BC GISSplash.gif"
RCEDIT64.exe /L "BC GIS.exe"

copy %WR_EXE% GbaUi.exe
RCEDIT64.exe /C GbaUi.exe
RCEDIT64.exe /I GbaUi.exe GbaUi.ico
RCEDIT64.exe /A GbaUi.exe GbaUi.ico
RCEDIT64.exe /N GbaUi.exe GbaUi.ini
RCEDIT64.exe /S GbaUi.exe GbaUiSplash.gif
RCEDIT64.exe /L GbaUi.exe

copy %WR_EXE% GbaTools.exe
RCEDIT64.exe /C GbaTools.exe
RCEDIT64.exe /I GbaTools.exe GbaTools.ico
RCEDIT64.exe /A GbaTools.exe GbaTools.ico
RCEDIT64.exe /N GbaTools.exe GbaTools.ini
RCEDIT64.exe /S GbaTools.exe GbaToolsSplash.gif
RCEDIT64.exe /L GbaTools.exe
pause

