@echo off
SET VERSION=%1
for /f %%f in ('dir /o:n /b gba-*-bin') do (
  set LATEST_VERSION=%%f
)
SET LATEST_VERSION=%LATEST_VERSION:gba-sites-=%
SET LATEST_VERSION=%LATEST_VERSION:-bin=%
IF [%VERSION%] EQU [] set /p VERSION=Enter Version (%LATEST_VERSION%):
IF [%VERSION%] EQU [] SET VERSION=%LATEST_VERSION%
IF EXIST %~dp0\gba-sites-%VERSION%-bin\bin (
  copy %~dp0\gba-sites-%VERSION%-bin\ini\GbaTools.ini ..\GbaTools.ini
  copy %~dp0\gba-sites-%VERSION%-bin\ini\GbaUi.ini ..\GbaUi.ini
  copy "%~dp0\gba-sites-%VERSION%-bin\ini\Bc GIS.ini" "..\Bc GIS.ini" 
  echo Activated %VERSION%
) ELSE (
  echo Version %VERSION% does not exist
)

if NOT "%2" == "--batch" (
  pause
)
