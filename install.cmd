@echo off
setlocal
set DIR=%~dp0%

set APPS_DIR=%DIR%\..\..\apps64
set PATH=%APPS_DIR%\7-zip;%PATH%

cd %DIR%\..
SET VERSION=%1
IF [%VERSION%] EQU [] (
  for /f %%f in ('dir /o:n /b gbasites-*-bin.zip') do (
    set LATEST_VERSION=%%f
  )
  SET LATEST_VERSION=%LATEST_VERSION:gbasites-=%
  SET LATEST_VERSION=%LATEST_VERSION:-bin.zip=%
  set /p VERSION="Enter Version (%LATEST_VERSION%):"
  IF [%VERSION%] EQU [] SET VERSION=%LATEST_VERSION%
)
set VERSION_DIR=gba-sites-%VERSION%-bin
set VERSION_ZIP=%VERSION_DIR%.zip
IF EXIST %VERSION_ZIP% (
  IF EXIST %VERSION_DIR% (
    echo Deleting %CD%\%VERSION_DIR%
    rmdir /S /Q %VERSION_DIR%
  )
  echo Installing %CD%\%VERSION_DIR%
  7z x %VERSION_ZIP% -o%VERSION_DIR%
  echo Installed %CD%\%VERSION_DIR%
) ELSE (
  echo Cannot find %CD%\%VERSION_ZIP%
)

if NOT "%2" == "--batch" (
  pause
)

endlocal
