@echo off
SET VERSION=%1
for /f %%f in ('dir /o:n /b gba-*-bin.zip') do (
  set LATEST_VERSION=%%f
)
SET LATEST_VERSION=%LATEST_VERSION:gba-=%
SET LATEST_VERSION=%LATEST_VERSION:-bin.zip=%
IF [%VERSION%] EQU [] set /p VERSION=Enter Version (%LATEST_VERSION%):
IF [%VERSION%] EQU [] SET VERSION=%LATEST_VERSION%
set VERSION_DIR=gba-%VERSION%-bin
set VERSION_ZIP=%VERSION_DIR%.zip
IF EXIST %VERSION_ZIP% (
  IF EXIST %VERSION_DIR% (
    echo Deleting %CD%\%VERSION_DIR%
    rmdir /S /Q %VERSION_DIR%
  )
  echo Installing %CD%\%VERSION_DIR%
  ..\apps64\7-Zip\7z x %VERSION_ZIP% -o%VERSION_DIR%
  echo Installed %CD%\%VERSION_DIR%
) ELSE (
  echo Cannot find %CD%\%VERSION_ZIP%
)
