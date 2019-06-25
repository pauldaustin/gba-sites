@echo off
set VERSION=%1

set VERSION_DIR=gba-%VERSION%-bin

set TEST_VERSION_DIR=I:\versions\%VERSION_DIR%\

IF EXIST %TEST_VERSION_DIR% (
  echo Deleting %TEST_VERSION_DIR%
  rmdir /S /Q %TEST_VERSION_DIR%
)
echo Installing %TEST_VERSION_DIR%
mkdir %TEST_VERSION_DIR%
xcopy /s /e /q %VERSION_DIR% %TEST_VERSION_DIR%
echo Activating%TEST_VERSION_DIR%
xcopy /q /y J:\*.ini I:\

set PROD_VERSION_DIR=K:\versions\%VERSION_DIR%\
IF EXIST %PROD_VERSION_DIR% (
  echo Deleting %PROD_VERSION_DIR%
  rmdir /S /Q %PROD_VERSION_DIR%
)
echo Installing %PROD_VERSION_DIR%
mkdir %PROD_VERSION_DIR%
xcopy /s /e /q %VERSION_DIR% %PROD_VERSION_DIR%
