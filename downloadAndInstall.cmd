call download.cmd
call install.cmd %VERSION% --batch
call activateVersion.cmd %VERSION% --batch
rem call copyTestProd.cmd %VERSION%

pause
