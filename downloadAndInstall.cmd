call download.cmd
call install.cmd %VERSION%
call activateVersion.cmd %VERSION% --batch
call copyTestProd.cmd %VERSION%

pause
