@echo off
SET VERSION=%1
for /f %%d in ('dir /o:n /b /A:D') do (
  for /f %%f in ('dir /o:n /b %%d\hs_err*') do (
    IF EXIST %%d\%%f del %%d\%%f
  )
)

pause