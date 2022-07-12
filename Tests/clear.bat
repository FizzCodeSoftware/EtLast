@echo off

echo Deleting bin directories...
rem for /f %%X in ('dir /s /b bin') do (rmdir /q /s %%X)
FOR /F "tokens=*" %%G IN ('DIR /B /AD /S bin') DO RMDIR /S /Q "%%G"

echo Deleting obj directories...
rem for /f %%X in ('dir /s /b obj') do (rmdir /q /s %%X)
FOR /F "tokens=*" %%G IN ('DIR /B /AD /S obj') DO RMDIR /S /Q "%%G"

echo Deleting TestResults directory...
rmdir /q /s TestResults

echo Ready.

pause