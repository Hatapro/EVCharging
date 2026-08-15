@echo off
if exist "C:\kafka\logs" (
    rmdir /s /q "C:\kafka\logs"
    echo Carpeta C:\kafka\logs eliminada.
) else (
    echo No existe la carpeta C:\kafka\logs.
)

cd C:\kafka

echo Generando UUID...
for /f "tokens=*" %%i in ('powershell -Command "[guid]::NewGuid().ToString()"') do set UUID=%%i

echo UUID generado: %UUID%
echo.

echo Formateando logs con UUID...
.\bin\windows\kafka-storage.bat format -t %UUID% -c .\config\server.properties