@echo off
cd C:\kafka

echo Creando topicos para EVCharging...

call .\bin\windows\kafka-topics.bat --create --topic SUPPLY_REQUEST --bootstrap-server 192.168.18.148:9092
call .\bin\windows\kafka-topics.bat --create --topic SUPPLY_RESPONSE --bootstrap-server 192.168.18.148:9092
call .\bin\windows\kafka-topics.bat --create --topic SUPPLY_COMPLETED --bootstrap-server 192.168.18.148:9092
call .\bin\windows\kafka-topics.bat --create --topic SUPPLY_PROGRESS --bootstrap-server 192.168.18.148:9092
call .\bin\windows\kafka-topics.bat --create --topic SUPPLY_DIRECT_START --bootstrap-server 192.168.18.148:9092
call .\bin\windows\kafka-topics.bat --create --topic DRIVER_STATUS --bootstrap-server 192.168.18.148:9092
call .\bin\windows\kafka-topics.bat --create --topic CP_STATUS --bootstrap-server 192.168.18.148:9092
call .\bin\windows\kafka-topics.bat --create --topic CP_REGISTRY --bootstrap-server 192.168.18.148:9092
call .\bin\windows\kafka-topics.bat --create --topic CP_COMMAND --bootstrap-server 192.168.18.148:9092

call .\bin\windows\kafka-topics.bat --list --bootstrap-server 192.168.18.148:9092

pause