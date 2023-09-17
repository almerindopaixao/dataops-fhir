@echo off

FOR /F "tokens=1" %%i IN ('docker ps ^| findstr fhir-server') DO docker logs %%i
