@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "JAR_PATH=%SCRIPT_DIR%bilibili-subtitle-export.jar"

if not exist "%JAR_PATH%" (
  echo jar not found: %JAR_PATH% 1>&2
  echo run .\build-bundle.bat first 1>&2
  exit /b 1
)

java -jar "%JAR_PATH%" %*
exit /b %errorlevel%
