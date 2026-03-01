@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
set "JAR_PATH=%PROJECT_ROOT%\bilibili-subtitle-export\target\bilibili-subtitle-export-1.0-SNAPSHOT-jar-with-dependencies.jar"

pushd "%PROJECT_ROOT%" || exit /b 1
mvn -pl bilibili-subtitle-export -am -DskipTests package
if errorlevel 1 (
  popd
  exit /b 1
)
popd

if not exist "%JAR_PATH%" (
  echo executable jar not found: %JAR_PATH% 1>&2
  exit /b 1
)

java -jar "%JAR_PATH%" %*
exit /b %errorlevel%
