@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
set "JAR_SOURCE=%PROJECT_ROOT%\bilibili-subtitle-export\target\bilibili-subtitle-export-1.0-SNAPSHOT-jar-with-dependencies.jar"
set "BUNDLE_DIR=%PROJECT_ROOT%\bilibili-subtitle-export\bundle"
set "RUN_TEMPLATE=%PROJECT_ROOT%\bilibili-subtitle-export\bundle-run.bat"
set "CONFIG_TEMPLATE=%PROJECT_ROOT%\bilibili-subtitle-export\example-config.json"

pushd "%PROJECT_ROOT%" || exit /b 1
mvn -pl bilibili-subtitle-export -am -DskipTests package
if errorlevel 1 (
  popd
  exit /b 1
)
popd

if not exist "%JAR_SOURCE%" (
  echo executable jar not found: %JAR_SOURCE% 1>&2
  exit /b 1
)

if not exist "%BUNDLE_DIR%" mkdir "%BUNDLE_DIR%"
copy /Y "%JAR_SOURCE%" "%BUNDLE_DIR%\bilibili-subtitle-export.jar" >nul
if errorlevel 1 exit /b 1
copy /Y "%RUN_TEMPLATE%" "%BUNDLE_DIR%\run.bat" >nul
if errorlevel 1 exit /b 1
copy /Y "%CONFIG_TEMPLATE%" "%BUNDLE_DIR%\config.json" >nul
if errorlevel 1 exit /b 1

echo Bundle ready:
echo   %BUNDLE_DIR%\bilibili-subtitle-export.jar
echo   %BUNDLE_DIR%\run.bat
echo   %BUNDLE_DIR%\config.json
