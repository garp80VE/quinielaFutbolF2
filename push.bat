@echo off
set /p TOKEN="GitHub token: "
git -C "%~dp0" push https://garp80VE:%TOKEN%@github.com/garp80VE/quinielaFutbolF2.git main
pause
