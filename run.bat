@echo off
setlocal enabledelayedexpansion

:: Define the list of arguments to pass to the Python script
set argsList= "1 5" "2 8" "3 11" "4 15" "5 20"

:: Loop through the arguments list and start a new cmd window for each argument
for %%a in (%argsList%) do (
    start cmd /k python m1.py %%a
)

endlocal
