@echo off

rem Emptying log.txt, metadata.txt, and dump.txt in each folder
for /l %%i in (1, 1, 5) do (
    if exist "log_node_%%i\log.txt" (
        > "log_node_%%i\log.txt" type nul
    )
    
    if exist "log_node_%%i\metadata.txt" (
        > "log_node_%%i\metadata.txt" type nul
    )
    
    if exist "log_node_%%i\dump.txt" (
        > "log_node_%%i\dump.txt" type nul
    )
)

echo Files emptied successfully.
