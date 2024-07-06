@echo off
setlocal enabledelayedexpansion

rem Creating 5 folders if they don't exist
for /l %%i in (1, 1, 5) do (
    if not exist "log_node_%%i" (
        mkdir "log_node_%%i"
    )
    
    cd "log_node_%%i"
    
    rem Creating txt files if they don't exist
    if not exist log.txt (
        type nul > log.txt
    )
    
    if not exist metadata.txt (
        type nul > metadata.txt
    )
    
    if not exist dump.txt (
        type nul > dump.txt
    )
    
    cd ..
)

echo Folder structure created successfully.
