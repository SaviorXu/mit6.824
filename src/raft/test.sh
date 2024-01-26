#!/bin/bash
count=0
file_prefix="test_"

while true;do
    num=$((count%5))
    if [ $((num)) -eq 0 ]; then
        tmp=$((count/5))
        file="1_${file_prefix}2C_${tmp}.txt"
        if [ $((tmp)) -gt 5 ];then 
            break
        fi
    fi
    go test -run TestFigure8Unreliable2C -race >> $file
    count=$((count+1))
done