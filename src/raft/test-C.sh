#!/bin/bash
count=0
file_prefix="test_"

while true;do
    if [ $((count)) -gt 10 ]; then
        break
    fi
    file="comment_${file_prefix}2C_${count}.txt"
    go test -run TestFigure8Unreliable2C -race >> $file
    count=$((count+1))
    
done

