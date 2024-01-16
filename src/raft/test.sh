#!/bin/bash
count=0
file_prefix="test_"

while true;do
    num=$((count%10))
    if [ $((num)) -eq 0 ]; then
        tmp=$((count/10))
        file="${file_prefix}2B_${tmp}.txt"
        if [ $((tmp)) -gt 50 ];then 
            break
        fi
    fi
    go test -run 2B -race >> $file
    count=$((count+1))
done

