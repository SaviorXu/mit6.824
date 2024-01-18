#!/bin/bash
count=0
file_prefix="test_"

while true;do
    num=$((count%5))
    if [ $((num)) -eq 0 ]; then
        tmp=$((count/5))
        file="${file_prefix}backUp2B_${tmp}.txt"
        if [ $((tmp)) -gt 60 ];then 
            break
        fi
    fi
    go test -run TestBackup2B -race >> $file
    count=$((count+1))
done

