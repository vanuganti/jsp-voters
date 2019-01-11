#!/bin/bash
dir=$1
cdir=`pwd`

cd $dir
IFS=$'\n'   

for i in 1 2 3 4 5 6 7 8 9 10 11 12 13
do
  for p in `grep Mandal[[:space:]]*. ${i}_*.txt | grep -v "Auxillary Polling" | grep -v "Name"`
  do
    file=`echo $p | sed "s/\.txt.*//g"`
    m=`echo $p | awk '{$1=$1};1' | sed "s/.*Mandal[ ].//g" |  awk '{$1=$1};1'`
    d=`echo $file | cut -d "_" -f1`
    c=`echo $file | cut -d "_" -f2`
    b=`echo $file | cut -d "_" -f3`
    if [ ! -z $m ]; then
      l=${#m}
      if [ $l -ge 3 ];then
        echo "$d,$c,$b,\"$m\""
      fi
    fi
  done
done
cd $cdir
