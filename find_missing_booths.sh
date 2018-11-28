#!/bin/bash
DC=${1}
AC=${2}

if [ -z $DC ] || [ -z $AC ]; then
 echo "Missing DC or AC"
 echo "Usage: $0 dc ac"
 exit 
fi

DB="mysql --login-path=imac -h192.168.86.2 -uvenu -N --batch jsp "
QUERY="SELECT group_concat(t.id-1) as booths FROM (select distinct booth as id from voters where dc=${DC} and ac=${AC})t LEFT JOIN (SELECT distinct booth as id from voters where dc=${DC} and ac=${AC}) t1 ON t.id=t1.id+1 WhERE t1.id IS NULL and t.id != 1"
QUERY="SELECT group_concat(SNO) from booths where dc=${DC} and ac=${AC} and SNO NOT IN(select distinct booth from voters where dc=${DC} and ac=${AC})"
missing=`$DB -e "$QUERY"`
booths=`echo $missing | sed "s/,/ /g"`
echo "Missing booths for District ${DC}, Assembly ${AC}: ${missing} ($booths)"

for f in ${booths}
do
pdf_file="dc_${DC}/${DC}_${AC}/${DC}_${AC}_${f}.pdf"
txt_file="dc_${DC}/txt/${DC}_${AC}_${f}.txt"
if [ -f ${txt_file} ]; then
  echo "Booth ${f} text file exists"
elif [ -f ${pdf_file} ]; then
  echo "Booth ${f} image file exists"
else 
  echo "Booth ${f} missing"
fi

done

#select dc, ac, actual, loaded, actual-loaded as missing from (select dc, ac, count(distinct booth) loaded, (select count(*) from booths where dc=v.dc and ac=v.ac) as actual from voters v where dc=6 group by 1,2)t order by 1,2


