mkdir -p /tmp/s3
rm -rf /tmp/s3/*

MYSQL="mysql --login-path=local -v -v -v "

for dc in 1 2 3 4 5 6 7 8 9 10 11 12 13 
do
  echo "Exporting DC ${dc} FROM DB "
  $MYSQL -e "select * from voters where dc=${dc} into outfile '/tmp/s3/dc_${dc}.csv' fields terminated by ',' enclosed by '\"' lines terminated by '\n'"
done

for dc in 1 2 3 4 5 6 7 8 9 10 11 12 13 
do
  echo "Archiving the file ${dc}"
  cd /tmp/s3 && tar -cvz -f dc_${dc}.tar.gz dc_${dc}.csv && rm -rf dc_${dc}.csv
done

echo "Syncing to s3"
cd /tmp/s3 && rm -rf *.csv
cd /tmp/s3 && aws s3 sync . s3://jsp-voters-data/data1/




