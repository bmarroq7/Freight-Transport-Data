

cd /tmp

mkdir -p /tmp/freight_all

unzip /tmp/freighttransport.zip -d /tmp/freight_all/

ls -lh /tmp/freight_all/Bel_60min_0101_0103_2019.csv

hdfs dfs -mkdir -p /tmp/freight_onecsv

hdfs dfs -put /tmp/freight_all/Bel_60min_0101_0103_2019.csv \
    /tmp/freight_onecsv/

hdfs dfs -ls -h /tmp/freight_onecsv

mkdir -p ~/freight_export

hdfs dfs -cat /user/mbuard/freight_output/heatmap/* \
    > ~/freight_export/heatmap.csv
	
hdfs dfs -cat /user/mbuard/freight_output/linechart/* \
    > ~/freight_export/linechart.csv
	
hdfs dfs -cat /user/mbuard/freight_output/efficiency/* \
    > ~/freight_export/efficiency.csv


