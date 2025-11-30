mkdir freighttransport                             # making the directory

unzip freighttransport.zip -d freighttransport/    # unzipping the file and placing it in the directory

cd freighttransport                                # changing the directory

ls freighttransport                                # looking to see the files

hdfs dfs -mkdir -p /tmp/freight_onecsv             # creating hdfs directory

hdfs dfs -put Bel_60min_0101_0103_2019.csv /tmp/freight_onecsv/    #placing file in hdfs directory