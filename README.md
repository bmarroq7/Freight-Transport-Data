# Freight-Transport-Data
Freight Transport Analytics Project

This project analyzes Belgian freight transport data using Hive, HDFS, and Power BI. The workflow follows the required big-data processing steps: loading a dataset into HDFS, creating a debugging sample, cleaning and enriching the data in Hive, exporting results, and visualizing them in Power BI.

Files Included

BelgiumFreight.hql contains the full HiveQL script for raw ingestion, cleaning, enrichment, and export.
HDFS.txt contains all Linux and HDFS commands used for loading data and retrieving results.
Freight Transport Report.pbix is the Power BI report with all final visuals.

Project Visuals.pdf contains an exported version of the dashboard.

Dataset Used

The project uses one CSV from the freighttransport.zip package: Bel_60min_0101_0103_2019.csv.
This file contains 60-minute traffic counts and speeds for Belgian street segments from January 1 to January 3, 2019.
Only this single CSV was used due to cluster storage limits. 

Processing Steps

The selected CSV was loaded into HDFS under /tmp/freight_onecsv.
An external Hive table was created pointing to the raw CSV.
A 10,000-row debugging sample was created as required by the instructor.
The sample was cleaned by removing invalid timestamps, null street IDs, and negative values.
The sample was enriched by adding hour-of-day values, time-segment labels, and a sorting column.
The same cleaning and enrichment logic was applied to the full dataset to create the final enriched table.
Four analytical result files were exported to HDFS: heatmap data, line-chart data, street efficiency data, and running total data.
These files were retrieved from HDFS and imported into Power BI, and Tableau.

Power BI Visuals

The report includes a heatmap showing total traffic for the top 15 busiest streets by time segment, a line chart showing average traffic speed across time segments, and an efficiency table comparing streets based on traffic, speed, and a calculated efficiency score.

Tableau Visual

The report includes an animated running total bar chart showing cumulative traffic count by time segment over the course of 3 days. 

Running the Project

Run the Hive pipeline using:
beeline -f BelgiumFreight.hql

Use the commands in HDFS.txt to load data into HDFS and to retrieve exported CSV files. SCP commands were not included.
