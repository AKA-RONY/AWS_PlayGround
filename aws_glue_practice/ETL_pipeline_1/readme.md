# overview:
1. create a s3 bucket that holds the csv files
2. run the 1st glue crawler and create a glue catalog
3. run 1st etl glue job that will convert csv file from the catalog and transform it to parquet
4. after transformation store the parquet file in another s3 output bucket
5. run 2nd etl glue job that will fetch the parquet file from s3 bucket and store it in aws redshift
 ##### IMPORTANT #####
 - the second crawler will crawl through redshift and fetch the table structure and create a glue catalog
 - now the glue ETL can store the parquet file to redshift with ease since we already know the table structure
<br>

![WhatsApp Image 2024-02-08 at 23 04 03_12b67972](https://github.com/AKA-RONY/AWS_PlayGround/assets/67736824/8c146739-96e3-4653-a27e-b23f1c60845c)

reference: https://www.youtube.com/watch?v=EhaXoMRCftY&list=PLLZXJgUgFQTdArrEHNwDLWBfeYEVzB4iq&index=4&t=250s
