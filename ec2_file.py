import boto3
import pandas as pd
import mysql.connector

# S3 client setup
s3 = boto3.client('s3')
bucket_name = 'daylorsourcebucket'
file_key = 'NBA Player Box Score Stats(1950 - 2022).csv'

# Download file from S3
s3.download_file(bucket_name, file_key, 'NBA Player Box Score Stats(1950 - 2022).csv')

# Transform data
data = pd.read_csv('NBA Player Box Score Stats(1950 - 2022).csv')
summary = data.groupby('PLAYER_NAME').PTS.mean().reset_index()
summary.columns = ['Player', 'Average Points']

# Insert into RDS
db_connection = mysql.connector.connect(
    host='mysql-nbafinal.chymesai6fez.us-east-1.rds.amazonaws.com',
    user='admin',
    password='belknapSLIME(8)',
    database='basketball_stats'
)
cursor = db_connection.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS PlayerStats (Player VARCHAR(255), AvgPoints FLOAT)")

for _, row in summary.iterrows():
    cursor.execute("INSERT INTO PlayerStats (Player, AvgPoints) VALUES (%s, %s)", (row['Player'], row['Average Points']))

db_connection.commit()
cursor.close()
db_connection.close()
