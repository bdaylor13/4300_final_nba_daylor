import boto3
import pandas as pd
import mysql.connector
import json

# AWS S3 Client Setup
s3 = boto3.client('s3')
bucket_name = 'daylorsourcebucket'
file_key = 'nba_stats.csv'

# MySQL Connection Setup
def connect_to_mysql():
    return mysql.connector.connect(
        host='mysql-nbafinal.chymesai6fez.us-east-1.rds.amazonaws.com',    # Update with your RDS endpoint
        user='admin',        # Update with your MySQL username
        password='belknapSLIME(8)',    # Update with your MySQL password
        database='nba_stats'  # Database name
    )

# Function to download file from S3
def download_file_from_s3():
    try:
        # Download the file from S3
        s3.download_file(bucket_name, file_key, 'nba_stats.csv')
        print("File downloaded successfully from S3.")
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return None
    return 'nba_stats.csv'

# Function to process the data
def process_data(file_path):
    try:
        # Load the CSV file into a DataFrame
        data = pd.read_csv(file_path)
        
        # Group by player name and calculate the average points
        summary = data.groupby('PLAYER_NAME').PTS.mean().reset_index()
        summary.columns = ['Player', 'Average Points']
        print("Data processed successfully.")
        return summary
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

# Function to insert data into MySQL
def insert_data_into_mysql(data_frame):
    try:
        conn = connect_to_mysql()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("CREATE TABLE IF NOT EXISTS PlayerStats (Player VARCHAR(255), AvgPoints FLOAT)")

        # Insert the data into the MySQL table
        for _, row in data_frame.iterrows():
            cursor.execute("INSERT INTO PlayerStats (Player, AvgPoints) VALUES (%s, %s)", 
                           (row['Player'], row['Average Points']))

        # Commit the transaction
        conn.commit()
        print("Data inserted into MySQL successfully.")
    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")
    finally:
        cursor.close()
        conn.close()

# Main script execution
if __name__ == "__main__":
    file_path = download_file_from_s3()

    if file_path:
        data_frame = process_data(file_path)

        if data_frame is not None:
            insert_data_into_mysql(data_frame)
        else:
            print("Data processing failed.")
    else:
        print("File download failed.")
