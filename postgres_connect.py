import psycopg2
import pandas as pd
import boto3


local_file_path = 'users_data.csv'
bucket_name = 'mwaa-artifact-store'
s3_object_key = 'test_output/users_data.csv'

try:
    conn = psycopg2.connect(
        host="sgx-db.c9xgkwg1lrq2.ap-south-1.rds.amazonaws.com",  # Or the IP address/hostname of your PostgreSQL server
        # database="some-postgress",
        user="postgres",
        password="3cvhgADnSXvQENSAQ0qE",
        port=5432
    )
    print("Connection to PostgreSQL successful!")

    cur = conn.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name VARCHAR(100), age INT);")
    cur.execute("INSERT INTO users (name, age) VALUES (%s, %s);", ("Alice", 30))
    conn.commit()

    #read data from Postgres
    df = pd.read_sql_query("SELECT * FROM users;", conn)

    # Manipulate data
    df['type']  = 'employee'
    df.to_csv('users_data.csv', index=False)
    print(df)

    # Upload the file to S3
    s3 = boto3.client('s3')
    s3.upload_file(local_file_path, bucket_name, s3_object_key)
    print(f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{s3_object_key}")

except Exception as e:
        print(f"Error uploading file: {e}")

except psycopg2.Error as e:
    print(f"Error connecting to PostgreSQL: {e}")

finally:
    if conn:
        conn.close()
        print("PostgreSQL connection closed.")