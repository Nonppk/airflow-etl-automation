#Import library
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os
import tarfile
import pandas as pd

# Define global variables
destination_directory = os.getenv("DEST_DIR", "/opt/airflow/dags/data")
csv_input_file = os.path.join(destination_directory, 'vehicle-data.csv')
csv_output_file = os.path.join(destination_directory, 'csv_data.csv')
tsv_input_file = os.path.join(destination_directory, 'tollplaza-data.tsv')
tsv_output_file = os.path.join(destination_directory, 'tsv_data.csv')
fixed_width_input_file = os.path.join(destination_directory, 'payment-data.txt')
fixed_width_output_file = os.path.join(destination_directory, 'fixed_width_data.csv')
extracted_data_file = os.path.join(destination_directory, 'extracted_data.csv')
transformed_data_file = os.path.join(destination_directory, 'transformed_data.csv')
source_url = os.getenv("SOURCE_URL","https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz")
downloaded_file_path = os.path.join(destination_directory, 'tolldata.tgz')

# download_dataset
def download_dataset():
    # Ensure the destination directory exists
    os.makedirs(destination_directory, exist_ok=True)

    # Download the file
    response = requests.get(source_url, stream=True)
    if response.status_code == 200:
        with open(downloaded_file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print(f"Downloaded {source_url} to {downloaded_file_path}")
    else:
        raise Exception(f"Failed to download file from {source_url}, status code: {response.status_code}")

# untar_dataset
def untar_dataset():
    # Ensure the extract directory exists
    os.makedirs(destination_directory, exist_ok=True)

    # Untar the file
    with tarfile.open(downloaded_file_path, 'r:gz') as tar:
        tar.extractall(path=destination_directory)
        print(f"Extracted {downloaded_file_path} to {destination_directory}")

# extract_csv_data
def extract_csv_data():
    # Define the column names
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Vehicle code']
    
    # Read the CSV file without header
    df = pd.read_csv(csv_input_file, header=None, names=column_names)
    
    # Extract the specified columns
    extracted_data = df[['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']]
    
    # Save the extracted data to a new CSV file
    extracted_data.to_csv(csv_output_file, index=False, header=False)
    print(f"Extracted data from {csv_input_file} and saved to {csv_output_file}")

# extract_tsv_data
def extract_tsv_data():
    # Define the column names
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id', 'Tollplaza code']
    
    # Read the TSV file without header
    df = pd.read_csv(tsv_input_file, sep='\t', header=None, names=column_names)
    
    # Extract the specified columns
    extracted_data = df[['Number of axles', 'Tollplaza id', 'Tollplaza code']]
    
    # Save the extracted data to a new CSV file without header
    extracted_data.to_csv(tsv_output_file, index=False, header=False)
    print(f"Extracted data from {tsv_input_file} and saved to {tsv_output_file}")

# extract_fixed_width_data
def extract_fixed_width_data():
    # Define the column specifications (start, end) for fixed-width fields
    colspecs = [(58, 61),  # Type of Payment code (assuming it is in the first 5 characters)
                (62, 67)]  # Vehicle Code (assuming it is in the next 5 characters)

    # Define the column names
    column_names = ['Type of Payment code', 'Vehicle Code']

    # Read the fixed-width file
    df = pd.read_fwf(fixed_width_input_file, colspecs=colspecs, header=None, names=column_names)

    # Save to CSV without header
    df.to_csv(fixed_width_output_file, index=False, header=False)
    print(f"Extracted data from {fixed_width_input_file} and saved to {fixed_width_output_file}")

# consolidate_all_data
def consolidate_all_data():
    # Read each CSV file into a DataFrame without header
    df1 = pd.read_csv(csv_output_file, header=None, index_col=False)
    df2 = pd.read_csv(tsv_output_file, header=None, index_col=False)
    df3 = pd.read_csv(fixed_width_output_file, header=None, index_col=False)
    print(df1,df2,df3)
    
    # Concatenate all DataFrames into one
    consolidated_data = pd.concat([df1, df2, df3], axis=1)

    # Save the consolidated DataFrame to a new CSV file
    consolidated_data.to_csv(extracted_data_file, index=False, header=False)
    print(f"Consolidated data saved to {extracted_data_file}")

# transform_consolidate_data
def transform_consolidate_data():
    # Define the column names for the extracted data
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code']
    
    # Read the data without header
    df = pd.read_csv(extracted_data_file, header=None, names=column_names)

    # Transform the vehicle_type field to uppercase
    df['Vehicle type'] = df['Vehicle type'].str.upper()

    # Save the transformed data to the specified path without header
    df.to_csv(transformed_data_file, index=False, header=False)
    print(f"Transformed data saved to {transformed_data_file}")
    
#Define DAG arguments
default_args = {
    'owner': 'ratchanon',
    'start_date': days_ago(0),
    'email': ['ratchanonppk@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#Define DAG
dag = DAG(
    'etl_toll_data_pipeline',
    default_args=default_args,
    description='Automated ETL pipeline using Apache Airflow',
    schedule_interval=timedelta(days=1),
)

# Define the task named
download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_dataset,
    dag=dag,
)

unzip_task = PythonOperator(
    task_id='unzip_data',
    python_callable=untar_dataset,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_csv_data,
    dag=dag,
)

extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_tsv_data,
    dag=dag,
)

extract_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_fixed_width_data,
    dag=dag,
)

consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_all_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_consolidate_data,
    dag=dag,
)

#Task pipeline
download_task >> unzip_task >> extract_csv_task >> extract_tsv_task >> extract_fixed_width_task >> consolidate_task >> transform_task