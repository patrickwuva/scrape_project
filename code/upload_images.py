import os
from google.cloud import storage

def upload_folder_to_bucket(bucket_name, local_folder_path, bucket_folder_path):
    client = storage.Client()

    bucket = client.get_bucket(bucket_name)

    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)

            relative_path = os.path.relpath(local_file_path, local_folder_path)
            blob_name = os.path.join(bucket_folder_path, relative_path)

            blob = bucket.blob(blob_name)
            blob.upload_from_filename(local_file_path)
            os.remove(local_file_path)
            print(f'Uploaded {local_file_path} to gs://{bucket_name}/{blob_name}')

bucket_name = 'offender-images'
local_folder_path = 'images'
bucket_folder_path = 'images'

upload_folder_to_bucket(bucket_name, local_folder_path, bucket_folder_path)

