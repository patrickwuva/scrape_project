from google.cloud import storage
import requests
import os
import glob
import threading
import imghdr
import time
import json
from based import based  # Assuming this is a custom module
total = 0
# Function to upload JSON data to Google Cloud Storage
def upload_json(bucket_name, data, destination):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print('JSON uploaded successfully')

# Function to upload images or files to Google Cloud Storage
def upload(bucket_name, local_file, destination):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(local_file)
    print(f'File {local_file} uploaded to {destination}')

# Function to download a file from Google Cloud Storage
def download(bucket_name, blob_name, file):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(file)
    print(f'File {blob_name} downloaded to {file}')

# Function to download an image from a URL, handling retries and proxy
def download_image(link, retry_limit=3):
    os.makedirs('images', exist_ok=True)
    offender_id = link.split('/')[-1]  # Get file name from link
    username = 'spxwhjvleu'
    password = 'Bydk9qPurElL5_3q1v'
    proxy = f"http://{username}:{password}@gate.smartproxy.com:7000"
    retries = 0
    global total
    while retries < retry_limit:
        try:
            response = requests.get(link, proxies={'http': proxy}, timeout=10)  # Added timeout
            if response.status_code == 200:
                image_type = imghdr.what(None, h=response.content)
                if image_type:
                    image_path = f'images/{offender_id}.{image_type}'
                    with open(image_path, 'wb') as file:
                        file.write(response.content)
                        total+=1
                    print(f'{total} images downloaded')
                    return image_path  # Return the file path for later use
            else:
                print(f'Failed to download {link}, status code: {response.status_code}')
        except Exception as e:
            print(f'Error downloading {link}: {e}')
        
        retries += 1
        time.sleep(2)

    print(f'Failed to download {link} after {retry_limit} retries')
    return None


def get_images(links, max_threads=250):
    threads = []
    for link in links:
        if len(threads) >= max_threads:
            for thread in threads:
                thread.join()
            threads.clear()

        thread = threading.Thread(target=download_image, args=(link,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def process_images(bucket_name='offender-images'):
    for file in glob.glob('images/*'):
        try:
            upload(bucket_name, file, f'{file}')
            os.remove(file)
            print(f'Processed and uploaded {file}')
        except Exception as e:
            print(f'Error processing {file}: {e}')

def main():
    db = based()
    db.connect()

    links = db.get_image_links('PA')
    db.close()

    get_images(links)

    process_images()

if __name__ == '__main__':
    main()
