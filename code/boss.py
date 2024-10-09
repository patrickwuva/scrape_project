import pandas as pd
from google.cloud import pubsub_v1
from test import get_leftover
from based import based
import json

project_id = 'global-sun-431221-s9'
topic_id = 'taskmaster'
ack_sub_id = 'taskmaster-acks-sub'

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(project_id, topic_id)
ack_sub_path = subscriber.subscription_path(project_id, ack_sub_id)

total_m = 0
done_m = 0

def test_pub():
    data_str = json.dumps({'hello': 'there'})
    data = data_str.encode("utf-8")
    retries = 3

    for attempt in range(retries):
        try:
            print(f"Attempt {attempt + 1} to publish message to {topic_path}...")
            future = publisher.publish(topic_path, data)
            message_id = future.result()  # Block until the message is published
            print(f"Published message ID: {message_id}")
            break  # Exit the loop if successful
        except InternalServerError as e:
            print(f"Internal server error: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"An error occurred: {e}")
            break  # Exit the loop on other exceptions

def publish_messages():
    global total_m
    #zipcodes = pd.read_csv('zips.csv')
    #zips = zipcodes[~zipcodes['state'].isin(states)]['code'].tolist()

    zips = get_leftover()
    zips = [str(z) for z in zips if len(str(z)) == 5]

    total = 0
    """
    data_str = json.dumps(["22903"])
    data = data_str.encode("utf-8")
    for i in range(0, 5):
        future = publisher.publish(topic_path, data)
        print(future.result())
    """
    for i in range(0, len(zips), 5):
        data_str = json.dumps(zips[i:i+5])
        data = data_str.encode("utf-8")
        future = publisher.publish(topic_path, data)
        total_m +=1

    print(f'total published = {total_m}')

def publish_links():
    global total_m
    db = based()
    db.connect()
    links = db.get_image_links('TX')
    print(len(links))
    total = 0
    for i in range(0, len(links), 25):
        data_str = json.dumps(links[i:i+25])
        data = data_str.encode("utf-8")
        future = publisher.publish(topic_path, data)
        total_m +=1
    print(f'total published = {total_m}')


def ack_callback(m):
    global done_m
    done_m +=1
    zip_codes = m.attributes.get('zip_codes')
    print(f'ack zip: {zip_codes} {total_m-done_m} left')
    m.ack()

def monitor_acks():
    streaming_pull_future = subscriber.subscribe(ack_sub_path, callback=ack_callback)
    print(f'listening for acks on {ack_sub_path}')
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == '__main__':
    publish_links()
    #publish_messages()
    monitor_acks()
