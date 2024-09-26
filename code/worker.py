from google.cloud import pubsub_v1
from add_offenders import insert_offenders
from get_offenders import get_offenders
from get_images import get_images, process_images
import json
import threading

project_id = "global-sun-431221-s9"
subscription_id = "scrape"
ack_topic_id = 'taskmaster-acks'

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
ack_publisher = pubsub_v1.PublisherClient()
ack_topic_path = ack_publisher.topic_path(project_id, ack_topic_id)

def callback(message):
    data_str = message.data.decode('utf-8')
    zip_codes = json.loads(data_str)

    offenders = get_offenders(zip_codes)
    if offenders != None:
        insert_offenders(offenders)
        print(f'done with {zip_codes}')
        ack_publisher.publish(ack_topic_path, b'',zip_codes=json.dumps(zip_codes))
        message.ack()

def push_images(message):
    data_str = message.data.decode('utf-8')
    links = json.loads(data_str)
    print(links)
    try:
        get_images(links)
        #process_images()
        print(f'done with links')
        ack_publisher.publish(ack_topic_path, b'',zip_codes=json.dumps(0))
        message.ack()
    except Exception as e:
        print(f'error: {e}')
streaming_pull_future = subscriber.subscribe(subscription_path, callback=push_images)
print(f"Listening for messages on {subscription_path}...")

try:
    streaming_pull_future.result()  # Block and listen for messages
except KeyboardInterrupt:
    streaming_pull_future.cancel()
