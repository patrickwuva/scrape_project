from google.cloud import pubsub_v1
import json
from get_offenders import get_offenders
from add_offenders import insert_offenders
project_id = "global-sun-431221-s9"
topic_id = "taskmaster"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def publish_test_message():
    sample_data = {
        "zip_codes": ['22903']
    }

    data_str = json.dumps(sample_data)
    data = data_str.encode("utf-8")

    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

def test_scrape():
    offenders =  get_offenders(["22903"])
    insert_offenders(offenders)
if __name__ == "__main__":
    publish_test_message()
    #print(test_scrape())
