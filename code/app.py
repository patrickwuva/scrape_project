from flask import Flask, request
from google.cloud import pubsub_v1
import base64
import json
import logging 
from get_offenders import get_offenders
from add_offenders import insert_offenders
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

@app.route('/', methods=['POST'])
def process_pubsub():
    envelope = request.get_json()
    if not envelope:
        return 'No pubsub mmessage recieved', 400
    
    message = envelope.get('message')
    if not message:
        return 'invalid message format', 400
    
    data = json.loads(base64.b64decode(message['data']).decode('utf-8'))
    zipcodes = data['zip_codes']
    logging.info(f'Received zips: {zipcodes}')
    offenders = get_offenders(zipcodes)
    insert_offenders(offenders)
    logging.info(f'offenders: {offenders}')

    return 'message processed', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)