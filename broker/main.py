from time import time

import requests
from flask import Flask, request, jsonify
import logging
logging.basicConfig(level=logging.DEBUG)
from broker.data.message_request import MessageRequest
from broker.application import broker

app = Flask(__name__)


@app.route('/queue/push', methods=['POST'])
def push():
    logging.info("POST /queue/push with data: {}".format(request.get_json()))
    if broker.REPLICA:
        requests.post('http://{}:5000/queue/push'.format(broker.REPLICA['ip']), json=request.get_json())
    data = request.get_json()
    message_request = MessageRequest(data['key'], data['value'], int(time()), data['producer_id'], data['sequence_number'])
    response = broker.push(message_request)
    return jsonify(response), 200


@app.route('/queue/pull', methods=['GET'])
def pull():
    logging.info("GET /queue/pull")
    if broker.REPLICA:
        requests.get('http://{}:5000/queue/pull'.format(broker.REPLICA['ip']), json=request.get_json())
    response = broker.pull()
    return jsonify(response), 200


@app.route('/health', methods=['GET'])
def health():
    data = 'server is up and running'
    return jsonify(data), 200


@app.route('/queue/ack', methods=['POST'])
def ack():
    data = request.get_json()
    producer_id = data['producer_id']
    sequence_number = data['sequence_number']
    response = broker.ack(producer_id, sequence_number)
    return jsonify(response), 200

@app.route('/broker/replica', methods=['POST'])
def accept_replica():
    data = request.get_json()
    logging.info("POST /broker/replica with data: {}".format(data))
    broker.accept_replica(data)
    return 'OK', 200

if __name__ == '__main__':
    print("broker is up")
    broker.join_server()
    app.run(host='0.0.0.0', port=5000, debug=True)
