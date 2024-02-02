from time import time
from flask import Flask, request, jsonify

from broker.data.message_request import MessageRequest
from broker.application import broker

app = Flask(__name__)


@app.route('/queue/push', methods=['POST'])
def push():
    data = request.get_json()
    message_request = MessageRequest(data.key, data.value, int(time()), data.producer_id, data.sequence_number)
    response = broker.push(message_request)
    return jsonify(response), 200


@app.route('/queue/pull', methods=['GET'])
def pull():
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


if __name__ == '__main__':
    app.run(debug=True)
