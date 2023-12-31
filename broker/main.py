from time import time
from flask import Flask, request, jsonify

from broker.data.message_request import MessageRequest
from broker.application import broker

app = Flask(__name__)


@app.route('/queue/push', methods=['POST'])
def post_data():
    data = request.get_json()
    message_request = MessageRequest(data.key, data.value, int(time()), data.producer_id)
    response = broker.push(message_request)
    return jsonify(response), 200


@app.route('/health', methods=['GET'])
def get_data():
    data = 'server is up and running'
    return jsonify(data), 200


if __name__ == '__main__':
    app.run(debug=True)
