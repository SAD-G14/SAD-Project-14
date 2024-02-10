from flask import Flask, request, jsonify

from application.application import Application
import logging

logging.basicConfig(level=logging.DEBUG)

application = Application()

app = Flask(__name__)


@app.route('/queue/push', methods=['POST'])
def push():
    data = request.get_json()
    response = application.push(data)
    return jsonify(response), 200


@app.route('/queue/pull', methods=['GET'])
def pull():
    response = application.pull()
    return jsonify(response), 200


@app.route('/health', methods=['GET'])
def health():
    data = 'server is up and running'
    return jsonify(data), 200


@app.route('/join', methods=['GET'])
def join():
    address = request.remote_addr
    broker = application.join(address)
    return jsonify({'broker': broker}), 200


if __name__ == '__main__':
    logging.info("server is up")
    app.run(host='0.0.0.0', port=4000, debug=True)
