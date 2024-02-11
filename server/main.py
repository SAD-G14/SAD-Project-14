import random
import time

from flask import Flask, request, jsonify

from application.application import Application
import logging
from prometheus_client import Counter, Histogram
from prometheus_client import start_http_server

logging.basicConfig(level=logging.DEBUG)

application = Application()

app = Flask(__name__)

COUNTER = Counter(name='method_calls', documentation='Number of method calls', labelnames=['node', 'method'])
HISTOGRAM = Histogram(name='method_latency', documentation='latency of methods', labelnames=['node', 'method'])

@app.route('/queue/push', methods=['POST'])
def push():
    start_time = time.time()
    data = request.get_json()
    response = application.push(data)
    COUNTER.labels('server', 'push').inc()
    HISTOGRAM.labels('server', 'push').observe(time.time() - start_time)
    return jsonify(response), 200


@app.route('/queue/pull', methods=['GET'])
def pull():
    start_time = time.time()
    response = application.pull()
    COUNTER.labels('server', 'pull').inc()
    HISTOGRAM.labels('server', 'pull').observe(time.time() - start_time)
    return jsonify(response), 200


@app.route('/health', methods=['GET'])
def health():
    start_time = time.time()
    print("health called")
    time.sleep(random.random())
    data = 'server is up and running'
    COUNTER.labels('server', 'health').inc()
    HISTOGRAM.labels('server', 'health').observe(time.time() - start_time)
    return jsonify(data), 200

@app.route('/alerts', methods=['POST'])
def alerts():
    data = request.get_json()
    logging.fatal(data)
    return jsonify('OK'), 200

@app.route('/join', methods=['GET'])
def join():
    address = request.remote_addr
    broker = application.join(address)
    return jsonify({'broker': broker}), 200


if __name__ == '__main__':
    logging.info("server is up")
    start_http_server(4005)
    app.run(host='0.0.0.0', port=4000)
