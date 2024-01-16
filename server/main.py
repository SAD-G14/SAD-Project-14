from flask import Flask, request, jsonify

from application.application import Application

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


if __name__ == '__main__':
    app.run(debug=True)
