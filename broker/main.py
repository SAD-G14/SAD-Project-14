from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/queue/push', methods=['POST'])
def post_data():
    data = request.get_json()
    response = {'message': 'Data received successfully'}
    return jsonify(response), 200


@app.route('/health', methods=['GET'])
def get_data():
    data = {'server is up and running'}
    return jsonify(data), 200


if __name__ == '__main__':
    app.run(debug=True)
