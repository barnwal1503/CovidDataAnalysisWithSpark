from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/')
def index():
    return "Welcome To This API Course"


@app.route("/get-square", methods=['GET'])
def getSquarreFive():
    return jsonify(5 * 5)


@app.route("/get-square<int:num>", methods=['GET'])
def getSquarreGiven(num):
    return jsonify(num * num)


if __name__ == "__main__":
    app.run(debug=True)
