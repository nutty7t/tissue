from flask import Flask

app = Flask(__name__)

@app.route("/")
def tissue():
    return "tissue -- a tiny issue tracker server"

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")

