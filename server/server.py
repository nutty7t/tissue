from flask import Flask

app = Flask(__name__)

@app.route("/")
def tissue():
    return "tissue -- a tiny issue tracker server"

