from flask import Flask
import argparse

app = Flask(__name__)

@app.route("/")
def tissue():
    return "tissue -- a tiny issue tracker server\n"

@app.route("/api")
def api():
    return "Not implemented.", 501

@app.route("/api/issue/<int:id>", methods=["GET"])
def get_issue(id):
    return "Not implemented.", 501

@app.route("/api/issue/<int:id>", methods=["POST"])
def create_issue(id):
    return "Not implemented.", 501

@app.route("/api/issue/<int:id>", methods=["PUT"])
def replace_issue(id):
    return "Not implemented.", 501

@app.route("/api/issue/<int:id>", methods=["PATCH"])
def update_issue(id):
    return "Not implemented.", 501

@app.route("/api/issue/<int:id>", methods=["DELETE"])
def delete_issue(id):
    return "Not implemented.", 501

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="tissue: a tiny issue tracker server"
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        help="http server port",
        default=5000
    )
    args = parser.parse_args()
    app.run(debug=True, host="0.0.0.0", port=args.port)

