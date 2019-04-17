from flask import Flask
import argparse

app = Flask(__name__)

@app.route("/")
def tissue():
    return "tissue -- a tiny issue tracker server\n"

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

