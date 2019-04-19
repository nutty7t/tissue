from flask import Flask, g
import argparse
import sqlite3

app = Flask(__name__)
DATABASE_FILE = "./tissue.db"
SCHEMA_FILE = "./schema.sql"

def get_database_connection():
    """
    Get a SQLite database connection from the application context.
    """
    connection = getattr(g, "database", None)
    if connection is None:
        g.database = sqlite3.connect(DATABASE_FILE)
        connection = g.database
        connection.row_factory = sqlite3.Row
    return connection

@app.teardown_appcontext
def close_database_connection(exception):
    """
    Automatically closes the database connection resource in the
    application context at the end of a request.
    """
    connection = getattr(g, "database", None)
    if connection is not None:
        connection.close()

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
    # Setup database schema.
    with open(SCHEMA_FILE) as schema:
        statements = schema.read().split("\n\n")
        connection = sqlite3.connect(DATABASE_FILE)
        cursor = connection.cursor()
        for statement in statements:
            cursor.execute(statement)
        connection.commit()
        connection.close()

    # Parse port number.
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

    # Start HTTP server.
    app.run(debug=True, host="0.0.0.0", port=args.port)

