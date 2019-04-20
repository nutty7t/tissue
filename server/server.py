from flask import Flask, g, jsonify, request
from jsonschema import validate, ValidationError

import argparse
import sqlite3

app = Flask(__name__)
DATABASE_FILE = "./tissue.db"
SCHEMA_FILE = "./schema.sql"

# The following JSON schema defines the structure of issues and tags
# as they appear in HTTP request and response payloads.
JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
        "tag": {
            "type": "object",
            "required": [
                "namespace",
                "predicate",
                "value",
            ],
            "properties": {
                "namespace": {
                    "type": "string",
                },
                "predicate": {
                    "type": "string",
                },
                "value": {
                    "type": ["string", "number"],
                },
            },
        },
        "issue": {
            "type": "object",
            "required": ["title"],
            "properties": {
                "title": {
                    "type": "string",
                },
                "description": {
                    "type": "string",
                },
                "tags": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/tag",
                    },
                    "default": [],
                    "minItems": 0,
                },
            },
        }
    },
}

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
    cursor = get_database_connection().cursor()
    cursor.execute(f"""
        SELECT
            issue.id,
            issue.title,
            issue.description,
            tag.namespace,
            tag.predicate,
            tag.value
        FROM
            issue LEFT JOIN tag
            ON issue.id = tag.issue_id
        WHERE
            issue.id = {id}
    """)

    issues = {}
    for row in cursor:
        if row["id"] not in issues:
            issues[row["id"]] = {
                "id": row["id"],
                "title": row["title"],
                "description": row["description"],
                "tags": [],
            }
        if row["value"]:
            issues[row["id"]]["tags"].append({
                "namespace": row["namespace"],
                "predicate": row["predicate"],
                "value": row["value"],
            })

    errors = []
    status_code = 200
    if len(issues.values()) == 0:
        errors.append(f"issue #{id} does not exist")
        status_code = 404

    return jsonify({
        "data": list(issues.values()),
        "errors": errors,
    }), status_code

@app.route("/api/issue", methods=["POST"])
def create_issue():
    request_payload = request.get_json()
    request_schema = {
        **JSON_SCHEMA,
        **{
            "type": "object",
            "properties": {
                "data": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/issue",
                    },
                    "minItems": 1,
                },
            },
        },
    }

    try:
        validate(
            instance=request_payload,
            schema=request_schema,
        )

    except ValidationError:
        return jsonify({
            "data": [],
            "errors": ["failed to validate payload against json schema"],
        }), 400

    # [todo] Validate the issue(s) against Prolog rules.

    # Attempt to create issues and tags in SQLite.
    # Rollback in the event of an exception.
    connection = get_database_connection()
    try:
        with connection:
            for issue in request_payload["data"]:
                # Create issue.
                cursor = connection.cursor()
                cursor.execute(f"""
                    INSERT INTO issue (
                        title,
                        description
                    )
                    VALUES (
                        "{issue["title"]}",
                        "{issue.get("description", "")}"
                    )
                """)

                # Add tags to issue.
                issue_id = cursor.lastrowid
                for tag in issue.get("tags", []):
                    cursor.execute(f"""
                        INSERT INTO tag (
                            namespace,
                            predicate,
                            value,
                            issue_id
                        )
                        VALUES (
                            "{tag["namespace"]}",
                            "{tag["predicate"]}",
                            "{tag["value"]}",
                            "{issue_id}"
                        )
                    """)

    except sqlite3.IntegrityError as error:
        return jsonify({
            "data": [],
            "errors": [
                "failed to create rows in sqlite",
                str(error),
            ],
        }), 400

    # [todo] Return the created issue(s).

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
        connection = sqlite3.connect(DATABASE_FILE)
        cursor = connection.cursor()
        cursor.executescript(schema.read())
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

