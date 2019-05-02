import functools

from flask import Blueprint
from server.models.database import get_database

blueprint = Blueprint("issue", __name__)


def validate_request_payload(
    require_issue_title=False,
    require_issue_id=False,
    require_tag_fields=False,
):
    """Validate JSON request payload.

    :param require_issue_title: issue 'title' field is required
    :param require_issue_id: 'id' field in issue is required
    :param require_tag_fields: 'namespace', 'predicate', 'value' are required
    :raise 400: payload is invalid
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            request_schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "definitions": {
                    "tag": {
                        "type": "object",
                        "required": [],
                        "properties": {
                            "namespace": {
                                "type": "string",
                            },
                            "predicate": {
                                "type": "string",
                            },
                            "value": {
                                "type": ["number", "string"],
                            },
                        },
                    },
                    "issue": {
                        "type": "object",
                        "required": [],
                        "properties": {
                            "title": {
                                "type": "string",
                            },
                            "description": {
                                "type": "string",
                            },
                            "tags": {
                                "type": "array",
                                "default": [],
                                "minItems": 0,
                                "items": {
                                    "$ref": "#/definitions/tag",
                                },
                            },
                        },
                    },
                },
                "oneOf": [
                    {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "$ref": "#/definitions/issue",
                        },
                    },
                    {
                        "$ref": "#/definitions/issue",
                    },
                ],
            }

            if require_issue_title:
                # 'description' will always be an optional field.
                request_schema["definitions"]["issue"]["required"] = ["title"]

            if require_issue_id:
                request_schema["definitions"]["issue"]["required"].append("id")
                request_schema["definitions"]["issue"]["properties"]["id"] = {
                    "type": ["integer", "string"],
                }

            if require_tag_fields:
                request_schema["definitions"]["tag"]["required"] = [
                    "namespace", "predicate", "value",
                ]

            try:
                validate(
                    instance=request.get_json(),
                    schema=request_schema,
                )

            except ValidationError:
                errors = ["invalid json payload"]
                return payload({}, errors, 400)

            return func(*args, **kwargs)
        return wrapper
    return decorator


def payload(data={}, errors=[], status_code=200):
    """Constructs a HTTP response payload.

    :param data: payload data
    :param errors: list of errors
    :param status_code: http status code
    """
    return jsonify({
        "data": data,
        "errors": errors
    }), status_code


def to_issue_list(request):
    """Normalizes the request payload body.

    Clients are allowed to send a payload containing a single issue (dict) or
    multiple issues (list of dict). This function converts dict payloads into
    list of dict payloads.

    :param request: Flask request
    :return: list of issues
    """
    payload = request.get_json()
    if type(payload) == dict:
        payload = [payload]

    return payload


@blueprint.route("/api/issue/<int:id>", methods=["GET"])
def get_issue_route(id):
    """Get issue by id.

    :param id: issue id
    :raise 404: issue not found
    :raise 500: unexpected server error
    """
    try:
        database = get_database().cursor()
        issue = get_issue(cursor, id)

    except Exception as error:
        errors.append("unexpected server error")
        errors.append(str(error))
        return payload({}, errors, 500)

    if issue is None:
        errors = [f"issue #{id} does not exist"]
        return payload(errors=errors, status_code=404)

    return payload(issue)


def get_issue(cursor, id):
    """Helper function that fetches an issue from SQLite.

    :param cursor: sqlite3 cursor
    :param id: issue id
    :return: issue if it exists; otherwise None.
    """
    cursor.execute(
        """
        SELECT
            issue.id,
            issue.title,
            issue.description,
            tag.namespace,
            tag.predicate,
            tag.value
        FROM
            issue
            LEFT JOIN
                tag
                ON issue.id = tag.issue_id
        WHERE
            issue.id = ?

        """, (id,),
    )

    issue = None
    for row in cursor:
        # Initialize issue dict.
        if issue is None:
            issue = {
                "id": row["id"],
                "title": row["title"],
                "description": row["description"],
                "tags": [],
            }

        # We can assume that a row contains a tag if any of its tag
        # fields are non-empty strings. This is enforced by a CHECK
        # constraint defined in the database schema.
        contains_tag = lambda r: bool(row["value"])

        # Add tag to issue.
        if contains_tag:
            issue["tags"].append({
                "namespace": row["namespace"],
                "predicate": row["predicate"],
                "value": row["value"],
            })

    return issue


@blueprint.route("/api/issue", methods=["POST"])
@validate_request_payload(
    require_issue_title=True,
    require_issue_id=True,
    require_tag_fields=True,
)
def post_issue_route():
    """Create one or more issues.

    :raise 400: created issue violates integrity checks
    :raise 500: unexpected server error
    """
    database = get_database()
    created_issues = []
    errors = []

    with database:
        try:
            for issue in to_issue_list(request):
                created_issue = create_issue(database.cursor(), issue)
                created_issues.append(created_issue)

            # TODO: Validate database state against Prolog rules.

        except sqlite3.IntegrityError as error:
            errors.append("failed to create rows in sqlite")
            errors.append(str(error))
            return payload({}, errors, 400)

        except Exception as error:
            errors.append("unexpected server error")
            errors.append(str(error))
            return payload({}, errors, 500)

    return payload(created_issues)


def create_issue(cursor, issue):
    """Helper function that creates an issue in SQLite.

    :param cursor: sqlite3 cursor
    :param issue: issue dict
    :return: original issue dict with additional id field
    """
    cursor.execute(
        "INSERT INTO issue (title, description) VALUES (?, ?)",
        (issue["title"], issue.get("description", "")),
    )

    issue_id = cursor.lastrowid
    for tag in issue.get("tags", []):
        cursor.execute(
            """
            INSERT INTO tag (namespace, predicate, value, issue_id)
            VALUES (?, ?, ?, ?)
            """, (
                tag.get("namespace", ""),
                tag.get("predicate", ""),
                tag.get("value", ""),
                issue_id,
            ),
        )

    return {**issue, "id": issue_id}


@blueprint.route("/api/issue", methods=["PUT"])
@validate_request_payload(require_issue_id=True)
def put_issue_route():
    """Replace one or more issues.

    :raise 400: created issue violates integrity checks
    :raise 500: unexpected server error
    """
    database = get_database()
    updated_issues = []
    errors = []

    with database:
        try:
            cursor = database.cursor()
            for issue in to_issue_list(request):
                # Check if the issue exists in the database.
                if get_issue(cursor, issue["id"]) is None:
                        create_issue(cursor, issue)
                else:
                    # PUT has replace semantics so we must ensure that all
                    # fields are being updated. For updating a subset of
                    # fields, PATCH should be used.
                    if request.method == "PUT":
                        if "title" not in issue:
                            issue["title"] = ""
                        if "description" not in issue:
                            issue["description"] = ""
                        if "tags" not in issue:
                            issue["tags"] = []

                    updated_issues.append(
                        replace_issue(
                            cursor,
                            issue["id"],
                            issue,
                        ),
                    )

            # TODO: Validate database state against Prolog rules.

        except sqlite3.IntegrityError as error:
            errors.append("failed to create rows in sqlite")
            errors.append(str(error))
            return payload({}, errors, 400)

        except Exception as error:
            errors.append("unexpected server error")
            errors.append(str(error))
            return payload({}, errors, 500)

    return updated_issues


def replace_issue(cursor, id, fields):
    """Helper function that replaces an issue in SQLite.

    :param cursor: sqlite3 cursor
    :param id: issue id
    :param fields: dict of fields values
    """
    # Update issue fields.
    cursor.execute(
        f"UPDATE issue SET title = ?, description = ? WHERE id = ?",
        (fields["title"], fields["description"], str(id)),
    )

    # Update issue tags.
    cursor.execute("DELETE FROM tag WHERE issue_id = ?", (str(id),))
    for tag in fields.get("tags", []):
        cursor.execute(
            """
            INSERT INTO tag (namespace, predicate, value, issue_id)
            VALUES (?, ?, ?, ?)
            """, (
                tag["namespace"],
                tag["predicate"],
                tag["value"],
                str(id),
            ),
        )

    return get_issue(cursor, id)


@blueprint.route("/api/issue", methods=["PATCH"])
@validate_request_payload(require_issue_id=True)
def patch_issue_route():
    """Patch one or more issues.

    :raise 400: created issue violates integrity checks
    :raise 500: unexpected server error
    """
    database = get_database()
    updated_issues = []
    errors = []

    with database:
        try:
            cursor = database.cursor()
            for issue in to_issue_list(request):
                if get_issue(cursor, issue["id"]) is None:
                    errors.append([f"issue #{issue['id']} does not exist"])
                    return payload({}, errors, 404)
                else:
                    updated_issues.append(
                        patch_issue(
                            cursor,
                            issue["id"],
                            issue,
                        ),
                    )

                    # TODO: Validate database state against Prolog rules.

        except sqlite3.IntegrityError as error:
            errors.append("failed to create rows in sqlite")
            errors.append(str(error))
            return payload({}, errors, 400)

        except Exception as error:
            errors.append("unexpected server error")
            errors.append(str(error))
            return payload({}, errors, 500)

    return updated_issues


def patch_issue(cursor, id, fields):
    """Helper function that patches an issue in SQLite.

    :param cursor: sqlite3 cursor
    :param id: issue id
    :param fields: dict of fields values
    """
    def generate_set_clause(field_names):
        """
        Construct a variadic SET clause for an UPDATE query (in terms of
        DB API parameter substitutions) based on the number of fields
        that will get updated.
        """
        return "SET " + ",".join(map(
            lambda key: f"{key} = ?",
            field_names,
        ))

    # Update issue fields.
    fields_to_update = {
        key: fields[key]
        for key in ["title", "description"]
        if key in fields
    }
    cursor.execute(
        f"""
        UPDATE issue
        {generate_set_clause(fields_to_update.keys())}
        WHERE id = ?
        """, (*fields_to_update.values(), (str(id),))
    )

    # Update issue tags.
    for tag in fields.get("tags", []):
        if "id" in tag:
            fields_to_update = {
                key: tag[key]
                for key in ["namespace", "predicate", "value"]
                if key in tag and tag[key] != ""
            }
            if len(fields_to_update) > 0:
                # Patch an existing tag's fields.
                cursor.execute(
                    f"""
                    UPDATE tag
                    {generate_set_clause(fields_to_update.keys())}
                    WHERE id = ? AND issue_id = ?
                    """, (
                        *fields_to_update.values(),
                        tag["id"],
                        str(id),
                    )
                )
            else:
                # Remove the tag if there are no fields to update.
                cursor.execute("DELETE FROM tag WHERE id = ?", (tag["id"],))
        else:
            # Add a new tag if a tag id was not provided.
            cursor.execute(
                """
                INSERT INTO tag (namespace, predicate, value, issue_id)
                VALUES (?, ?, ?, ?)
                """, (
                    tag["namespace"],
                    tag["predicate"],
                    tag["value"],
                    str(id),
                ),
            )

    return get_issue(cursor, id)


@blueprint.route("/api/issue/<int:id>", methods=["DELETE"])
def delete_issue_route(id):
    database = get_database()
    with database:
        try:
            cursor = database.cursor()
            cursor.execute("DELETE FROM issue WHERE id = ?", (id, ))
            cursor.execute("DELETE FROM tag WHERE issue_id = ?", (id, ))

        except Exception as error:
            errors = []
            errors.append("unexpected server error")
            errors.append(str(error))
            return payload({}, errors, 500)

    return payload()

