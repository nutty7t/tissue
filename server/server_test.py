import functools
import pytest
import sqlite3

from server import (
    SCHEMA_FILE,
    fetch_issue,
)


MOCK_DATA = """
    INSERT INTO issue (title, description) VALUES ("Leave Maple Island", "There's a boat in Southperry.");
    INSERT INTO issue (title, description) VALUES ("Get to level 8", "I think I'm ready.");
    INSERT INTO issue (title, description) VALUES ("Talk to Olaf", "He can be found in Lith Harbor.");
    INSERT INTO issue (title, description) VALUES ("Travel to Ellinia", "Some old man can teach me some magic.");
    INSERT INTO issue (title, description) VALUES ("Talk to Grendel the Really Old", "Become a magician.");
    INSERT INTO issue (title, description) VALUES ("Lonely Issue", "This issue has no tags.");

    INSERT INTO tag (namespace, predicate, value, issue_id) VALUES ("project", "name", "Maplestory", 1);
    INSERT INTO tag (namespace, predicate, value, issue_id) VALUES ("project", "name", "Maplestory", 2);
    INSERT INTO tag (namespace, predicate, value, issue_id) VALUES ("project", "name", "Maplestory", 3);
    INSERT INTO tag (namespace, predicate, value, issue_id) VALUES ("project", "name", "Maplestory", 4);
    INSERT INTO tag (namespace, predicate, value, issue_id) VALUES ("project", "name", "Maplestory", 5);
    INSERT INTO tag (namespace, predicate, value, issue_id) VALUES ("links", "blocked-by", "2", 4);
"""


def mock(fixture):
    """
    :param fixture: pytest-style generator function fixture
    :return: setup and teardown functions for the fixture
    """
    generator = fixture()
    setup_called = False
    teardown_called = False

    def setup():
        nonlocal setup_called
        if setup_called:
            raise Exception("setup function should only be called once")
        else:
            setup_called = True
            return next(generator)

    def teardown():
        nonlocal setup_called
        nonlocal teardown_called
        if not setup_called:
            raise Exception("setup must happen before teardown")
        elif teardown_called:
            raise Exception("teardown function should only be called once")
        else:
            try:
                teardown_called = True
                next(generator)
            except StopIteration:
                pass

    return setup, teardown


def sqlite_fixture():
    with open(SCHEMA_FILE) as schema:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        cursor.executescript(schema.read())
        cursor.executescript(MOCK_DATA)
        connection.commit()
    yield connection
    connection.close()


def test_fetch_issue():
    test_cases = [
        {
            "name": "happy path",
            "issue_id": 1,
            "expected": {
                "id": 1,
                "title": "Leave Maple Island",
                "description": "There's a boat in Southperry.",
                "tags": [
                    {
                        "namespace": "project",
                        "predicate": "name",
                        "value": "Maplestory",
                    },
                ],
            },
        },
        {
            "name": "issue not found",
            "issue_id": 81132329,
            "expected": None,
        },
    ]
    for test_case in test_cases:
        sqlite_setup, sqlite_teardown = mock(sqlite_fixture)
        mock_connection = sqlite_setup()
        actual = fetch_issue(mock_connection.cursor(), test_case["issue_id"])
        assert actual == test_case["expected"]
        sqlite_teardown()


