import functools
import sqlite3

from server import (
    SCHEMA_FILE,
    create_issue,
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
            "name": "issue with tag",
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
            "name": "issue without tags",
            "issue_id": 6,
            "expected": {
                "id": 6,
                "title": "Lonely Issue",
                "description": "This issue has no tags.",
                "tags": [],
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


def test_create_issue():
    test_cases = [
        {
            "name": "issue without tags",
            "issue": {
                "title": "Scroll a 21 atk bwg",
                "description": "10% chance? 7 times in a row?",
            },
            "expectedIssue": {
                "title": "Scroll a 21 atk bwg",
                "description": "10% chance? 7 times in a row?",
            },
            "expectedTags": [],
            "expectedException": False,
        },
        {
            "name": "issue with a single tag",
            "issue": {
                "title": "Ride the ship to Orbis",
                "description": "Watch out for the Crimson Balrog!",
                "tags": [
                    {
                        "namespace": "project",
                        "predicate": "name",
                        "value": "Maplestory",
                    },
                ],
            },
            "expectedIssue": {
                "title": "Ride the ship to Orbis",
                "description": "Watch out for the Crimson Balrog!",
            },
            "expectedTags": [
                {
                    "namespace": "project",
                    "predicate": "name",
                    "value": "Maplestory",
                },
            ],
            "expectedException": False,
        },
        {
            "name": "issue with multiple tags",
            "issue": {
                "title": "Kill a King Slime",
                "description": "S> track 20k @@@...",
                "tags": [
                    {
                        "namespace": "project",
                        "predicate": "name",
                        "value": "Maplestory",
                    },
                    {
                        "namespace": "party",
                        "predicate": "quest",
                        "value": "kpq",
                    },
                ],
            },
            "expectedIssue": {
                "title": "Kill a King Slime",
                "description": "S> track 20k @@@...",
            },
            "expectedTags": [
                {
                    "namespace": "project",
                    "predicate": "name",
                    "value": "Maplestory",
                },
                {
                    "namespace": "party",
                    "predicate": "quest",
                    "value": "kpq",
                },
            ],
            "expectedException": False,
        },
        {
            "name": "issue with duplicate tags",
            "issue": {
                "title": "Issue with duplicated tags",
                "description": "This should raise an exception.",
                "tags": [
                    {
                        "namespace": "this is a",
                        "predicate": "duplicated",
                        "value": "tag",
                    },
                    {
                        "namespace": "this is a",
                        "predicate": "duplicated",
                        "value": "tag",
                    },
                ],
            },
            "expectedIssue": None,
            "expectedTags": [],
            "expectedException": True,
        },
        {
            "name": "issue with invalid tag",
            "issue": {
                "title": "Issue with invalid tag",
                "description": "This should raise an exception.",
                "tags": [
                    {
                        "namespace": "invalid",
                        "predicate": "tag",
                        # missing value field
                    },
                ],
            },
            "expectedIssue": None,
            "expectedTags": [],
            "expectedException": True,
        },
        {
            "name": "issue with empty tag",
            "issue": {
                "title": "Issue with empty tag values",
                "description": "This should raise an exception.",
                "tags": [
                    {
                        "namespace": "",
                        "predicate": "",
                        "value": "",
                    },
                ],
            },
            "expectedIssue": None,
            "expectedTags": [],
            "expectedException": True,
        },
        {
            "name": "issue without 'title' field",
            "issue": {
                "description": "Where is the title?",
            },
            "expectedIssue": None,
            "expectedTags": [],
            "expectedException": True,
        },
        {
            "name": "issue with empty 'title' field",
            "issue": {
                "title": "",
                "description": "Where is the title?",
            },
            "expectedIssue": None,
            "expectedTags": [],
            "expectedException": True,
        },
        {
            "name": "issue without 'description' field",
            "issue": {
                "title": "Where is the description?",
            },
            "expectedIssue": {
                "title": "Where is the description?",
                "description": "",
            },
            "expectedTags": [],
            "expectedException": False,
        },
        {
            "name": "issue with empty 'description' field",
            "issue": {
                "title": "Where is the description?",
                "description": "",
            },
            "expectedIssue": {
                "title": "Where is the description?",
                "description": "",
            },
            "expectedTags": [],
            "expectedException": False,
        },
    ]

    for test_case in test_cases:
        sqlite_setup, sqlite_teardown = mock(sqlite_fixture)
        mock_connection = sqlite_setup()

        try:
            cursor = mock_connection.cursor()
            issue_id = create_issue(cursor, test_case["issue"])
        except Exception:
            assert test_case["expectedException"]
            continue
        assert not test_case["expectedException"]

        # Check that the issue was created.
        cursor.execute(f"""
            SELECT *
            FROM issue
            WHERE id = ?
        """, (issue_id,))

        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0]["title"] == test_case["expectedIssue"]["title"]
        assert results[0]["description"] == test_case["expectedIssue"]["description"]

        # Check that the tags were created.
        for tag in test_case["expectedTags"]:
            cursor.execute(f"""
                SELECT *
                FROM tag
                WHERE
                    issue_id = ? AND
                    namespace = ? AND
                    predicate = ? AND
                    value = ?
            """, (
                issue_id,
                tag["namespace"],
                tag["predicate"],
                tag["value"],
            ))
            assert len(cursor.fetchall()) == 1

        sqlite_teardown()

