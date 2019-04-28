import functools
import sqlite3

from server import (
    SCHEMA_FILE,
    create_issue,
    fetch_issue,
    update_issue,
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
            "issueId": 1,
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
            "issueId": 6,
            "expected": {
                "id": 6,
                "title": "Lonely Issue",
                "description": "This issue has no tags.",
                "tags": [],
            },
        },
        {
            "name": "issue not found",
            "issueId": 81132329,
            "expected": None,
        },
    ]

    for test_case in test_cases:
        sqlite_setup, sqlite_teardown = mock(sqlite_fixture)
        mock_connection = sqlite_setup()
        actual = fetch_issue(mock_connection.cursor(), test_case["issueId"])
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

        # Did the issue get created?
        cursor.execute("""
            SELECT *
            FROM issue
            WHERE id = ?
        """, (issue_id,))

        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0]["title"] == test_case["expectedIssue"]["title"]
        assert results[0]["description"] == test_case["expectedIssue"]["description"]

        # Did the tags get created?
        for tag in test_case["expectedTags"]:
            cursor.execute("""
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


def test_update_issue():
    test_cases = [
        {
            "name": "no update fields (removing tags)",
            "issueId": 1,
            "fields": {},
            "expectedIssue": {
                "title": "Leave Maple Island",
                "description": "There's a boat in Southperry.",
            },
            "expectedTags": [],
            "expectedException": False,
        },
        {
            "name": "update multiple fields (removing tags)",
            "issueId": 5,
            "fields": {
                "title": "Talk to Athena Pierce",
                "description": "Become a bowman.",
            },
            "expectedIssue": {
                "title": "Talk to Athena Pierce",
                "description": "Become a bowman.",
            },
            "expectedTags": [],
            "expectedException": False,
        },
        {
            "name": "update multiple fields (preserving tags)",
            "issueId": 5,
            "fields": {
                "title": "Talk to Athena Pierce",
                "description": "Become a bowman.",
                "tags": [
                    {
                        "namespace": "project",
                        "predicate": "name",
                        "value": "Maplestory",
                    },
                ],
            },
            "expectedIssue": {
                "title": "Talk to Athena Pierce",
                "description": "Become a bowman.",
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
            "name": "update title to empty string",
            "issueId": 6,
            "fields": {
                "title": "",
            },
            "expectedIssue": None,
            "expectedTags": [],
            "expectedException": True,
        },
        {
            "name": "update description to empty string",
            "issueId": 6,
            "fields": {
                "description": "",
            },
            "expectedIssue": {
                "title": "Lonely Issue",
                "description": "",
            },
            "expectedTags": [],
            "expectedException": False,
        },
        {
            "name": "add tag to tagless issue",
            "issueId": 6,
            "fields": {
                "description": "This issue has one tag.",
                "tags": [
                    {
                        "namespace": "project",
                        "predicate": "name",
                        "value": "Loners",
                    },
                ],
            },
            "expectedIssue": {
                "title": "Lonely Issue",
                "description": "This issue has one tag.",
            },
            "expectedTags": [
                {
                    "namespace": "project",
                    "predicate": "name",
                    "value": "Loners",
                },
            ],
            "expectedException": False,
        },
    ]

    for test_case in test_cases:
        sqlite_setup, sqlite_teardown = mock(sqlite_fixture)
        mock_connection = sqlite_setup()

        try:
            cursor = mock_connection.cursor()
            print(test_case["issueId"])
            update_issue(cursor, test_case["issueId"], test_case["fields"])
        except Exception:
            assert test_case["expectedException"]
            continue
        assert not test_case["expectedException"]

        # Did the issue get updated?
        cursor.execute("""
            SELECT *
            FROM issue
            WHERE id = ?
        """, (test_case["issueId"],))

        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0]["title"] == test_case["expectedIssue"]["title"]
        assert results[0]["description"] == test_case["expectedIssue"]["description"]

        # Did the tags get updated?
        for tag in test_case["expectedTags"]:
            cursor.execute("""
                SELECT *
                FROM tag
                WHERE
                    issue_id = ? AND
                    namespace = ? AND
                    predicate = ? AND
                    value = ?
            """, (
                test_case["issueId"],
                tag["namespace"],
                tag["predicate"],
                tag["value"],
            ))
            assert len(cursor.fetchall()) == 1

        sqlite_teardown()

