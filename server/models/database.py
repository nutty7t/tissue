import sqlite3

from flask import current_app, g


def init_database(app):
    """
    Create SQLite tables in the database if they don't already exist. Intended
    to be called from within the application factory.

    :param app: Flask instance
    """
    with app.open_resource("models/schema.sql") as f:
        connection = sqlite3.connect(app.config["DATABASE"])
        connection.executescript(f.read().decode("utf-8"))
        connection.close()


def get_database():
    """Get a SQLite database connection from the application context."""
    if "database" not in g:
        g.database = sqlite3.connect(current_app.config["DATABASE"])
        g.database.row_factory = sqlite3.Row

    return g.database


def close_database(exception=None):
    """Close the database connection resource in the application context."""
    database = g.pop("database", None)
    if database is not None:
        database.close()


def init_app(app):
    """Register teardown function with the application factory."""
    app.teardown_appcontext(close_database)

