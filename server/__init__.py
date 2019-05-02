import os

from flask import Flask


def create_app():
    """Flask application factory. Constructs a Flask instance."""
    app = Flask(__name__, instance_relative_config=True)
    app.config["DATABASE"] = os.path.join(app.instance_path, "tissue.sqlite")

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # Initialize the database.
    from server.models import database
    database.init_database(app)
    database.init_app(app)

    # Register the blueprints.
    from server import issue
    app.register_blueprint(issue.blueprint)

    return app

