-- For whatever reason, SQLite does not enforce foreign key constraints by
-- default, so we're going to enable this option here. The contents of this
-- file should get executed at the beginning of every database connection.
PRAGMA foreign_keys = 1;

-- Project Table
CREATE TABLE IF NOT EXISTS issue (
	id integer PRIMARY KEY,
	title TEXT NOT NULL,
	description TEXT
);

-- Tag Table
CREATE TABLE IF NOT EXISTS tag (
	id integer PRIMARY KEY,
	namespace TEXT NOT NULL,
	predicate TEXT NOT NULL,
	value TEXT NOT NULL,
	issue_id integer NOT NULL,
	FOREIGN KEY (issue_id) REFERENCES issue (id)
);

