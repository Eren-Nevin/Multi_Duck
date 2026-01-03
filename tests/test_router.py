"""Tests for query routing logic."""

import pytest

from multi_duck.router import QueryType, classify_query


class TestClassifyQuery:
    """Tests for the classify_query function."""

    def test_select_is_read(self):
        """SELECT queries should be classified as READ."""
        assert classify_query("SELECT * FROM users") == QueryType.READ
        assert classify_query("SELECT id, name FROM users WHERE age > 25") == QueryType.READ
        assert classify_query("  SELECT * FROM users  ") == QueryType.READ

    def test_select_with_comments(self):
        """SELECT with comments should be classified as READ."""
        assert classify_query("-- comment\nSELECT * FROM users") == QueryType.READ
        assert classify_query("/* comment */ SELECT * FROM users") == QueryType.READ

    def test_with_cte_is_read(self):
        """WITH (CTE) queries should be classified as READ."""
        query = """
        WITH active_users AS (
            SELECT * FROM users WHERE active = true
        )
        SELECT * FROM active_users
        """
        assert classify_query(query) == QueryType.READ

    def test_explain_is_read(self):
        """EXPLAIN queries should be classified as READ."""
        assert classify_query("EXPLAIN SELECT * FROM users") == QueryType.READ
        assert classify_query("EXPLAIN ANALYZE SELECT * FROM users") == QueryType.READ

    def test_show_is_read(self):
        """SHOW queries should be classified as READ."""
        assert classify_query("SHOW TABLES") == QueryType.READ
        assert classify_query("SHOW ALL TABLES") == QueryType.READ

    def test_describe_is_read(self):
        """DESCRIBE queries should be classified as READ."""
        assert classify_query("DESCRIBE users") == QueryType.READ

    def test_insert_is_write(self):
        """INSERT queries should be classified as WRITE."""
        assert classify_query("INSERT INTO users VALUES (1, 'test')") == QueryType.WRITE
        assert classify_query("INSERT INTO users (id, name) SELECT * FROM temp") == QueryType.WRITE

    def test_update_is_write(self):
        """UPDATE queries should be classified as WRITE."""
        assert classify_query("UPDATE users SET name = 'test' WHERE id = 1") == QueryType.WRITE

    def test_delete_is_write(self):
        """DELETE queries should be classified as WRITE."""
        assert classify_query("DELETE FROM users WHERE id = 1") == QueryType.WRITE

    def test_create_is_write(self):
        """CREATE queries should be classified as WRITE."""
        assert classify_query("CREATE TABLE users (id INT, name VARCHAR)") == QueryType.WRITE
        assert classify_query("CREATE INDEX idx ON users(id)") == QueryType.WRITE
        assert classify_query("CREATE VIEW active_users AS SELECT * FROM users") == QueryType.WRITE

    def test_drop_is_write(self):
        """DROP queries should be classified as WRITE."""
        assert classify_query("DROP TABLE users") == QueryType.WRITE
        assert classify_query("DROP VIEW active_users") == QueryType.WRITE

    def test_alter_is_write(self):
        """ALTER queries should be classified as WRITE."""
        assert classify_query("ALTER TABLE users ADD COLUMN email VARCHAR") == QueryType.WRITE

    def test_truncate_is_write(self):
        """TRUNCATE queries should be classified as WRITE."""
        assert classify_query("TRUNCATE TABLE users") == QueryType.WRITE

    def test_vacuum_is_write(self):
        """VACUUM queries should be classified as WRITE."""
        assert classify_query("VACUUM") == QueryType.WRITE

    def test_checkpoint_is_write(self):
        """CHECKPOINT queries should be classified as WRITE."""
        assert classify_query("CHECKPOINT") == QueryType.WRITE

    def test_copy_is_write(self):
        """COPY queries should be classified as WRITE."""
        assert classify_query("COPY users TO 'users.csv'") == QueryType.WRITE

    def test_case_insensitive(self):
        """Query classification should be case-insensitive."""
        assert classify_query("select * from users") == QueryType.READ
        assert classify_query("SELECT * FROM users") == QueryType.READ
        assert classify_query("insert into users values (1)") == QueryType.WRITE
        assert classify_query("INSERT INTO users VALUES (1)") == QueryType.WRITE
