-- Runs once, when the Postgres volume is first created.
-- The Prefect server keeps its own metadata in a separate database.
CREATE DATABASE prefect;
