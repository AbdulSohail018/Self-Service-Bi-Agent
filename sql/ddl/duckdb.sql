-- DuckDB setup script for local development
-- This script is used by the bootstrap script to set up the initial database structure

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS seeds;

-- Note: Tables will be created by dbt models and seeds
-- This file can be extended with additional DuckDB-specific setup

-- Enable some useful DuckDB settings
PRAGMA enable_progress_bar;
PRAGMA threads=4;