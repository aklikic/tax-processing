-- Create TaxProcessing database for PostgreSQL
-- This script sets up the database and schema

-- Create schema for tax processing tables
CREATE SCHEMA IF NOT EXISTS tax;

-- Set the default search path to include the tax schema
SET search_path TO tax, public;

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create sequences for ID generation
CREATE SEQUENCE IF NOT EXISTS tax.opening_balances_id_seq;
CREATE SEQUENCE IF NOT EXISTS tax.transactions_id_seq;

COMMENT ON SCHEMA tax IS 'Schema for tax processing tables and functions';

-- Log successful creation
\echo 'PostgreSQL TaxProcessing database schema created successfully.'