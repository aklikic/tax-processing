-- Create TaxProcessing database
-- This script runs automatically when the container starts

USE master;
GO

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'TaxProcessing')
BEGIN
    CREATE DATABASE TaxProcessing;
END
GO

USE TaxProcessing;
GO

-- Create schema for tax processing tables
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'tax')
BEGIN
    EXEC('CREATE SCHEMA tax');
END
GO