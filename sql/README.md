# Database Setup for Tax Processing System

This guide explains how to set up and populate the SQL Server database for testing the tax processing system.

**Connection Details:**
- **Host:** localhost
- **Port:** 5432
- **Database:** TaxProcessing
- **Username:** taxuser
- **Password:** TaxProcessing123!
- **Web Interface:** http://localhost:8080 (Adminer)

## Quick Start

1. **Start the database**:
```bash
docker-compose -f docker-compose-postgresql.yml up -d
```

2. **Wait for database to be ready** (check health status):
```bash
docker-compose -f docker-compose-postgresql.yml ps
```

3. **Connect to database**:

```bash
# Interactive psql session (recommended):
docker exec -it tax-processing-postgresql psql -U taxuser -d TaxProcessing
```

4. **Run schema and data generation**:

Create database:
```bash
docker exec -i tax-processing-postgresql psql -U taxuser -d TaxProcessing < postgresql/01_create_database.sql
```
Create schema:
```bash
docker exec -i tax-processing-postgresql psql -U taxuser -d TaxProcessing < postgresql/02_create_schema.sql
```
Generate test data:
```bash
docker exec -i tax-processing-postgresql psql -U taxuser -d TaxProcessing < postgresql/03_generate_test_data.sql
```

## Cleanup

### Stop Database
```bash
docker-compose -f docker-compose-postgresql.yml down -v
```