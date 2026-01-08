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
docker exec -it tax-processing-postgresql psql -U postgres -d postgres
```

4. **Run schema and data generation**:

Docker-based PostgreSQL:
```bash
docker exec -i tax-processing-postgresql psql -U postgres -d postgres < postgresql/01_create_database.sql
docker exec -i tax-processing-postgresql psql -U postgres -d postgres < postgresql/02_create_schema.sql
docker exec -i tax-processing-postgresql psql -U postgres -d postgres < postgresql/03_generate_test_data.sql
```

Local PostgreSQL installation:
```bash
psql -h 34.73.113.145 -p 5432 -U postgres -d postgres -f postgresql/01_create_database.sql
psql -h 34.73.113.145 -p 5432 -U postgres -d postgres -f postgresql/02_create_schema.sql
psql -h 34.73.113.145 -p 5432 -U postgres -d postgres -f postgresql/03_generate_test_data.sql
```

## Cleanup

### Stop Database
```bash
docker-compose -f docker-compose-postgresql.yml down -v
```