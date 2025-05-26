# Database Migrations

This project uses Alembic for database schema migrations. Migrations are automatically run on deployment.

## How it works

1. **Environment Configuration**: The database URL is configured via environment variables:
   - `DATA_DIR`: Directory where the database file will be stored (defaults to ".")
   - `DATABASE_URL`: Full database URL (defaults to `sqlite+aiosqlite:///{DATA_DIR}/db.db`)

2. **Automatic Migration on Deploy**: The `start.sh` script runs `alembic upgrade head` before starting the server, ensuring the database schema is always up to date.

3. **Docker Integration**: The Dockerfile sets up the environment and ensures the data directory exists with proper permissions.

## Migration Files

- `4aee20fc87fb_initial_migration_create_base_tables.py`: Creates all base tables (users, databases, schemas, tables, etc.)
- `c17f1101ab6d_add_status_column_to_tables.py`: Adds partition_keys, sort_keys, and status columns to the tables table

## Development

To create a new migration:
```bash
uv run alembic revision --autogenerate -m "Description of changes"
```

To run migrations manually:
```bash
uv run alembic upgrade head
```

To check current migration status:
```bash
uv run alembic current
```

## Environment Variables

- `DATA_DIR=/data` (set in Docker)
- `DATABASE_URL` (automatically constructed from DATA_DIR if not set)

The migrations will create the database file at `{DATA_DIR}/db.db` and ensure all tables are created with the correct schema. 