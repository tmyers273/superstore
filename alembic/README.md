# Database Migrations with Alembic

This project uses [Alembic](https://alembic.sqlalchemy.org/) for database schema migrations.

## Setup

Alembic is already configured and ready to use. The configuration files are:
- `alembic.ini` - Main configuration file
- `alembic/env.py` - Environment configuration that imports our SQLAlchemy models

## Common Commands

### Check current migration status
```bash
uv run alembic current
```

### View migration history
```bash
uv run alembic history
```

### Create a new migration
```bash
# Auto-generate migration from model changes
uv run alembic revision --autogenerate -m "Description of changes"

# Create empty migration for manual editing
uv run alembic revision -m "Description of changes"
```

### Apply migrations
```bash
# Apply all pending migrations
uv run alembic upgrade head

# Apply specific migration
uv run alembic upgrade <revision_id>
```

### Rollback migrations
```bash
# Rollback one migration
uv run alembic downgrade -1

# Rollback to specific revision
uv run alembic downgrade <revision_id>

# Rollback all migrations
uv run alembic downgrade base
```

## Current Schema

The current migration adds support for table status tracking:
- `partition_keys` (JSON) - For physical partitioning
- `sort_keys` (JSON) - For sorting within micropartitions  
- `status` (VARCHAR) - Table lifecycle status ('active' or 'dropped')

## Notes

- The database URL is configured to use `sqlite:///scratch/db.db`
- All existing tables are automatically marked as 'active' when the migration is applied
- SQLite has limited ALTER TABLE support, so complex schema changes may require manual migrations 