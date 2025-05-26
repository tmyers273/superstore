# Fake Data Seeding Guide

This guide explains how to seed your development environment with fake data for testing and development purposes.

## Quick Start

To create fake data tables in your development environment:

1. **Set environment to development:**
   ```bash
   export APP_ENV=dev
   ```

2. **Run the migration and seed script:**
   ```bash
   python migrate_and_seed.py
   ```

3. **Verify the data was created:**
   ```bash
   python check_fake_data.py
   ```

## What Gets Created

The seeding process creates 4 realistic tables with a total of **16,500 rows**:

- **fake_users** (1,000 rows): User profiles with demographics and employment data
- **fake_orders** (5,000 rows): E-commerce order data with products and pricing  
- **fake_analytics** (10,000 rows): Web analytics events and metrics
- **fake_products** (500 rows): Product catalog with categories and inventory

## Scripts Available

### `migrate_and_seed.py`
- **Purpose**: Main script to migrate database schema and create fake data
- **Features**: 
  - Automatically adds missing database columns (like `status`)
  - Creates all fake tables and data
  - Safe to run multiple times (won't duplicate data)
- **Usage**: `python migrate_and_seed.py`

### `check_fake_data.py`
- **Purpose**: Verify fake data status and get detailed statistics
- **Features**:
  - Shows row counts, micropartitions, and file sizes
  - Displays partition and sort keys
  - Provides summary statistics
- **Usage**: `python check_fake_data.py`

## Database Schema Migration

The `migrate_and_seed.py` script automatically handles database schema updates:

- Ensures all required tables exist
- Adds missing columns (particularly the `status` column in the `tables` table)
- Uses SQLAlchemy's `create_all()` to safely update schema
- Handles SQLite-specific column additions

## Environment Requirements

- `APP_ENV=dev` must be set (fake data only works in development)
- `DATA_DIR` must be configured (where database and files are stored)
- All dependencies from `requirements.txt` must be installed

## Data Characteristics

- **Reproducible**: Uses fixed random seeds for consistent data
- **Realistic**: Names, emails, addresses follow realistic patterns
- **Relational**: Foreign key relationships between tables
- **Partitioned**: Tables use appropriate partition keys for performance
- **Temporal**: Realistic date ranges and temporal patterns

## File Structure

After seeding, your data directory will contain:
```
ams_scratch/
├── db.db                          # SQLite database with metadata
├── fake_users/mps/                # User data micropartitions
├── fake_orders/mps/               # Order data micropartitions  
├── fake_analytics/mps/            # Analytics data micropartitions
└── fake_products/mps/             # Product data micropartitions
```

## Troubleshooting

### "no such column: status" Error
This indicates the database schema is outdated. Run `migrate_and_seed.py` which will automatically add the missing column.

### "APP_ENV must be set to 'dev'" Error
Ensure you've exported the environment variable:
```bash
export APP_ENV=dev
```

### "DATA_DIR is not configured" Error
Check that your `.env` file contains a valid `DATA_DIR` setting, or that the environment variable is set.

## Integration with Server

The fake data integrates seamlessly with the main server:

- Server automatically creates fake data on startup if `APP_ENV=dev` and tables don't exist
- API endpoints available at `/dev/create-fake-data`, `/dev/reset-fake-data`, `/dev/fake-data-status`
- Use the `/execute` endpoint to query the fake data
- Browse tables through the `/databases` endpoint

## Next Steps

After seeding:

1. Start the server: `python server.py`
2. Use the web interface to browse and query the fake data
3. Test your application features with realistic data
4. Use the fake data for development and testing scenarios

The fake data provides a solid foundation for development work without needing to set up complex data pipelines or use production data. 