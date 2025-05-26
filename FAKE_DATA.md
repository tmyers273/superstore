# Fake Data Generation for Development

This document describes the fake data generation functionality that helps developers work with realistic test data during development.

## Overview

The fake data generation system creates realistic sample tables and data when `APP_ENV=dev`. This includes:

- **fake_users**: User profiles with demographics and employment data
- **fake_orders**: E-commerce order data with products and pricing
- **fake_analytics**: Web analytics events and metrics
- **fake_products**: Product catalog with categories and inventory

The system is implemented in the `fake_data.py` module and automatically runs during server startup if the environment is set to development and the tables don't already exist.

## Environment Setup

To enable fake data generation, set the environment variable:

```bash
export APP_ENV=dev
```

The fake data will be automatically created when the server starts up if:
1. This environment variable is set to "dev"
2. The fake tables don't already exist

## Automatic Creation

The fake data creation happens automatically during server startup via the `create_db_and_tables()` function in `server.py`. The system:

1. Checks if `APP_ENV=dev`
2. Checks if any fake tables already exist
3. If both conditions are met (dev environment + no existing tables), creates all fake tables and data
4. If tables already exist, skips creation to avoid duplicates
5. If not in dev environment, skips entirely

This ensures that:
- Fake data is only created in development
- Existing data is preserved across server restarts
- No duplicate tables are created

## Generated Tables

### fake_users (1,000 rows)
- **Partition Key**: `department`
- **Sort Keys**: `id`
- **Columns**:
  - `id` (Int64): Unique user identifier
  - `first_name` (String): User's first name
  - `last_name` (String): User's last name
  - `email` (String): Email address
  - `age` (Int32): Age in years
  - `department` (String): Department (Engineering, Marketing, Sales, HR, Finance)
  - `salary` (Int64): Annual salary
  - `created_at` (Timestamp): Account creation date
  - `is_active` (Boolean): Account status

### fake_orders (5,000 rows)
- **Partition Key**: `status`
- **Sort Keys**: `order_date`, `id`
- **Columns**:
  - `id` (Int64): Unique order identifier
  - `user_id` (Int64): Reference to fake_users.id
  - `product_name` (String): Product name
  - `quantity` (Int32): Quantity ordered
  - `unit_price` (Float64): Price per unit
  - `total_amount` (Float64): Total order amount
  - `order_date` (Timestamp): Order date
  - `status` (String): Order status (pending, processing, shipped, delivered, cancelled)
  - `shipping_address` (String): Delivery address

### fake_analytics (10,000 rows)
- **Partition Keys**: `country`, `device_type`
- **Sort Keys**: `event_date`
- **Columns**:
  - `id` (Int64): Unique event identifier
  - `event_date` (Timestamp): Event timestamp
  - `user_id` (Int64): Reference to fake_users.id
  - `page_path` (String): Page URL path
  - `session_duration` (Int32): Session duration in seconds
  - `page_views` (Int32): Number of page views
  - `bounce_rate` (Float64): Bounce rate (0.0-1.0)
  - `conversion` (Boolean): Whether conversion occurred
  - `device_type` (String): Device type (desktop, mobile, tablet)
  - `browser` (String): Browser name
  - `country` (String): Country code

### fake_products (500 rows)
- **Partition Key**: `category`
- **Sort Keys**: `id`
- **Columns**:
  - `id` (Int64): Unique product identifier
  - `name` (String): Product name
  - `description` (String): Product description
  - `category` (String): Product category
  - `brand` (String): Brand name
  - `price` (Float64): Selling price
  - `cost` (Float64): Cost price
  - `stock_quantity` (Int32): Available inventory
  - `weight` (Float64): Product weight
  - `dimensions` (String): Product dimensions
  - `is_featured` (Boolean): Featured product flag
  - `created_at` (Timestamp): Product creation date

## API Endpoints

### Manual Data Creation
```http
POST /dev/create-fake-data
```
Manually trigger fake data creation. Requires authentication and `APP_ENV=dev`.

### Reset Fake Data
```http
DELETE /dev/reset-fake-data
```
Drop existing fake tables and recreate them with fresh data. Requires authentication and `APP_ENV=dev`.

### Check Status
```http
GET /dev/fake-data-status
```
Get detailed status of all fake tables including row counts and statistics. Requires authentication and `APP_ENV=dev`.

## Example Queries

Here are some example queries you can run against the fake data:

### User Demographics
```sql
SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
FROM fake_users 
GROUP BY department
ORDER BY avg_salary DESC
```

### Order Analysis
```sql
SELECT 
    status,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM fake_orders 
GROUP BY status
```

### Analytics Insights
```sql
SELECT 
    device_type,
    country,
    COUNT(*) as events,
    AVG(session_duration) as avg_session_duration,
    SUM(CASE WHEN conversion THEN 1 ELSE 0 END) as conversions
FROM fake_analytics 
GROUP BY device_type, country
ORDER BY events DESC
```

### Product Inventory
```sql
SELECT 
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price,
    SUM(stock_quantity) as total_inventory
FROM fake_products 
GROUP BY category
```

## Testing

You can test the fake data generation functionality using the provided test script:

```bash
python test_fake_data.py
```

This script will:
1. Test individual data generators
2. Create tables and insert data
3. Verify the data was created correctly
4. Display statistics about the generated data

## Module Structure

The fake data functionality is organized in the `fake_data.py` module with the following key functions:

- `generate_fake_*_data()`: Individual data generators for each table
- `get_fake_tables_config()`: Configuration for all fake tables
- `check_fake_tables_exist()`: Check if any fake tables already exist
- `create_fake_tables_and_data()`: Main function to create tables and data
- `reset_fake_data()`: Drop and recreate all fake tables
- `get_fake_data_status()`: Get status and statistics for all fake tables

## Data Characteristics

- **Reproducible**: Uses fixed random seeds for consistent data across runs
- **Realistic**: Names, emails, addresses, and other data follow realistic patterns
- **Relational**: Foreign key relationships between tables (e.g., orders reference users)
- **Partitioned**: Tables use appropriate partition keys for performance testing
- **Temporal**: Includes realistic date ranges and temporal patterns
- **Varied**: Different data types and distributions to test various scenarios

## Development Workflow

1. Set `APP_ENV=dev` in your environment
2. Start the server - fake data will be created automatically if tables don't exist
3. Use the fake tables for development and testing
4. Query the data using the `/execute` endpoint
5. Reset data as needed using the `/dev/reset-fake-data` endpoint
6. Restart the server - existing fake data will be preserved

## Security

- All fake data endpoints require authentication
- Endpoints only work when `APP_ENV=dev`
- No fake data is created in production environments
- Fake data is clearly labeled with "fake_" prefix
- Automatic creation only happens if tables don't already exist

## Customization

To add new fake tables or modify existing ones:

1. Create a new data generator function in `fake_data.py` (e.g., `generate_fake_inventory_data`)
2. Add the table configuration to `get_fake_tables_config()`
3. The new table will be automatically included in all operations
4. Update documentation for the new table

The data generators use the `polars` library and return DataFrames with the appropriate schema for insertion into the blob storage system. 