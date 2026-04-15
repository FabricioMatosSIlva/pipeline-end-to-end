# E-Commerce Data Pipeline (End-to-End ETL Architecture)

A complete ETL (Extract-Transform-Load) data pipeline that ingests e-commerce data from the Fake Store API, transforms it through a multi-layer architecture, and loads it into a PostgreSQL data warehouse with a dimensional star schema.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Getting Started](#getting-started)
- [Pipeline Execution](#pipeline-execution)
- [Data Layers](#data-layers)
- [Logging and Monitoring](#logging-and-monitoring)

## Overview

This project demonstrates a modern data engineering pipeline that follows the **Medallion Architecture** pattern with three distinct layers:

1. **RAW Layer** - Stores original JSON data from API endpoints with timestamps for versioning
2. **TRUSTED Layer** - Contains validated and transformed data in Apache Parquet format
3. **ANALYTICAL Layer** - PostgreSQL database with a dimensional star schema optimized for analytics

### Data Source

The pipeline extracts data from the [Fake Store API](https://fakestoreapi.com/), a free REST API that provides mock e-commerce data including:

- **Products**: 20 items with titles, prices, categories, and ratings
- **Users**: 10 user profiles with addresses and contact information
- **Carts**: 7 shopping carts with product references and quantities

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│  Fake Store API │────▶│   RAW Layer      │────▶│  TRUSTED Layer  │────▶│  PostgreSQL DW   │
│  (REST API)     │     │  (JSON files)    │     │  (Parquet)      │     │  (Star Schema)   │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └──────────────────┘
        │                       │                        │                        │
        │                       │                        │                        │
   extract_api.py          Timestamped            transform.py            load_postgres.py
                           versioning             Schema validation       Dimensional model
                                                  Data normalization      Fact & Dimension tables
```

### Pipeline Stages

| Stage | Script | Input | Output | Description |
|-------|--------|-------|--------|-------------|
| **Extract** | `extract_api.py` | Fake Store API | `data/raw/*.json` | Fetches data from API endpoints and saves as timestamped JSON files |
| **Transform** | `transform.py` | `data/raw/*.json` | `data/trusted/*.parquet` | Validates schema, normalizes nested data, converts types |
| **Load** | `load_postgres.py` | `data/trusted/*.parquet` | PostgreSQL tables | Loads data into dimensional model with calculated metrics |

## Technologies

| Category | Technology | Purpose |
|----------|------------|---------|
| **Language** | Python 3.11 | Core programming language |
| **Data Processing** | Pandas | DataFrame manipulation and transformation |
| **File Format** | Apache Parquet (PyArrow) | Columnar storage for trusted layer |
| **Database** | PostgreSQL 15 | Data warehouse backend |
| **ORM** | SQLAlchemy | Database interactions and connection pooling |
| **HTTP Client** | Requests | API data extraction |
| **Containerization** | Docker & Docker Compose | Environment orchestration |
| **Configuration** | python-dotenv | Environment variable management |

## Project Structure

```
data-pipeline-end-to-end/
│
├── docker/
│   ├── docker-compose.yml      # Multi-container orchestration
│   └── Dockerfile              # Container image definition
│
├── sql/
│   └── create_tables.sql       # Database schema (dimensional model)
│
├── src/
│   ├── ingestion/
│   │   └── extract_api.py      # Step 1: Extract from API
│   ├── processing/
│   │   └── transform.py        # Step 2: Transform to Parquet
│   └── load/
│       └── load_postgres.py    # Step 3: Load to PostgreSQL
│
├── data/
│   ├── raw/                    # RAW layer (JSON with timestamps)
│   └── trusted/                # TRUSTED layer (Parquet files)
│
├── logs/                       # Pipeline execution logs
│   ├── ingestion.log
│   ├── transform.log
│   └── load.log
│
├── requirements.txt            # Python dependencies
└── README.md
```

## Data Model

The analytical layer implements a **Star Schema** optimized for business intelligence queries:

### Dimension Tables

**dim_products**
| Column | Type | Description |
|--------|------|-------------|
| product_id | INT (PK) | Unique product identifier |
| title | TEXT | Product name |
| category_name | TEXT | Product category |
| price_usd | NUMERIC(10,2) | Price in USD |

**dim_users**
| Column | Type | Description |
|--------|------|-------------|
| user_id | INT (PK) | Unique user identifier |
| email | TEXT | User email address |
| username | TEXT | User login name |
| city | TEXT | User city from address |
| country | TEXT | User country |

### Fact Table

**fact_carts**
| Column | Type | Description |
|--------|------|-------------|
| cart_id | INT (PK) | Unique cart identifier |
| user_id | INT (FK) | Reference to dim_users |
| cart_date | DATE | Date of cart creation |
| total_items | INT | Calculated total items in cart |

## Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Python 3.11+ (for local development)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data-pipeline-end-to-end
   ```

2. **Start the infrastructure**
   ```bash
   cd docker
   docker-compose up -d
   ```
   This will start:
   - PostgreSQL 15 database on port 5432
   - ETL container with Python 3.11

3. **Create database tables**
   ```bash
   docker exec -it fake_store_postgres psql -U de_user -d fakestore_dw -f /app/sql/create_tables.sql
   ```
   Or connect manually and run the SQL script.

### Environment Variables

The pipeline uses the following environment variables (with defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | localhost | Database host |
| `POSTGRES_DB` | fakestore_dw | Database name |
| `POSTGRES_USER` | de_user | Database user |
| `POSTGRES_PASSWORD` | de_password | Database password |
| `POSTGRES_PORT` | 5432 | Database port |

## Pipeline Execution

### Running the Complete Pipeline

Execute each stage sequentially:

```bash
# Step 1: Extract data from API
python src/ingestion/extract_api.py

# Step 2: Transform raw data to trusted layer
python src/processing/transform.py

# Step 3: Load data into PostgreSQL
python src/load/load_postgres.py
```

### Running with Docker

```bash
# Enter the ETL container
docker exec -it fake_store_etl bash

# Run the pipeline scripts
python src/ingestion/extract_api.py
python src/processing/transform.py
python src/load/load_postgres.py
```

### Verifying the Results

Connect to PostgreSQL and query the data:

```bash
docker exec -it fake_store_postgres psql -U de_user -d fakestore_dw
```

```sql
-- Check loaded records
SELECT COUNT(*) FROM dim_products;  -- Expected: 20
SELECT COUNT(*) FROM dim_users;     -- Expected: 10
SELECT COUNT(*) FROM fact_carts;    -- Expected: 7

-- Sample analytical query
SELECT
    u.username,
    u.city,
    COUNT(c.cart_id) as total_carts,
    SUM(c.total_items) as total_items_purchased
FROM fact_carts c
JOIN dim_users u ON c.user_id = u.user_id
GROUP BY u.username, u.city
ORDER BY total_items_purchased DESC;
```

## Data Layers

### RAW Layer (`data/raw/`)

- **Format**: JSON
- **Naming Convention**: `{endpoint}_{YYYYMMDD}_{HHMMSS}.json`
- **Purpose**: Preserve original data with full history
- **Features**:
  - Timestamped files for versioning
  - No transformations applied
  - Enables reprocessing from source

### TRUSTED Layer (`data/trusted/`)

- **Format**: Apache Parquet
- **Files**: `products.parquet`, `users.parquet`, `carts.parquet`
- **Purpose**: Validated and transformed data ready for analysis
- **Features**:
  - Schema validation
  - Normalized nested structures
  - Standardized column names
  - Optimized columnar storage

### ANALYTICAL Layer (PostgreSQL)

- **Format**: Relational tables
- **Schema**: Star schema with facts and dimensions
- **Purpose**: Business intelligence and reporting
- **Features**:
  - Foreign key relationships
  - Calculated metrics
  - Query-optimized structure

## Logging and Monitoring

All pipeline stages generate detailed logs in the `logs/` directory:

| Log File | Stage | Contents |
|----------|-------|----------|
| `ingestion.log` | Extract | API calls, file saves, timestamps |
| `transform.log` | Transform | Validation results, record counts, transformations |
| `load.log` | Load | Database operations, insert counts |

**Log Format**: `%(asctime)s - %(levelname)s - %(message)s`

### Sample Log Output

```
2024-01-15 10:30:45 - INFO - Starting data extraction from Fake Store API
2024-01-15 10:30:46 - INFO - Successfully fetched 20 products
2024-01-15 10:30:46 - INFO - Data saved to data/raw/products_20240115_103046.json
```

## Key Features

- **Versioned Data Ingestion**: Timestamped raw files enable historical tracking and reprocessing
- **Schema Validation**: Transform layer validates required columns before processing
- **Error Handling**: Custom exceptions and comprehensive logging for debugging
- **Dimensional Modeling**: Clean star schema optimized for analytical queries
- **Containerized Environment**: Docker ensures consistent execution across environments
- **Flexible Configuration**: Environment variables for database connection settings
- **Multi-Format Support**: JSON for raw data, Parquet for trusted layer, PostgreSQL for analytics


## License

This project is for educational and demonstration purposes.
