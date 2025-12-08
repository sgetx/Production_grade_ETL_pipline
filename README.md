# Production_grade_ETL_pipline

# 1. Bronze Layer (Ingestion & Raw Data)
Objective: Ingest raw data from external sources into the data lake incrementally.

Source: Azure Data Lake Storage (ADLS) Gen2 containing retail data (Orders, Customers, Products) in Parquet format.

Key Technology: Auto Loader (cloudFiles) ensures efficient incremental ingestion by detecting new files as they arrive.

Implementation Details:

Dynamic Ingestion: A single, parameterized notebook handles ingestion for multiple tables (orders, customers, products) using Databricks Widgets.

Schema Evolution: Utilizes schemaHints and schemaLocation to handle schema changes gracefully.

Data Writing: Writes data in Parquet format to the Bronze container with checkpointing enabled to ensure exactly-once processing.

Orchestration: A Databricks Workflow loops through a list of tables and triggers the dynamic notebook for each.

# 2. Silver Layer (Transformation & Cleansing)
Objective: Clean raw data, apply business logic, and store it in efficient Delta format.

Transformations:

Data Cleaning: Drops technical columns like _rescued_data created by Auto Loader.

Date Handling: Converts string dates to proper Timestamp formats (e.g., order_date).

Derived Columns:

Customers: Splits email addresses to extract domain and creates a full_name column.

Orders: Extracts year from timestamps for partitioning.

User Defined Functions (UDFs): Uses Unity Catalog to create reusable SQL and Python UDFs (e.g., calculating discounted prices, formatting text) applied directly to DataFrames.

Storage: Data is written to the Silver container in Delta format (format("delta")), enabling ACID transactions.

# 3. Gold Layer (Business Logic & Star Schema)
Objective: Organize data into a Star Schema (Fact and Dimensions) for reporting.

A. Dimension: Customers (SCD Type 1 - Upsert)
Logic: Implements Slowly Changing Dimension (SCD) Type 1, where existing records are updated, and new records are inserted (no history preservation).

Surrogate Keys: Generates a dim_customer_key using monotonically_increasing_id and logic to ensure keys increment correctly across batch loads.

Upsert Implementation: Uses DeltaTable.merge() to perform the Upsert (Update + Insert) operation based on the primary key.

Audit Columns: Adds create_date and update_date timestamps.

B. Dimension: Products (SCD Type 2 - History Tracking)
Technology: Delta Live Tables (DLT).

Logic: Implements Slowly Changing Dimension (SCD) Type 2, which creates a new record for every update to preserve historical data.

Declarative ETL:

Uses the dlt.apply_changes() API to define target, source, keys, and stored_as_scd_type="2".

Automatically manages __START_AT and __END_AT columns to track record validity validity periods.

Data Quality: Applies DLT Expectations (e.g., @dlt.expect_or_drop("valid_id", "id IS NOT NULL")) to ensure data integrity by dropping bad records.

C. Fact Table: Orders
Logic: Aggregates transactional data and links it to dimensions.

Join Logic: Reads Silver Orders and joins with Gold Customers and Gold Products to retrieve Surrogate Keys.

Refinement: Replaces natural keys (e.g., customer_id) with Surrogate Keys (dim_customer_key) to optimize join performance in reporting tools.

Storage: Stored as a Delta table with Upsert logic enabled to handle order updates.
