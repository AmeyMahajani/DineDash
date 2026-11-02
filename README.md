# DineDash – Food Delivery Data Engineering Capstone

An end-to-end food delivery data engineering project built using **Azure Databricks, PySpark, Spark SQL, Delta Lake and Lakeflow Pipelines**. The project demonstrates data ingestion, incremental processing, data transformation, data quality validation, analytical querying, dashboarding and workflow orchestration.

## 1. Project Overview

DineDash is a food delivery analytics case study designed to demonstrate how data from multiple operational sources can be integrated into a lakehouse for business reporting and analysis.

The dataset contains information about customers, restaurants, menu items, delivery agents, locations and food orders.

The project processes these datasets through a Bronze, Silver and Gold architecture, transforming raw files into structured datasets suitable for analytical queries and business dashboards.

This was completed as a structured Data Engineering training capstone. The exercise followed provided case-study requirements and a defined implementation workflow, with selected starter code and independently written SQL queries.

## 2. Business Objectives

The project focuses on answering business questions related to food delivery operations, including:

* Which restaurants receive the highest number of orders?
* Which menu items are ordered most frequently?
* Which cities generate the highest revenue?
* What are the daily revenue trends?
* Which customers place the most orders?
* How do delivery agents perform?
* Which payment methods are most commonly used?
* What proportion of orders are delivered, cancelled or failed?

## 3. Technology Stack

| Category            | Technologies                                          |
| ------------------- | ----------------------------------------------------- |
| Cloud Platform      | Azure Databricks                                      |
| Programming         | Python, PySpark, SQL                                  |
| Processing Engine   | Apache Spark                                          |
| Storage Format      | Delta Lake                                            |
| Data Ingestion      | `read_files()`, `COPY INTO`                           |
| Pipeline Processing | Lakeflow Pipelines (formerly Delta Live Tables / DLT) |
| Architecture        | Medallion Architecture                                |
| Governance          | Unity Catalog                                         |
| Orchestration       | Databricks Workflows                                  |
| Visualization       | Databricks AI/BI Dashboards                           |
| Development         | Databricks Notebooks, Git, GitHub                     |

## 4. Data Sources

The project uses six datasets representing different parts of the food delivery business.

### Dimension datasets

* Customers
* Restaurants
* Menu items
* Delivery agents
* Locations

### Transaction dataset

* Monthly food order records in JSON format, covering January–December 2024.

The source files are organized into separate directories within a Unity Catalog Volume:

```text
dinedash_volume/
│
├── dimensions/
│   ├── dim_customers.csv
│   ├── dim_delivery_agents.csv
│   ├── dim_locations.csv
│   ├── dim_menu_items.csv
│   └── dim_restaurants.csv
│
└── stream_data/
    ├── orders_2024_01.json
    ├── orders_2024_02.json
    ├── orders_2024_03.json
    └── ... orders_2024_12.json
```

## 5. Data Pipeline Architecture

The project follows the Medallion Architecture.

```text
                   SOURCE DATA
                        |
             -----------------------
             |                     |
       Dimension CSVs        Monthly Order JSON
             |                     |
             v                     v
       Bronze Tables        Bronze Order Table
             |                     |
             |              Incremental Ingestion
             |               COPY INTO / DLT
             |                     |
             -----------+-----------
                        |
                        v
                  SILVER LAYER
                        |
              Data Cleaning & Validation
                        |
              Dimension Enrichment
                        |
                 Joins & Explode
                        |
                        v
                   GOLD LAYER
                        |
             Business Aggregations
                        |
             Analytical SQL Queries
                        |
              ------------------
              |                |
              v                v
          Dashboards       Workflow
```

### Bronze Layer – Raw Data Ingestion

The Bronze layer stores the source datasets as Delta tables.

Implementation includes:

* Reading CSV and JSON files using `read_files()`.
* Creating temporary SQL views for dimension datasets.
* Defining explicit schemas for dimension tables.
* Creating Bronze Delta tables.
* Recording ingestion metadata, including source file information and ingestion timestamps.
* Loading monthly order files incrementally using `COPY INTO`.
* Using Lakeflow/DLT streaming table definitions for order processing.

### Silver Layer – Data Cleaning and Transformation

The Silver layer contains cleaned and enriched datasets.

Implementation includes:

* Filtering records with missing mandatory attributes.
* Applying data quality expectations.
* Validating incoming order records.
* Joining orders with customer, restaurant, delivery-agent, menu-item and location datasets.
* Exploding the nested `items_ordered` array for item-level analysis.
* Creating enriched datasets for customer, agent, item and location analysis.

### Gold Layer – Business-Level Analytics

The Gold layer contains aggregated datasets designed for reporting and analytical consumption.

Key datasets include:

| Gold Dataset                  | Purpose                                                  |
| ----------------------------- | -------------------------------------------------------- |
| `total_orders_per_restaurant` | Measures restaurant-wise order volume                    |
| `top_selling_items`           | Identifies frequently ordered menu items                 |
| `avg_fees_by_city`            | Analyses city-level restaurant ratings and tips          |
| `agent_order_counts`          | Measures delivery-agent order volume and average ratings |
| `daily_revenue_trend`         | Tracks daily revenue                                     |
| `customer_ltv`                | Calculates customer lifetime spending                    |
| `customer_order_counts`       | Measures customer ordering frequency                     |

## 6. Incremental Data Ingestion

The project demonstrates incremental processing of monthly order files.

`COPY INTO` was used to load monthly order data into the Bronze table while verifying the number of records inserted.

The Lakeflow/DLT pipeline was also used to process order data in stages:

* Initial processing of January–June 2024 data.
* Incremental processing of July–August 2024 data.
* Subsequent processing of September–December 2024 data.

This demonstrates how additional source files can be introduced and processed without treating every monthly load as an entirely new historical dataset.

## 7. Data Quality

Data quality checks and expectations were applied during processing.

The implementation includes:

* Mandatory-field validation.
* Null-value checks.
* Filtering invalid dimension records.
* Order enrichment validation.
* Data quality expectations in the Silver pipeline.

### Observed Data Quality Result

During the July–August incremental processing run:

* 58,962 records (75.5%) were written.
* 19,154 records (24.5%) were dropped.

The rejected records failed the `valid_restaurant_name` expectation. The `valid_agent` and `valid_item` expectations recorded no failures during that run.

This highlighted the importance of validating reference data before using it to enrich transaction records.

## 8. SQL Analytics

The project includes two sets of analytical SQL exercises:

**Order Data Analysis – 12 Queries**

Examples include:

* Total order count.
* Order distribution by payment method.
* Order-status distribution.
* Restaurant-wise order volume.
* Customer ordering frequency.
* Top orders by total amount.
* Average tips by payment method.
* Item-level analysis using `EXPLODE()`.

**Orders and Dimension Analysis – 15 Queries**

Examples include:

* Customer and restaurant analysis.
* Cuisine-wise average order value.
* Delivery-agent performance.
* Revenue by location and cuisine.
* Customer lifetime value.
* Repeat-customer analysis.
* Restaurant performance by location.
* Payment preferences across cuisines.

These queries use joins, aggregations, grouping, filtering, nested-array transformations and analytical SQL functions.

## 9. Dashboard and Workflow Orchestration

A Databricks AI/BI dashboard named **Dine Dash Metrics Dashboard** was created to present business metrics.

The dashboard includes visualizations based on the Gold datasets, including:

* Total orders per restaurant.
* Top-selling menu items.
* Daily revenue trends.
* Overall revenue metrics.

A Databricks Workflow was configured to automate the execution sequence:

```text
DineDash Workflow
       |
       v
Lakeflow / DLT Pipeline
       |
       v
Dashboard Refresh
```

The workflow was executed and both pipeline and dashboard tasks were verified.

## 10. Repository Structure

```text
DineDash/
│
├── DLT/
│   ├── Dinedash_DLT_PL.ipynb
│   └── Dinedash_DLT_PL.html
│
├── Databricks_caseStudy_Dataset/
│   ├── dimensions/
│   │   ├── dim_customers.csv
│   │   ├── dim_delivery_agents.csv
│   │   ├── dim_locations.csv
│   │   ├── dim_menu_items.csv
│   │   └── dim_restaurants.csv
│   │
│   └── stream_data/
│       └── Monthly order JSON files
│
├── SQL_MAIN/
│   ├── html/
│   │   ├── 01_direct_read.html
│   │   ├── 02_views_and_tables.html
│   │   ├── 03_12_queries.html
│   │   └── 04_15_queries.html
│   │
│   └── ipynb/
│       ├── 01_direct_read.ipynb
│       ├── 02_views_and_tables.ipynb
│       ├── 03_12_queries.ipynb
│       └── 04_15_queries.ipynb
│
└── Documentation/
    ├── Capstone.docx
    ├── DineDash_Capstone_Report_Amey.docx
    └── DineDash_Capstone_Report_Amey.pdf
```

## 11. Execution and Setup

The notebooks were developed and executed in an Azure Databricks workspace.

To explore or reproduce the project:

1. Clone or download this repository.
2. Open an Azure Databricks workspace with the required permissions.
3. Create or identify a Unity Catalog catalog and schema.
4. Create a Unity Catalog Volume for the project.
5. Upload the dimension CSV and monthly order JSON files into the corresponding directories.
6. Import the SQL notebooks from `SQL_MAIN/ipynb/`.
7. Update the volume, catalog and schema paths to match your workspace.
8. Execute the SQL notebooks in numerical order.
9. Import and configure the Lakeflow/DLT notebook.
10. Configure and execute the pipeline.
11. Create the AI/BI dashboard and Databricks Workflow.

**Note:** Catalog names, volume paths and workspace-specific identifiers used during development must be adapted to the target Databricks environment before execution.

## 12. Key Data Engineering Concepts Demonstrated

* Medallion Architecture (Bronze, Silver and Gold)
* Delta Lake tables
* Incremental data ingestion
* `COPY INTO`
* `read_files()`
* Lakeflow Pipelines / Delta Live Tables
* Schema definition and data validation
* Data quality expectations
* PySpark and Spark SQL transformations
* Joins and aggregations
* Nested JSON processing
* `EXPLODE()` for array transformations
* Unity Catalog Volumes
* Databricks Workflows
* Analytical dashboards

## 13. Project Outcome

The DineDash capstone demonstrates an end-to-end food delivery data processing workflow on Azure Databricks.

The project covers source-data ingestion, Bronze Delta table creation, incremental order processing, Silver-layer cleaning and enrichment, Gold-layer analytical transformations, SQL-based business analysis, data quality validation, dashboard development and workflow orchestration.

It provides practical exposure to the fundamental components of a Databricks-based lakehouse architecture.
