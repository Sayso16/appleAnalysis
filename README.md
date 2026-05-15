# appleAnalysis
Apple Product Purchase Analysis - ETL Pipeline
# Apple Product Purchase Analysis - ETL Pipeline

A production-grade ETL pipeline built with **Apache Spark (PySpark)** on **Databricks** that analyzes customer purchase patterns for Apple products (iPhone and AirPods).

## Project Overview

This ETL pipeline extracts transaction and customer data from multiple sources (CSV files and Delta tables), applies three different transformation logics to identify customer purchase patterns, and loads the results back to cloud storage in optimized formats.

The pipeline identifies three distinct customer segments:

| Analysis | Description | Business Value |
|----------|-------------|----------------|
| **AirPods After iPhone** | Customers who bought AirPods immediately after purchasing an iPhone | Identify cross-sell opportunities |
| **Only AirPods and iPhone** | Customers who exclusively bought only these two products | Target brand-loyal customers |
| **Both Products** | Customers who bought both AirPods and iPhone (any order) | Product bundling strategies |

## Technology Stack

- **Apache Spark (PySpark)** - Distributed data processing engine
- **Databricks** - Unified data analytics platform
- **Delta Lake** - ACID transactions and time travel capabilities
- **Unity Catalog** - Data governance and table management
- **Python** - Core programming language

## Project Structure

## 📁 Project Structure

```
apple-purchase-etl/
│
├── code/
│   ├── reader_factory.py # Extract layer: Factory pattern for CSV/Parquet/Delta sources
│   ├── extractor.py # Extractor implementations
│   ├── transform.py # Transform layer: Business logic transformers
│   ├── loader_factory.py # Load layer: Factory pattern for data sinks
│   ├── loader.py # Loader implementations
│   └── apple_analysis.py # Workflow orchestrator and runner
│
├── data/
│   └── Customer_Updated.csv
|   └── Products_Updated.csv
|   └── Transaction_Updated.csv
│
└── README.md # Project documentation
```
## ETL Architecture

The pipeline follows a modular ETL design with three distinct layers:
```
┌─────────────────────────────────────────────────────────────────┐
│ EXTRACT LAYER                                                   │
│ • Reader Factory Pattern                                        │
│ • Supports: CSV, Parquet, Delta Tables                          │
│ • Broadcast joins for optimization                              │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ TRANSFORM LAYER                                                 │
│ • Window functions (LEAD/LAG for sequential analysis)           │
│ • Aggregations with collect_set                                 │
│ • Array operations for product set analysis                     │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ LOAD LAYER                                                      │
│ • Loader Factory Pattern                                        │
│ • Supports: DBFS write, Partitioned writes                      │
│ • Overwrite/Append modes                                        │
└─────────────────────────────────────────────────────────────────┘
```

```

## Key Technical Implementations

| Analysis | PySpark Technique Used |
|----------|----------------------|
| AirPods After iPhone | Window functions with `LEAD()` for sequential purchase analysis |
| Only AirPods and iPhone | `collect_set()` aggregation with `array_contains()` and `size()` filtering |
| Both Products | Combined filter with `OR` logic on `LEAD()` results |
```
### Code Examples

**AirPods After iPhone Transformer:**
```python
windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
transformDF = df.withColumn("next_product", lead("product_name").over(windowSpec))
filteredDF = transformDF.filter(
    (col("product_name") == 'iPhone') & (col("next_product") == 'AirPods')
)
