# appleAnalysis
Apple Product Purchase Analysis - ETL Pipeline
# Apple Product Purchase Analysis - ETL Pipeline
![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange.svg)
![Databricks](https://img.shields.io/badge/Databricks-Platform-red.svg)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.x-green.svg)

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

## Project Structure

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



## Key Technical Implementations

| Analysis | PySpark Technique Used |
|----------|----------------------|
| AirPods After iPhone | Window functions with `LEAD()` for sequential purchase analysis |
| Only AirPods and iPhone | `collect_set()` aggregation with `array_contains()` and `size()` filtering |
| Both Products | Combined filter with `OR` logic on `LEAD()` results |

### Code Examples

**AirPods After iPhone Transformer:**
```python
windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
transformDF = df.withColumn("next_product", lead("product_name").over(windowSpec))
filteredDF = transformDF.filter(
    (col("product_name") == 'iPhone') & (col("next_product") == 'AirPods')
)
```

**Only AirPods and iPhone Transformer:**
```python
groupedDF = df.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
filteredDF = groupedDF.filter(
    array_contains(col("products"), 'iPhone') &
    array_contains(col("products"), 'AirPods') &
    (size(col("products")) == 2)
)
```

## Performance Optimizations

| Technique | Implementation | Benefit |
|-----------|---------------|---------|
| Broadcast Joins | `broadcast(filteredDF)` | Optimizes small DataFrame joins |
| Partitioned Writes | `partitionBy("location")` | Faster location-based queries |
| Window Functions | Single pass over data | Efficient sequential analysis |
| Column Pruning | `select()` specific columns | Reduces data shuffle |


<!--to put sample outputs -->>

## Setup Instructions

### Prerequisites
- Databricks workspace access
- Cluster with Spark 3.x+
- Unity Catalog enabled

### Steps

1. **Clone or upload** all code files to your Databricks workspace
2. **Update file paths** in `extractor.py`:
   ```python
   transaction_path = "/Volumes/your_catalog/your_schema/your_volume/Transaction_Updated.csv"
   customer_table = "your_catalog.your_schema.customer_delta_table"

3. ** Run the desired workflow in apple_analysis.py:
   ```python
   WorkFlowRunner("onlyAripodsAfterIphoneWorkFlow").runner()
   
# Running the Pipeline
   ## Run specific workflow
   ```python
   WorkFlowRunner("aripodsAfterIphoneWorkFlow").runner()      # AirPods after iPhone
   WorkFlowRunner("onlyAripodsAfterIphoneWorkFlow").runner() # Only AirPods and iPhone
   WorkFlowRunner("bothAripodsAfterIphoneWorkFlow").runner() # Both products
```

# What I Learned
- **Factory patterns** make ETL pipelines extensible and maintainable
- **Window functions** (LEAD/LAG) are powerful for sequence analysis without self-joins
- **Broadcast joins** dramatically improve performance for small lookup tables
- **Partitioning strategies** significantly impact query performance
- **Medallion architecture** (Bronze/Silver/Gold) provides clear data lineage
- **Abstract base classes** enforce consistent interfaces across implementations

#  Future Enhancements
- Add data quality validation layer
- Implement incremental loading (append mode)
- Add unit tests for transformers
- Create Databricks SQL dashboard for visualization
- Add monitoring and alerting


# Contributing
This project is for educational purposes. Feel free to fork and modify for your own learning
