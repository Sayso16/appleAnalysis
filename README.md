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

apple-purchase-etl/
│
├── code/
│   ├── reader_factory.py
│   ├── extractor.py
│   ├── transform.py
│   ├── loader_factory.py
│   ├── loader.py
│   └── apple_analysis.py
│
├── data/
│   └── Transaction_Updated.csv
│
└── README.md
