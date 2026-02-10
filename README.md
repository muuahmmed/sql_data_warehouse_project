# 🚀 Enterprise Data Warehouse & Analytics Platform

<p align="center">
  <b>Production-Grade Data Warehousing Solution</b><br>
  End-to-end implementation using SQL Server and Medallion Architecture
</p>

<p align="center">
  <img src="https://img.shields.io/badge/SQL_Server-2019+-CC2927?style=for-the-badge&logo=microsoft-sql-server" />
  <img src="https://img.shields.io/badge/Architecture-Medallion-4DB33D?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Data_Modeling-Star_Schema-FF6B6B?style=for-the-badge" />
  <img src="https://img.shields.io/badge/ETL-SQL_Pipelines-3498DB?style=for-the-badge" />
</p>

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Key Features](#-key-features)
- [Technical Implementation](#-technical-implementation)
- [Repository Structure](#-repository-structure)
- [Business Analytics](#-business-analytics)
- [Getting Started](#-getting-started)
- [Project Team](#-project-team)
- [Technologies Used](#-technologies-used)

---

## 🎯 Project Overview

This repository demonstrates a complete data warehouse implementation that transforms raw operational data into actionable business insights. The project follows enterprise-grade practices and showcases proficiency in modern data engineering principles, from initial data ingestion through to analytical reporting.

The solution processes data from multiple source systems (ERP and CRM) and implements a robust three-layer architecture that ensures data quality, consistency, and accessibility for business intelligence purposes.

### Business Context

Organizations often struggle with fragmented data spread across multiple systems. This project addresses that challenge by creating a unified analytical platform that enables data-driven decision making across sales, customer analytics, and product performance domains.

### Learning Outcomes

This project was developed as a collaborative effort with Baraa, focusing on practical application of data engineering concepts including dimensional modeling, ETL pipeline development, data quality management, and SQL-based analytics.

---

## 🏗️ Architecture

The project implements the Medallion Architecture, a modern data lakehouse design pattern that organizes data into three distinct layers, each serving a specific purpose in the data transformation pipeline.

```
┌─────────────────────────────────────────────┐
│        Source Systems (CSV Files)           │
│         • ERP System Data                   │
│         • CRM System Data                   │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│          🥉 Bronze Layer (Raw)              │
│  • Exact copy of source data                │
│  • No transformations applied               │
│  • Historical audit trail maintained        │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│        🥈 Silver Layer (Refined)            │
│  • Data cleansing and standardization       │
│  • Duplicate removal                        │
│  • NULL handling and validation             │
│  • Type conversions and formatting          │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│         🥇 Gold Layer (Analytics)           │
│  • Star schema implementation               │
│  • Fact and dimension tables                │
│  • Business-ready datasets                  │
│  • Optimized for reporting and BI           │
└─────────────────────────────────────────────┘
```

### Layer Details

**Bronze Layer** serves as the landing zone for raw data ingestion. This layer preserves the original state of source data without any modifications, providing a foundation for data lineage and enabling the ability to replay transformations if needed.

**Silver Layer** focuses on data quality and standardization. Transformations applied in this layer include handling missing values, removing duplicates, standardizing formats (such as country codes and date formats), and ensuring referential integrity across related datasets.

**Gold Layer** contains business-ready analytical models structured as a star schema. This layer supports efficient querying and reporting, with denormalized structures optimized for analytical workloads rather than transactional processing.

---

## ✨ Key Features

### Data Engineering Excellence

The project demonstrates sophisticated data engineering practices including incremental loading strategies, slowly changing dimension handling, and comprehensive data quality checks throughout the pipeline. The ETL processes are designed to be idempotent and rerunnable, ensuring reliability in production environments.

### Dimensional Modeling

The Gold layer implements a complete star schema with clearly defined fact and dimension tables. Dimension tables capture descriptive business attributes while fact tables record measurable business events and metrics. This design supports flexible analytical queries and efficient aggregations.

### Data Quality Framework

Multiple validation checkpoints ensure data accuracy and completeness at each layer. The framework includes null value handling, duplicate detection and removal, referential integrity validation, data type verification, and business rule enforcement.

### Analytical Capabilities

The solution provides SQL-based analytics that answer critical business questions across multiple domains including customer segmentation and behavior analysis, product performance evaluation, sales trend identification, revenue analytics, and operational efficiency metrics.

---

## 🔧 Technical Implementation

### Bronze Layer Implementation

The Bronze layer ingestion process reads CSV files directly into SQL Server tables using BULK INSERT operations. Each source file is loaded into a dedicated staging table with minimal transformations, preserving data types and structure from the source systems. Load timestamps and source identifiers are captured for audit purposes.

### Silver Layer Transformations

Silver layer transformations apply business rules and data quality standards. Key transformations include standardizing country codes (converting entries like 'DE' to 'Germany' and 'US' or 'USA' to 'United States'), handling null and empty values consistently across all fields, removing duplicate records based on business keys, trimming whitespace and standardizing text fields, and converting date formats to ISO standards.

Example transformation logic:

```sql
CASE WHEN TRIM(entry) = 'DE' THEN 'Germany'
     WHEN TRIM(entry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(entry) = '' OR entry IS NULL THEN 'n/a'
     ELSE TRIM(entry)
END AS cntry
```

### Gold Layer Star Schema

The Gold layer implements a dimensional model with fact tables capturing business transactions and events, dimension tables providing descriptive context, surrogate keys for all dimensions, and foreign key relationships establishing connections between facts and dimensions.

The schema supports common analytical patterns including time-series analysis, customer segmentation, product performance tracking, and multi-dimensional aggregations.

---

## 📂 Repository Structure

```
sql_data_warehouse_project/
│
├── datasets/                    # Source data files
│   ├── erp/                    # ERP system exports
│   └── crm/                    # CRM system exports
│
├── scripts/                     # SQL implementation
│   ├── bronze/                 # Raw data ingestion
│   │   ├── create_tables.sql
│   │   └── load_data.sql
│   │
│   ├── silver/                 # Data cleansing layer
│   │   ├── transformations.sql
│   │   └── quality_checks.sql
│   │
│   └── gold/                   # Analytics layer
│       ├── dimensions.sql
│       ├── facts.sql
│       └── views.sql
│
├── tests/                       # Data quality tests
│   ├── row_counts.sql
│   ├── null_checks.sql
│   └── referential_integrity.sql
│
├── docs/                        # Documentation
│   ├── requirements.md         # Business requirements
│   ├── architecture.md         # Technical architecture
│   ├── data_dictionary.md      # Schema documentation
│   └── diagrams/               # Visual documentation
│
├── requirements.txt             # Python dependencies (if any)
└── README.md                    # This file
```

---

## 📊 Business Analytics

The analytical layer supports comprehensive business intelligence across multiple domains.

### Customer Analytics

The solution enables detailed customer segmentation based on purchase behavior, geographic location, and lifetime value calculations. Analysis capabilities include customer retention metrics, cohort analysis for understanding behavior patterns over time, and identification of high-value customer segments.

### Product Performance

Product analytics track sales velocity, revenue contribution, and inventory turnover. The dimensional model supports drill-down analysis from product categories to individual SKUs, enabling identification of top-performing products and underperforming inventory.

### Sales Insights

Sales reporting capabilities include trend analysis across time periods, geographic performance comparison, channel effectiveness evaluation, and sales representative performance tracking. The star schema design enables flexible aggregation at various granularity levels.

### Operational Metrics

The warehouse tracks key operational indicators including order fulfillment rates, processing times, data quality scores, and system performance metrics.

---

## 🚀 Getting Started

### Prerequisites

To run this project, you will need SQL Server 2019 or later (Express edition is sufficient), SQL Server Management Studio (SSMS), and approximately 500MB of free disk space for the database.

### Installation Steps

Begin by cloning the repository to your local machine. Next, open SQL Server Management Studio and create a new database named `DataWarehouse`. Execute the Bronze layer scripts to create and populate raw tables with source data. Run the Silver layer transformation scripts to clean and standardize the data. Finally, execute the Gold layer scripts to build the dimensional model and analytical views.

### Verification

After installation, verify the implementation by checking row counts in each layer, running data quality tests from the tests directory, and executing sample analytical queries to confirm the schema is functioning correctly.

---

## 👥 Project Team

This project was developed collaboratively by **Muuahmmed** and **Baraa** as part of our data engineering learning journey. The project represents our combined efforts in understanding and implementing modern data warehouse principles and best practices.

---

## 🛠️ Technologies Used

### Core Technologies

**SQL Server Express** provides the database platform for all three layers of the data warehouse. **SQL Server Management Studio** serves as the primary development and administration interface. **T-SQL** is used for all ETL logic, transformations, and analytical queries.

### Design and Documentation

**Draw.io** is utilized for creating architecture diagrams and data models. **Notion** supports project planning and requirements management. **Git and GitHub** enable version control and collaborative development.

### Why These Technologies

All tools selected for this project are freely available, making the solution accessible for learning and demonstration purposes. SQL Server provides robust enterprise features while remaining suitable for development environments. The technology stack represents common tools used in professional data engineering roles.

---

## 📈 Future Enhancements

Potential extensions to this project include implementing incremental ETL processes for efficient updates, adding slowly changing dimension support for historical tracking, developing automated testing frameworks, creating Power BI dashboards for visualization, and implementing data lineage tracking.

---

## 📝 License

This project is open source and available for educational and portfolio purposes.

---

## 🤝 Contributing

While this is primarily a portfolio project, feedback and suggestions are welcome. Feel free to open issues or submit pull requests with improvements.

---

## 📧 Contact

For questions or collaboration opportunities, please reach out through GitHub issues or connect via the contact information in the profile.

---

<p align="center">
  <b>⭐ If you find this project helpful, please consider giving it a star! ⭐</b>
</p>

<p align="center">
  <i>Built with attention to detail and best practices in data engineering</i>
</p>
