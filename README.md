# Azure Lakehouse ETL Platform

A production-oriented, end-to-end data engineering project built around a retail / eCommerce analytics use case. The goal is to demonstrate the full lifecycle of a modern cloud data platform — from raw ingestion through curated serving — using Azure-native tooling and industry-standard patterns.

---

## Architecture Summary

The platform follows a **medallion architecture** (Bronze / Silver / Gold) on ADLS Gen2, orchestrated by Azure Data Factory and processed by Azure Databricks.

- **Bronze** — raw ingestion from APIs and flat files, stored as-is in Delta Lake
- **Silver** — cleansed, validated, and conformed data; business rules applied
- **Gold** — aggregated, analytics-ready tables serving BI and reporting consumers

---

## Business Use Case

Retail / eCommerce decision support platform. Consolidates operational and reference data to enable:

- Sales performance analysis across channels and time periods
- Returns monitoring and trend detection
- Customer behavior and segmentation analysis
- Campaign and channel attribution
- Executive KPI reporting
- Lightweight forecasting signals

---

## Core Stack

- **Azure Data Factory** — pipeline orchestration and scheduling
- **Azure Databricks** — distributed transformation with PySpark
- **ADLS Gen2** — scalable cloud storage for all lakehouse layers
- **Delta Lake** — ACID-compliant table format across medallion layers
- **PySpark** — transformation logic and data quality enforcement
- **dbt** *(optional)* — SQL-based modelling for Gold layer
- **Power BI** *(optional)* — analytical dashboards on the serving layer

---

## What This Project Demonstrates

- End-to-end platform design: ingestion, transformation, modelling, and serving
- Multi-source ingestion handling (REST APIs and structured files)
- Medallion architecture with Delta Lake on ADLS Gen2
- Orchestration patterns using Azure Data Factory
- Data quality validation at pipeline boundaries
- Business-facing analytical output from a well-structured data model

---

## Why This Project Matters

Most portfolio projects stop at transformation. This one is scoped as a working data platform: it connects real-world orchestration, storage, compute, and serving layers in the way a professional team would. The focus is on engineering decisions that matter in production — not just code that runs.

---

## Repository Structure

- `docs/` — architecture decisions, business case, data model, pipeline design, runbook
- `adf/` — Azure Data Factory pipeline definitions
- `databricks/` — PySpark notebooks and Databricks job configurations
- `sql/` — analytical queries and validation scripts
- `tests/` — data quality checks and pipeline test cases
- `data_samples/` — representative input files for local development
- `config/` — environment and pipeline configuration artifacts
- `diagrams/` — architecture diagrams and data flow visuals

---

## Status

Project in active development. Core architecture and documentation in progress.
