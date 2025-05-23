# Financial Portfolio Management System: Data Warehouse Implementation

## 📊 Overview

This project implements a comprehensive **Financial Portfolio Management Data Warehouse System**. It transforms raw financial data into actionable insights through a robust combination of an operational database, data pipelines, a dimensional data warehouse, and an analytics dashboard. The system is designed to optimize decision-making in investment portfolio management.

---

## 🚀 Features

- 📁 **Operational Database** with 24 normalized tables across customer, portfolio, instrument, and market domains
- ⚙️ **Data Pipelines** using Apache Airflow for ingestion, cleaning, ETL, and dashboard refresh
- 🏗️ **Star Schema Data Warehouse** optimized for analytics
- 📉 **Metabase Dashboards** for portfolio, advisor, and risk analytics
- ✅ **Data Governance** via dbt tests and monitoring
- 🐳 **Containerized Deployment** using Docker and AWS EC2
- 🔁 **Self-Service Analytics** for business users

---

## 📦 Architecture

1. **Ingestion Pipeline**: Extracts raw CSV data from Amazon S3 into PostgreSQL
2. **Cleaning Pipeline**: Standardizes, validates, and fixes data inconsistencies
3. **ETL Pipeline**: Transforms cleaned data into a star schema and loads it into the data warehouse
4. **Dashboard Refresh**: Updates Metabase dashboards via webhooks

---

## 🧱 Data Warehouse Design

### 🔸 Fact Table
- `FactPortfolioPerformance`: Tracks daily portfolio metrics like returns, value, and risk

### 🔹 Dimension Tables
- `DimDate`, `DimCustomer`, `DimPortfolio`, `DimAdvisor`, `DimInstrumentHolding`

### 🧠 Capabilities
- Time-series and segment-based portfolio analysis  
- Risk-return and advisor performance comparisons  
- Benchmarking and performance attribution

---

## 🧼 Data Quality and Governance

Implemented using **dbt**:
- Not Null / Unique / Referential Integrity tests
- Business rule validation (e.g. portfolio weights = 1)
- Conditional pipeline execution on test results

---

## 🛠️ DevOps and Deployment

- **Infrastructure as Code**: AWS EC2, S3, VPCs, and security groups
- **Containers**: Docker for PostgreSQL, Airflow, Metabase
- **Automation**: Airflow scheduling, dbt tests, dashboard refreshes
- **Monitoring**: Logs, alerts, performance tracking

---

## 📊 Visualizations

Hosted in **Metabase**:
- Portfolio Performance
- Customer Segmentation
- Market Trends
- Advisor Analysis
- Risk Assessment

---

## 📈 Outcomes

### ✅ Technical Wins
- Modular pipeline design
- Clean, reliable data models
- Scalable cloud deployment

### 💼 Business Value
- Faster, data-driven investment decisions
- Accurate and insightful portfolio analysis
- Improved operational efficiency for analysts

---

## 🧭 Future Enhancements

- Predictive analytics & ML integration  
- Real-time data streaming  
- Mobile analytics support  
- Integration with market news feeds and alternative data  
- Scalable API layer

---

## 🧠 Authors & Credits

- **Syed Hamza Asif**  
  [GitHub](https://github.com/syedhamzasiff) • [LinkedIn](https://www.linkedin.com/in/syedhamzasiff/)

- **Syed Ali Rizwan**  
  [GitHub](https://github.com/rzn1337) • [LinkedIn](https://www.linkedin.com/in/syedalirizwann/)

For contributions or feedback, feel free to open an issue or pull request.

---
