# Modern Data Stack
The "Boring" Way to Build High-Performance Data Pipelines. Demo for modern data pipelines.

### **Project Description: Embedded Modern Data Stack (EMDS)**

**The "Boring" Way to Build High-Performance Data Pipelines.**

This project is a tutorial and boilerplate for building a **serverless, local-first ELT pipeline**. It demonstrates how to replace complex, over-engineered architectures (like Spark clusters or cloud warehouses) with a lightweight "Embedded Data Stack" that runs entirely on a single node or even your laptop.

#### **🚀 Why this stack?**

* **"Boring is Better":** Eliminates technical debt by removing the need for standalone database servers (Postgres) or cloud warehouses (Snowflake) for small-to-medium datasets.


* **Zero-Copy Performance:** Leverages **Apache Arrow** to share memory instantly between **DuckDB** and **Polars**, allowing for lightning-fast handoffs between SQL transformations and Python DataFrame processing without data serialization overhead.


* **Local-First Storage:** Uses the local file system as a high-performance "Data Lake" with **Parquet** files, skipping the ingestion step entirely by querying files where they sit.



#### **🛠️ The Stack**

* **Ingestion:** [dlt (Data Load Tool)]() for automated, schema-aware API data landing.


* **Storage:** Local **Parquet** files for efficient, columnar data storage.


* **Transformation Engine:** [DuckDB]() for serverless SQL filtering and joins.


* **Processing Engine:** [Polars]() for complex, multi-threaded Python-native transformations and rolling aggregations.


* **Orchestration Logic:** Structured like a **dbt** project (Staging vs. Marts) but implemented in modular Python scripts.



#### **📖 What You'll Learn**

1. How to land raw API data into a local data lake using **dlt**.


2. How to use **DuckDB** to clean and filter millions of rows in milliseconds using standard SQL.


3. How to perform "Zero-Copy" handoffs to **Polars** for complex logic that SQL struggles with.


4. How to implement **Incremental Loading** (the "Lookback" pattern) so your pipeline only processes new data.



---

Inspired by the philosophy that the best data pipeline is the one you don't have to build.
