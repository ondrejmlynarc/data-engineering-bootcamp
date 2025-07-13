# Week 1: Dimensional Data Modeling Overview
This repo summarizes the first week of our Data Engineering Bootcamp on Dimensional Data Modeling. It covers the following:

- Design scalable data models using dimensions, SCDs, and graph structures
- Build reliable pipelines with idempotency and smart schema design
- Optimize performance with efficient modeling patterns like cumulative tables.
---

## **Day 1: Dimensional Data Modeling Complex Data Type and Cumulation Day**


Day 1 covered the basics of dimensions, how data models serve different users, and introduced key patterns like cumulative tables and using complex data types.

| Concept                     | Description                                                                                             | Example / Use Case                                           |
| :-------------------------- | :------------------------------------------------------------------------------------------------------ | :----------------------------------------------------------- |
| **Dimensions** | Attributes defining an entity: **`Identifier`** (unique ID) & **`Attribute`** (descriptive, can be `Slowly Changing` or `Fixed`). | `user_id`, `product_category`, `birth_date`                  |
| **Data Consumer Alignment** | Models must suit specific users (Analysts, Engineers, ML, End Users).                                   | Flat tables for analysts; complex types for engineers.       |
| **Modeling Spectrum** | Continuum from `OLTP` (transactional) to `Metrics Layer` (aggregated).                                  | `MySQL` (OLTP) → `Data Warehouse` (OLAP)                     |
| **Cumulative Tables** | Builds full historical data by merging daily changes (e.g., `FULL OUTER JOIN`).                         | Tracking daily user activity.                                |
| **Compactness vs. Usability** | Tradeoff: highly denormalized for ease of use, or highly compressed for storage.                        | Flat tables (usable); `ARRAY<STRUCT>` (middle ground).       |
| **Complex Data Types** | `STRUCT` (record), `MAP` (key-value), `ARRAY` (ordered list).                                           | Storing semi-structured or temporal data efficiently.        |
| **Temporal Explosion** | Handling massive data volume from time-series dimensions.                                               | `ARRAY` of `STRUCT` pattern to preserve compression.         |

---

## **Day 2: Dimensional Data Modeling: Building Slowly Changing Dimensions**

Day 2 dove into Slowly Changing Dimensions (SCDs) and the important concept of idempotency for building reliable data pipelines.

| Concept                         | Description                                                                                             | Impact / Best Practice                                       |
| :------------------------------ | :------------------------------------------------------------------------------------------------------ | :----------------------------------------------------------- |
| **Slowly Changing Dimensions (SCDs)** | Attributes that change infrequently over time (e.g., `age`, `weight`).                                  | Must be modeled carefully to preserve historical context.    |
| **Idempotency (Core Principle)**| Pipeline produces identical output regardless of run time or frequency.                                 | **Ensures data quality, reproducibility, and trustworthiness.** |
| **Common Non-Idempotent Pitfalls** | Anti-patterns causing data inconsistencies (e.g., `INSERT INTO` without `OVERWRITE`, unbounded date ranges, reliance on "latest" partition). | Avoids silent failures and data discrepancies.               |
| **SCD Modeling Strategies** | Approaches to manage dimensional changes:                                                               |                                                              |
| _SCD Type 0_                    | Value is constant; cannot be labeled SCD.                                                               | `birth_date`.                                                |
| _SCD Type 1 (Overwrite)_        | Only stores the latest value, overwriting historical data.                                              | **Loses historical attribute context; not idempotent for history.** |
| _SCD Type 2 (Row Versioning)_   | Creates new rows for changes, tracking validity with `start_date`/`end_date`.                           | **Idempotent; preserves full history efficiently.** |
| _SCD Type 3_                    | Stores "original" and "current" values.                                                                 | **Not idempotent** for backfills; less common.               |

---

## **Day 3: Dimensional Data Modeling: Graph Data Modeling Day 3 Lecture**

Day 3 covered Graph Data Modeling, focusing on relationships, additive vs. non-additive dimensions, ENUMs, and flexible schemas.

| Concept                           | Description                                                                                             | Application / Consideration                                  |
| :-------------------------------- | :------------------------------------------------------------------------------------------------------ | :----------------------------------------------------------- |
| **Graph Data Modeling** | **Relationship-focused**, less entity-focused. Flexible schema.                                         | Understanding connections (e.g., social networks).           |
| **Graph Schema Structure** | **Vertices (Nodes):** `ID`, `Type`, `Properties (MAP)`. **Edges:** `Subject`, `Object`, `Edge Type`, `Properties (MAP)`. | `Player` (Vertex), `PLAYS_AGAINST` (Edge).                  |
| **Additive vs. Non-Additive Dimensions** | Determines if subtotals can be summed for a grand total.                                               | Critical for `COUNT DISTINCT` metrics where overlaps occur (e.g., distinct users across multiple products). |
| **Power of `ENUM`s (Enumerations)** | Predefined, limited sets of values. Provides data quality, static fields, and documentation.            | `notification_channel` (`SMS`, `Email`, `Push`). Not for high cardinality fields. |
| **"Little Book of Pipelines" Pattern** | `ENUM`-driven pattern to integrate diverse data into a shared schema with granular quality checks.        | Complex data ingestion (e.g., Unit Economics).              |
| **Flexible Schema (`MAP`)** | Using `MAP` columns for varied properties, avoiding wide tables.                                        | Consolidating disparate event properties.                      |
| **Flexible Schema Drawbacks** | **Poor compression** due to key names stored per row.                                                   | Tradeoff for schema flexibility.                             |