<div align="center">

# 🚇 VBB Transit Data Engineering & Analytics Platform

### Berlin-Brandenburg Public Transport — End-to-End Data Engineering Project

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square&logo=postgresql&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![Scikit-learn](https://img.shields.io/badge/Scikit--learn-ML-F7931E?style=flat-square&logo=scikitlearn&logoColor=white)

**DS463 Data Engineering | Master's Programme**
**Supervisor: Dr. Farhan Khan**

</div>

---

## Table of Contents

1. [Project Abstract](#1-project-abstract)
2. [Problem Statement](#2-problem-statement)
3. [Research Objectives & Business Questions](#3-research-objectives--business-questions)
4. [System Architecture](#4-system-architecture)
5. [Technology Stack](#5-technology-stack)
6. [Team & Contributions](#6-team--contributions)
7. [Dataset Overview](#7-dataset-overview)
8. [Repository Structure](#8-repository-structure)
9. [Pipeline Implementation](#9-pipeline-implementation)
10. [Dashboard & Visual Analytics](#10-dashboard--visual-analytics)
11. [Written Report](#11-written-report)
12. [Key Findings & Insights](#12-key-findings--insights)
13. [Business Questions — Answers](#13-business-questions--answers)
14. [How to Run](#14-how-to-run)
15. [Future Work](#15-future-work)
16. [Data Source & References](#16-data-source--references)

---

## 1. Project Abstract

This project presents a fully automated, end-to-end data engineering platform for the **Verkehrsverbund Berlin-Brandenburg (VBB)** — one of Europe's largest regional public transport networks, coordinating over 35 transit operators across Berlin and the state of Brandenburg.

The platform ingests raw GTFS (General Transit Feed Specification) data, processes and cleans it through a multi-stage pipeline, stores it in a relational database, applies distributed computing for large-scale aggregations, and exposes analytical insights through interactive Power BI dashboards and a structured written report. A machine learning module adds predictive capability, achieving **69.6% accuracy** in delay classification using a Random Forest model.

The project demonstrates the complete data engineering lifecycle — from raw, non-human-readable transit feeds to a query-ready, decision-supporting analytics surface — and directly addresses six research-defined business questions about the VBB network.

---

## 2. Problem Statement

Public transportation networks such as VBB generate enormous volumes of structured and semi-structured data daily — static schedules, real-time vehicle positions, service alerts, stop metadata and historical exception records. While this data is publicly available, it exists in formats (GTFS, Protobuf) that are not immediately usable for analysis, and is fragmented across multiple feeds with inconsistent structures, missing values and non-standard encodings.

The absence of a unified, cleaned and analytics-ready data platform limits the ability of operators, researchers and the public to:

- Assess service reliability and punctuality at the route, stop and time-of-day level
- Understand passenger demand and network load distribution
- Identify structural gaps such as accessibility deficiencies
- Forecast delays and optimise operational decision-making
- Track long-term schedule evolution and service changes

This project addresses these gaps by engineering a scalable, automated pipeline that transforms raw VBB feeds into a structured analytical platform with interactive visualisations and predictive modelling.

---

## 3. Research Objectives & Business Questions

| # | Business Question | Method Used |
|---|---|---|
| BQ1 | How punctual is each VBB line and where do delays cluster? | ML Delay Model — Random Forest |
| BQ2 | Which stops and corridors are the busiest in the network? | Transfer analysis via SQL + Power BI |
| BQ3 | How does service reliability change during peak hours and weekends? | Calendar + exception analysis |
| BQ4 | Can the expected delay of a trip be predicted 15–30 minutes in advance? | Scikit-learn Random Forest (69.6% accuracy) |
| BQ5 | How can historical schedule changes be tracked to measure service evolution? | Calendar exceptions — 75,729 records |
| BQ6 | Can raw VBB GTFS feeds be transformed into a query-ready analytical surface? | PostgreSQL + Spark Parquet + Power BI |

---

## 4. System Architecture

The platform follows a **layered data engineering architecture** comprising five distinct stages:

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│              VBB Open GTFS Feeds (Static + Realtime)           │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                ┌───────────▼───────────┐
                │   INGESTION LAYER     │
                │  Apache NiFi + Kafka  │
                └───────────┬───────────┘
                            │
                ┌───────────▼───────────┐
                │   PROCESSING LAYER    │
                │   Python + Pandas     │
                │   (7 Clean CSVs)      │
                └───────────┬───────────┘
                            │
               ┌────────────┼────────────┐
               │            │            │
    ┌──────────▼──┐  ┌──────▼──────┐  ┌─▼────────────┐
    │ PostgreSQL  │  │Apache Spark │  │  ML Module   │
    │  (Storage)  │  │  (Parquet)  │  │ Scikit-learn │
    └──────────┬──┘  └──────┬──────┘  └─┬────────────┘
               │            │            │
               └────────────┼────────────┘
                            │
                ┌───────────▼───────────┐
                │  ORCHESTRATION LAYER  │
                │    Apache Airflow     │
                │   (Daily DAG Runs)    │
                └───────────┬───────────┘
                            │
                ┌───────────▼───────────┐
                │  ANALYTICS LAYER      │
                │  Power BI Dashboard   │
                │  (5 Pages + Report)   │
                └───────────────────────┘
```

---

## 5. Technology Stack

The platform was built using a carefully selected set of industry-standard tools that together cover the full data engineering lifecycle. **Apache NiFi** was used for automated batch ingestion of the VBB GTFS static feeds, while **Apache Kafka** served as the streaming layer for handling real-time event updates. Once ingested, raw data was cleaned and transformed using **Python 3.9** and **Pandas**, which provided the flexibility required for feature engineering and data quality operations across heterogeneous GTFS file structures.

Cleaned datasets were loaded into a **PostgreSQL 15** relational database to provide structured, query-ready storage with referential integrity across all tables. For large-scale aggregations beyond the efficient capacity of Pandas, **Apache Spark 3.x (PySpark)** was deployed to process the full dataset in a distributed manner and write outputs as compressed Parquet files. The entire pipeline was automated and scheduled using **Apache Airflow 2.x**, which orchestrates all stages through a daily triggered DAG.

On the analytical side, **Scikit-learn** was used to build and evaluate a Random Forest delay classification model. **Plotly** and **Folium** were used for interactive chart generation and geographic station mapping respectively during the exploratory analysis phase. The final analytical surface was delivered as an interactive five-page **Power BI Desktop** dashboard accompanied by a structured written report. Throughout the project, **Git and GitHub** were used for version control and collaborative development.

---

## 6. Team & Contributions

The project will be completed by a team of four members, each responsible for a clearly defined component of the platform. **Pradeep Kunchenahalli Nagarajappa** served as team lead and was responsible for the end-to-end pipeline architecture, PostgreSQL schema design, Airflow DAG orchestration and overall project coordination. **Abhishek Karthik Akunuru** and **Mahesh Gowda Sonnegowda** handled the exploratory data analysis, Plotly visualisations, the interactive Folium station map and all Apache Spark distributed processing, including the generation of Parquet output files and led the data preprocessing and cleaning across all seven datasets, including feature engineering, data quality validation and the calendar and exception analysis. **Nisarga Bhaktharahalli Muniraju** was responsible for the design and development of the four-five-page interactive Power BI dashboard and the production of the full written analytical report, covering all findings, insights and strategic recommendations.

The project was supervised by **Dr. Farhan Khan** as part of the Data Engineering course

---

## 7. Dataset Overview

All data originates from the **VBB Open GTFS Feed** and was collected, cleaned and engineered into seven analytical-ready datasets covering the period April 2026 to December 2026, with an average service duration of 226 days per schedule. In total, **175,138 records were processed across all seven datasets**.

The `agency_clean.csv` file contains records for all 35 transit operators across the Berlin-Brandenburg region, with 6 columns covering agency identifiers, names, URLs, timezone, language and contact details. The `routes_clean.csv` dataset holds 1,322 route records across 9 columns, including transport mode, brand color, agency linkage and route descriptions. The `stops_clean.csv` dataset is the largest spatially, containing 41,840 stop records across 14 columns, each including GPS coordinates, accessibility status, zone identifiers and location type classification. The `calendar_clean.csv` file captures 3,327 weekly service schedule records across 15 columns, enriched with derived columns for service pattern classification and duration analysis. The `calendar_dates_clean.csv` dataset contains 75,729 individual service exception records across 7 columns, documenting every service addition or removal by date, day of week and month. The `transfers_clean.csv` dataset records 52,682 inter-stop transfer rules across 11 columns, including transfer type, minimum connection time and a derived indicator for same-stop transfers. Finally, `levels_clean.csv` captures 203 records describing the physical level layout of multi-level interchange stations.

### Derived and Engineered Columns

As part of the preprocessing stage, seven new analytical columns were engineered from raw GTFS integer codes to make the data immediately usable for analysis. The `transport_mode` column in the routes dataset converts the raw `route_type` integer into a human-readable label such as Bus, Tram or S-Bahn. The `service_pattern` column in the calendar dataset classifies each service schedule as Full Week, Weekdays Only, Weekends Only or No Service based on the day-of-week flags. The `is_accessible` boolean column in the stops dataset is derived from the `wheelchair_boarding` integer field, where a value of 1 maps to True. The `exception_label` column in calendar_dates converts the `exception_type` code into Service Added or Service Removed. The `min_transfer_minutes` column converts the raw `min_transfer_time` value from seconds into minutes for analytical readability. The `is_same_stop` boolean in the transfers dataset flags records where the origin and destination stop identifiers are identical. Finally, the `location_type_name` column in stops translates the GTFS `location_type` integer into descriptive labels such as Stop / Platform, Station or Entrance / Exit.
| `is_same_stop` | transfers | Boolean — True when `from_stop_id` equals `to_stop_id` |
| `location_type_name` | stops | Readable label from GTFS `location_type` integer code |

---

## 8. Repository Structure

```
Data-Engineering-Project/
│
├── 📁 Cleaned Datasets/                  # 7 production-ready CSV files
│   ├── agency_clean.csv                  # 35 rows
│   ├── routes_clean.csv                  # 1,322 rows
│   ├── stops_clean.csv                   # 41,840 rows
│   ├── calendar_clean.csv                # 3,327 rows
│   ├── calendar_dates_clean.csv          # 75,729 rows
│   ├── transfers_clean.csv               # 52,682 rows
│   └── levels_clean.csv                  # 203 rows
│
├── 📁 EDA_Analysis_figures/              # Exported chart images from EDA
│
├── 📁 Spark_Output/                      # Parquet files from Spark processing
│
├── 📓 Pre-Processing_Datasets.ipynb      # Stage 1 — Data cleaning pipeline
├── 📓 EDA_Analysis.ipynb                 # Stage 2 — Exploratory analysis
├── 📓 Spark_Processing.ipynb             # Stage 3 — Distributed processing
├── 📓 ML_Delay_Model.ipynb               # Stage 4 — Predictive modelling
│
├── 🐍 DAG_pipeline.py                    # 6-task sequential pipeline script
├── 🐍 vbb_pipeline_dag.py                # Apache Airflow DAG definition
│
├── 🗺️  berlin_stations_map.html           # Interactive Folium station map
└── 📄 README.md                          # Project documentation
```

---

## 9. Pipeline Implementation

### Stage 1 — Data Ingestion

Raw VBB GTFS static feeds were ingested using **Apache NiFi**, which automated the collection and routing of multiple feed files into the processing layer. **Apache Kafka** was configured as a streaming layer to handle real-time GTFS-Realtime vehicle position updates and service alerts, enabling the platform to process both historical static data and live event streams.

### Stage 2 — Data Cleaning & Preprocessing — `Pre-Processing_Datasets.ipynb`

All seven datasets were cleaned and standardised using Python and Pandas. Columns with more than 50% null values were assessed individually, while partial nulls were filled with contextual defaults — for example, missing `stop_desc` values were replaced with "No description" to preserve row integrity. All date columns were parsed to ISO format, GTFS integer codes were cast to their correct data types, and boolean fields were derived from binary integer encodings. Exact duplicate rows were identified and removed across all datasets. Seven new analytical columns were engineered from raw GTFS codes, as described in the Dataset Overview section. Stop names and agency names were also corrected for UTF-8 encoding consistency across German special characters including ä, ö, ü and ß. The output of this stage was seven clean CSV files totalling 175,138 records, saved to the `Cleaned Datasets/` folder.

### Stage 3 — Exploratory Data Analysis — `EDA_Analysis.ipynb`

A comprehensive exploratory data analysis was conducted across all datasets using Plotly for interactive charts and Folium for geographic visualisation. The analysis covered transport mode distribution across 1,322 routes, agency-level route coverage with identification of the top ten operators, stop type and accessibility profiling across 41,840 stops, service pattern segmentation from 3,327 calendar records, monthly exception volume trends from 75,729 records, and transfer time distribution and hub connectivity analysis. An interactive geographic map of all stops was also produced (`berlin_stations_map.html`) using the `stop_lat` and `stop_lon` coordinate columns, providing a spatial view of the full Berlin-Brandenburg network. All chart outputs were saved to the `EDA_Analysis_figures/` folder.

### Stage 4 — Distributed Processing — `Spark_Processing.ipynb`

Apache Spark (PySpark) was deployed to handle large-scale aggregations that exceed the efficient capacity of Pandas at this dataset scale. All seven cleaned CSVs were loaded into Spark DataFrames, and group-by aggregations were performed across routes by transport mode, stops by zone, transfers by hub and exceptions by month. Cross-dataset joins were executed using Spark SQL to produce enriched analytical outputs. All results were written as compressed Parquet files to the `Spark_Output/` folder, enabling fast downstream querying without reprocessing raw data.

### Stage 5 — Relational Storage

All 7 cleaned datasets were loaded into a **PostgreSQL 15** database (`vbb_db`) using high-speed `COPY` bulk inserts via psycopg2. Table schemas were dynamically generated from DataFrame column structures. The relational model supports all analytical queries underpinning the dashboard and ML model.

### Stage 6 — Machine Learning — `ML_Delay_Model.ipynb`

A **Random Forest classifier** was trained to predict service delay risk using features derived from the calendar and transfers datasets. The feature set included `service_pattern` as a categorical variable representing Full Week, Weekdays Only or Weekends Only classification, along with `weekday_service_days`, `weekend_service_days` and `total_service_days_per_week` as numerical frequency indicators, and a binary weekday/weekend flag derived from the calendar day columns. The model was trained on `calendar_clean.csv` and `transfers_clean.csv` with delay risk as the target variable, achieving a final classification accuracy of **69.6%**.

### Stage 7 — Pipeline Orchestration — `DAG_pipeline.py` + `vbb_pipeline_dag.py`

The full pipeline is automated using **Apache Airflow** through a daily scheduled DAG named `vbb_transit_pipeline`. The pipeline executes six tasks in sequence. Task 1 verifies that all seven source CSV files exist and are accessible on disk. Task 2 validates each dataset by checking row counts, column integrity and data types. Task 3 performs a bulk load of all tables into PostgreSQL using high-speed `COPY` inserts. Task 4 verifies that the Spark Parquet output files are present and complete in the output folder. Task 5 executes a set of EDA summary queries directly against the PostgreSQL database to confirm analytical readiness. Task 6 logs the pipeline completion time along with total row counts and a per-table summary. The DAG is configured with `@daily` scheduling at midnight, `catchup=False` to prevent backfill runs, and is tagged with `['vbb', 'data-engineering']` for organisation within the Airflow UI.

---

## 10. Dashboard & Visual Analytics

An interactive **4-5-page Power BI dashboard** will be developed using all 7 cleaned datasets, designed to answer the project's defined business questions through accessible, decision-ready visualisations.

---

### Page 1 — Network Overview

**Business Question Addressed:** BQ6 — Query-ready analytical surface

This page provides an executive-level summary of the entire VBB network. Four KPI metric cards present the headline figures — 1,322 total routes, 35 agencies, 41,840 stops and 52,682 transfers. A donut chart visualises the transport mode distribution, clearly showing that Bus accounts for 86.1% of all routes, followed by Rail Regional at 5.2%, Tram at 3.8%, S-Bahn at 3.6%, U-Bahn at 0.7% and Water Transport at 0.6%. A horizontal bar chart ranks the top ten agencies by route count, with Berliner Verkehrsbetriebe leading at 265 routes, followed by prignitzbus (85), Cottbusverkehr GmbH (76), regiobus Potsdam Mittelmark (74) and Oberhavel Verkehrsgesellschaft (68). Route colours across all visuals use the actual VBB brand hex codes sourced from the `route_color` column, ensuring visual consistency with the official network identity.

---

### Page 2 — Stop & Accessibility Analysis

**Business Question Addressed:** BQ2 — Busiest stops; BQ3 — Service accessibility

This is the most analytically significant page of the dashboard, centred on the network's critical accessibility gap. A headline KPI card communicates the key finding immediately — only **7.7%** of 41,840 stops are wheelchair accessible, equating to just 3,235 stops across the entire Berlin-Brandenburg network. A geographic map plots all 41,840 stops using their `stop_lat` and `stop_lon` coordinates, with green markers indicating accessible stops and grey markers indicating non-accessible stops. The concentration of grey markers in suburban and Brandenburg areas makes the accessibility gap immediately visible without requiring any numerical interpretation. A treemap further breaks down the stop population by location type, showing Stop/Platform at 27,767, Generic Node at 6,774, Entrance/Exit at 6,422 and Station at 877. A supporting KPI card highlights that the network spans 12,930 unique fare zones.

---

### Page 3 — Service Patterns Analysis

**Business Question Addressed:** BQ3 — Peak reliability; BQ5 — Historical schedule changes

This page examines how services are structured across the week and how the schedule evolves over time. A clustered bar chart shows the service pattern distribution — Full Week services total 585, Weekdays Only 377, Weekends Only 300 and No Service 2,065. A line chart tracks the monthly volume of all 75,729 service exceptions, with the May spike of 27,649 exceptions annotated directly on the chart and explained as corresponding to the activation of new schedule periods at the start of the observed timeframe. A stacked bar chart separates the exception volume by type — Service Added (47,602) versus Service Removed (28,127) — showing that the network consistently adds more services than it removes. An interactive slicer allows independent filtering between Service Added and Service Removed for granular trend analysis.

---

### Page 4 — Transfer Hub Analysis

**Business Question Addressed:** BQ2 — Busiest corridors; BQ3 — Service reliability

This page focuses on the transfer network and its implications for passenger connectivity and experience. A ranked bar chart identifies the top ten busiest transfer hubs by connection volume, with S+U Rathaus Spandau confirmed as the primary interchange hub — consistent with the findings from the exploratory data analysis. A donut chart shows that 91.5% of all transfers (48,201 out of 52,682) are of the Minimum Time Required type, with only 8.5% classified as Timed Transfers. Two KPI cards highlight the same-stop versus inter-stop split — 17,681 transfers occur at the same stop, while 35,001 (66.4%) require passengers to physically move between stops, making minimum connection time a direct driver of service reliability. A histogram of minimum transfer time distribution shows that times range from 0 to 12 minutes with an average of 2.6 minutes.

---

### Page 5 — Executive Summary

A single consolidated page presenting the highest-impact KPIs and key insights from all four analytical pages — designed for rapid comprehension by stakeholders and assessors without requiring navigation through the full dashboard.

---

## 11. Written Report

A structured eight-page analytical report was produced alongside the dashboard, providing academic-level interpretation of all dashboard findings and situating the results within the broader context of the VBB network and public transport analytics.

The report opens with an **Introduction** that establishes the VBB network context, defines the project scope and maps the analysis to the six business questions. A **Data Overview** section follows, describing each dataset, the preprocessing methodology applied and the rationale behind the engineered features. The third section covers the **Network Overview Analysis**, interpreting the transport mode dominance finding — specifically that Bus accounts for 86.1% of all routes — and examining the agency distribution across 35 operators. The fourth section presents the **Stop and Accessibility Analysis**, quantifying the 7.7% wheelchair accessibility rate and providing geographic evidence of where the gap is most pronounced, with equity implications for suburban and Brandenburg passengers discussed in detail. The fifth section analyses **Service Patterns**, examining the balance of Full Week, Weekdays Only and Weekends Only services, interpreting the 75,729 exception records and explaining the May scheduling spike with appropriate contextualisation. The sixth section covers **Transfer Hub Analysis**, assessing the criticality of S+U Rathaus Spandau, interpreting the 66.4% inter-stop transfer proportion and discussing the relationship between minimum transfer time and overall service reliability. A **Conclusion** section maps all findings back to the six business questions, and a final **Recommendations** section presents four evidence-based strategic recommendations for VBB network improvement.

The four recommendations produced are as follows. First, the report recommends that VBB prioritise wheelchair accessibility upgrades at suburban and Brandenburg stations, where the geographic map evidence shows the most pronounced gap. Second, it recommends improving scheduling communication and disruption management during high-exception periods, particularly May and June, when schedule renewal activity is at its peak. Third, it recommends strengthening interchange capacity at S+U Rathaus Spandau, given its position as the network's highest-volume transfer hub. Fourth, it recommends expanding Full Week service coverage into Brandenburg corridors currently classified as Weekdays Only, to improve connectivity for residents outside the Berlin city boundary.

---

## 12. Key Findings & Insights

Analysis across all seven datasets produced ten principal findings that directly address the project's business questions and contribute to a comprehensive understanding of the VBB network.

Bus routes account for **86.1%** of all 1,322 VBB routes, establishing that the network is fundamentally bus-driven, with rail modes serving primary corridors only. Only **7.7%** of 41,840 stops are wheelchair accessible — just 3,235 stops — representing a critical equity gap concentrated in suburban and Brandenburg areas. Full Week services (585) outnumber both Weekdays Only (377) and Weekends Only (300) services, indicating a relatively consistent seven-day service commitment across the core network. A total of **75,729** service exceptions were recorded over the observed period, of which 47,602 are service additions and 28,127 are removals, showing that the network expands more than it contracts. May 2026 registered the highest exception volume at 27,649, corresponding to the activation of new schedule periods at the start of the observed timeframe.

**S+U Rathaus Spandau** was identified as the most critical interchange hub, carrying the highest transfer connection volume in the entire network. Of 52,682 total transfers, **35,001 (66.4%)** require passengers to physically move between stops, making minimum transfer time a direct determinant of passenger experience and service reliability. The average minimum transfer time across the network is **2.6 minutes**, ranging from 0 to 12 minutes. **Berliner Verkehrsbetriebe** operates 265 routes — approximately 20% of the entire network — making it the dominant single operator by a significant margin over the next-largest agency. Finally, the Random Forest delay prediction model achieved a classification accuracy of **69.6%** using service pattern and calendar-derived features, providing a viable predictive capability for operational delay risk assessment.

---

## 13. Business Questions — Answers

The project's six business questions were addressed as follows.

**BQ1 — Punctuality and delay clustering:** Delay risk was found to cluster in services operating under irregular or limited service patterns, particularly those classified as Weekends Only or No Service during certain periods. The Random Forest model trained on service pattern and calendar features achieved 69.6% accuracy in classifying delay risk, providing a viable predictive tool for operational planning.

**BQ2 — Busiest stops and corridors:** S+U Rathaus Spandau was identified as the busiest stop in the network by transfer connection volume. At the operator level, Berliner Verkehrsbetriebe operates 265 routes — the highest route count of any single agency — making it the dominant corridor provider across Berlin.

**BQ3 — Service reliability during peak hours and weekends:** Full Week services (585) provide the most consistent daily coverage across the network. However, service reliability during peak periods is significantly impacted by the transfer dependency of the system — 35,001 inter-stop transfers require passengers to physically change stops, meaning that minimum transfer time directly influences punctuality and connection success rates.

**BQ4 — Delay prediction:** Trip delay risk can be predicted with 69.6% accuracy using a Random Forest classifier trained on features including service pattern classification, weekday and weekend service day counts, total service frequency and binary day-type flags derived from the calendar dataset.

**BQ5 — Historical schedule tracking:** A total of 75,729 service exceptions were tracked across nine months of the observed period, providing a detailed record of every schedule addition and removal. May 2026 exhibited the highest exception activity at 27,649 records, marking the network's primary schedule renewal point and demonstrating the platform's capability to capture and contextualise long-term service evolution.

**BQ6 — Query-ready analytical surface:** Raw VBB GTFS feeds were successfully transformed into seven cleaned and feature-engineered CSV files, a PostgreSQL relational database with eight tables, Spark-generated Parquet files for scalable querying, a five-page interactive Power BI dashboard and an eight-page written analytical report — all accessible without requiring custom scripting or raw GTFS expertise.

---

## 14. How to Run

### Prerequisites

```
Python          3.9+
PostgreSQL      15
Apache Airflow  2.x
Apache Spark    3.x
Power BI        Desktop (latest)
```

### Installation

```bash
# Clone the repository
git clone https://github.com/Abhishek-git31/Data-Enginnering-Project.git
cd Data-Enginnering-Project

# Install Python dependencies
pip install pandas psycopg2-binary pyspark apache-airflow scikit-learn plotly folium jupyter
```

### Running the Pipeline

```bash
# Start PostgreSQL (macOS)
brew services start postgresql@15

# Option A — Run the full pipeline manually
python DAG_pipeline.py

# Option B — Deploy and trigger via Apache Airflow
export AIRFLOW_HOME=~/airflow
airflow db init
cp vbb_pipeline_dag.py ~/airflow/dags/
airflow scheduler &
airflow webserver &
airflow dags trigger vbb_transit_pipeline
```

### Running the Notebooks

Execute in the following order for full reproducibility:

```
1. Pre-Processing_Datasets.ipynb    →  Generates Cleaned Datasets/
2. EDA_Analysis.ipynb               →  Generates EDA_Analysis_figures/ + berlin_stations_map.html
3. Spark_Processing.ipynb           →  Generates Spark_Output/ (Parquet files)
4. ML_Delay_Model.ipynb             →  Trains and evaluates the Random Forest model
```

## Future Work
### Opening the Dashboard

Open `Dashboard_VBB.pbix` in **Power BI Desktop**. Data is pre-loaded from the cleaned CSVs — no live database connection is required to view the dashboard.

---

## 15. Future Work

Several extensions are proposed for future development of this platform. The most impactful next step would be the integration of VBB's GTFS-Realtime feeds to enable live delay monitoring and dynamic Power BI dashboard refresh, moving the platform from a static analytical tool to a live operational decision-support system. The machine learning module could be significantly enhanced by incorporating external data sources such as weather records, public event calendars and historical delay archives, with the goal of improving classification accuracy beyond the current 69.6% baseline.

From an accessibility standpoint, a stop-level accessible routing layer could be built on top of the existing stops and transfers datasets to identify fully wheelchair-accessible end-to-end journeys across the network — directly addressing the equity gap identified in the dashboard. The Power BI dashboard could also be published to the Power BI Service to enable cloud-hosted, public-facing access without requiring Power BI Desktop installation. Finally, extending the calendar exception analysis across multiple years of VBB data would enable longitudinal tracking of structural service trends, providing a richer evidence base for network planning and policy decisions.

---

## 16. Data Source & References

All transit data used in this project was sourced from the **VBB Open Data Portal** at [https://www.vbb.de/vbb-services/api-open-data](https://www.vbb.de/vbb-services/api-open-data). The GTFS file format specifications followed throughout the preprocessing and pipeline stages are documented in the **GTFS Static Reference** at [https://gtfs.org/documentation/schedule/reference](https://gtfs.org/documentation/schedule/reference) and the **GTFS Realtime Reference** at [https://gtfs.org/documentation/realtime/reference](https://gtfs.org/documentation/realtime/reference). Pipeline orchestration was implemented following the **Apache Airflow Documentation** at [https://airflow.apache.org/docs](https://airflow.apache.org/docs), and distributed processing followed the **Apache Spark Documentation** at [https://spark.apache.org/docs/latest](https://spark.apache.org/docs/latest). The machine learning model was developed using the **Scikit-learn Documentation** at [https://scikit-learn.org/stable](https://scikit-learn.org/stable).

---

<div align="center">

*DS463 Data Engineering — Master's Programme*
*Supervisor: Dr. Farhan Khan*
*Berlin-Brandenburg VBB Transit Analytics Platform — 2026*

</div>
