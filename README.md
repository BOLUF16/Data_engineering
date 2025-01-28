# Data Engineering Project: User Engagement and Retention Insights

## Project Overview
This project reflects my approach to solving real-world data engineering challenges, inspired by companies like Monesize. The goal was to design a system that provides actionable insights about user engagement and retention for an app-based business. The project addresses key business questions, including:

- **Which users are active versus inactive?**
- **How do subscription plans influence user behavior?**
- **What features drive the most user interaction?**

By answering these questions, I developed a logical data model and pipeline that deliver meaningful insights tailored to practical business needs.

---

## Key Components

### 1. **Defining the Use Case**

The project was guided by imagining real-world business problems. Specific focus areas included:
- Identifying active and inactive users.
- Analyzing the impact of subscription plans on user behavior.
- Discovering the features that drive the most engagement.

This clarity informed the data model and pipeline design to ensure that outputs aligned with actionable business objectives.

---

### 2. **Logical Model Design**

The system's architecture was based on a dimensional model:
- **Dimension Tables**: `dim_users`, `dim_subscription_plans`.
- **Fact Table**: `fact_interactions`.

Key design considerations included:
- Ensuring interaction dates always occurred after registration dates.
- Creating relationships that provided a comprehensive view of user activity.

---

### 3. **Data Generation**

Since real-world datasets were unavailable, I generated realistic fake data to simulate app usage:
- **Tools**: Python, using functions with lambda expressions and conditional logic.
- **Validation**: Ensured coherence in relationships, such as city-state-country hierarchies and chronological consistency in dates.

This process gave me hands-on experience in data preparation and managing raw data challenges.

---

### 4. **Data Warehouse and dbt Implementation**

The data pipeline leveraged **Snowflake** as the data warehouse and followed **dbt best practices**:

#### Layered Approach:
1. **Staging Layer**: Consolidated raw data into clean, structured tables.
2. **Intermediate Layer**: Applied business logic for transformation.
3. **Mart Layer**: Produced final models, such as `active_and_inactive_user`.

#### Mart Layer Insights:
The `active_and_inactive_user` model provided:
- Subscription plans.
- Total interactions.
- Last activity dates.
- User classification as inactive if days since the last activity exceeded 30.

#### Materialization:
- Tables were materialized as **incremental**, ensuring scalability by processing only new or updated data.

---

### 5. **Testing for Data Integrity**

To maintain data quality:
- **Generic Tests**: Validated constraints such as non-nullable columns and unique values.
- **Singular Tests**: Ensured business rules, e.g., `interaction_date` never preceded `registration_date`.

These tests ensured reliability across the pipeline.

---

## Future Plans

1. **Orchestration with Airflow**: Automating tasks like data ingestion, transformation, and reporting.
2. **Advanced Analytics**: Expanding the pipeline to support additional metrics and insights.





