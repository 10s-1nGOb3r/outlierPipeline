✈️ Aircrew Flight Hour Outlier ETL Pipeline

📌 Overview
This project builds an ETL pipeline to analyze aircrew monthly flight hours and detect statistical outliers based on operational grouping.
The pipeline processes multiple AIMS reports and produces analytical datasets for management review and workforce balancing.

📂 Data Sources
AIMS Report 1.2.1 – Daily Flight Schedule
Flight legs
Crew assignments
Block time
AIMS Report 4.4.2.5 – Block/Duty Time Totals
Monthly crew records
Instructor & Structural Aircrew Database
Crew status filtering

⚙️ Pipeline Flow
Data cleansing and normalization
Block time conversion to decimal hours
Monthly aggregation per crew
Merge with aircrew master data
Exclusion of instructor/structural crew
Union key generation (BASE.AC.POS.MONTH.YEAR)
Calculation of:
Average flight hour
Standard deviation
Outlier classification:
Outlier Above
Outlier Below
Distributed Exponent

KPI aggregation per:
Union
Aircrew Type

📊 Output Files
File	Description
dfsFlightHourDetails.csv	Cleaned flight leg data
aircrewFlightHourPerMonthPerYear.csv	Aggregated monthly flight hours
aircrewOutlierView.csv	Full enriched dataset with outlier tagging
outlierAggregationPerMonthPerUnion.csv	Union-level outlier KPI
outlierAggregationPerMonthPerAircrewType.csv	Cockpit vs Cabin analysis

🧠 Outlier Logic
An aircrew is classified as an outlier if:
| TotalFlightHour - GroupAverage | > 10 hours
Only non-structural, non-instructor crew are considered assignable.

🚀 Tech Stack
Python
Pandas
NumPy

📈 Business Use Case
Workforce balancing
Flight hour fairness monitoring
Assignable crew distribution
Operational anomaly detection

🛠 Future Improvements
Parameterized threshold
Z-score method comparison
Logging implementation

Conversion into Airflow/Dagster job
